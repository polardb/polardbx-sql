/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.rplchecker;

import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yudong
 * @since 2023/8/29 12:19
 **/
public class LogicalTableHashCalculator {
    private final String schemaName;
    private final String tableName;
    private final Map<Integer, ParameterContext> boundParameters;
    private final ExecutionContext ec;
    private final PhyTableOperation planSelectHashChecker;
    private final boolean withLowerBound;
    private final boolean withUpperBound;

    public LogicalTableHashCalculator(String schemaName, String tableName, List<String> checkColumnList,
                                      List<Object> lowerBounds, List<Object> upperBounds, ExecutionContext ec) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.ec = ec;
        this.withLowerBound = withBound(lowerBounds);
        this.withUpperBound = withBound(upperBounds);
        this.boundParameters = prepareParameterContext(lowerBounds, upperBounds);
        this.planSelectHashChecker = preparePlanSelectHashChecker(schemaName, tableName, checkColumnList);
    }

    /**
     * 唯一对外暴露的接口，计算逻辑表的哈希值
     */
    public Long calculateHash() {
        // step1. 构造物理执行计划
        List<PhyTableOperation> plans = buildPhysicalPlan();

        // step2. 执行物理执行计划
        List<Long> hashValues = new ArrayList<>();
        for (PhyTableOperation plan : plans) {
            Long hash = executePhysicalPlan(plan);
            if (hash != null) {
                hashValues.add(hash);
            }
        }
        // hash values为0，说明指定的范围内不包含任何数据
        if (hashValues.isEmpty()) {
            return 0L;
        }

        // step3. 综合所有物理表的哈希值
        final OrderInvariantHasher calculator = new OrderInvariantHasher();
        for (Long elem : hashValues) {
            calculator.add(elem);
        }
        return calculator.getResult();
    }

    /**
     * 构造所有的物理执行计划，每张物理表对应一个物理执行计划
     */
    private List<PhyTableOperation> buildPhysicalPlan() {
        List<PhyTableOperation> result = new ArrayList<>();
        final Map<String, Set<String>> phyTables = GsiUtils.getPhyTables(schemaName, tableName);
        for (Map.Entry<String, Set<String>> entry : phyTables.entrySet()) {
            String phyDb = entry.getKey();
            for (String phyTb : entry.getValue()) {
                result.add(buildPhysicalPlanHelper(phyDb, phyTb));
            }
        }
        return result;
    }

    /**
     * 为物理表构造物理执行计划，物理执行计划就是下推到DN上执行的hashcheck函数
     */
    private PhyTableOperation buildPhysicalPlanHelper(String phyDb, String phyTb) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTb, 1));

        PhyTableOpBuildParams opBuildParams = new PhyTableOpBuildParams();
        opBuildParams.setGroupName(phyDb);
        opBuildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTb)));

        // parameters for where (DNF)
        int beginParamIndex = 2;
        final int pkNumber;
        if (withLowerBound || withUpperBound) {
            pkNumber = boundParameters.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
        } else {
            pkNumber = boundParameters.size();
        }

        if (withLowerBound) {
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(beginParamIndex,
                        new ParameterContext(boundParameters.get(j).getParameterMethod(),
                            new Object[] {beginParamIndex, boundParameters.get(j).getArgs()[1]}));
                    beginParamIndex++;
                }
            }
        }
        if (withUpperBound) {
            final int base = withLowerBound ? pkNumber : 0;
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(beginParamIndex,
                        new ParameterContext(boundParameters.get(base + j).getParameterMethod(),
                            new Object[] {beginParamIndex, boundParameters.get(base + j).getArgs()[1]}));
                    beginParamIndex++;
                }
            }
        }

        opBuildParams.setDynamicParams(planParams);
        return PhyTableOperationFactory.getInstance()
            .buildPhyTableOperationByPhyOp(planSelectHashChecker, opBuildParams);
    }

    /**
     * 执行一个物理计划，每个物理计划对应计算一张物理表的哈希值
     */
    private Long executePhysicalPlan(PhyTableOperation plan) {
        // TODO: retry on exception
        Long result = null;
        Cursor cursor = null;
        try {
            cursor = ExecutorHelper.executeByCursor(plan, ec, false);
            Row row;
            if (cursor != null && (row = cursor.next()) != null) {
                result = (Long) row.getObject(0);
            }
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }
        return result;
    }

    private Map<Integer, ParameterContext> prepareParameterContext(List<Object> lowerBounds, List<Object> upperBounds) {
        Map<Integer, ParameterContext> result = new HashMap<>();
        Cursor cursor = convertToCursor(lowerBounds, upperBounds);
        List<Map<Integer, ParameterContext>> maps;
        try {
            maps = Transformer.convertUpperBoundWithDefault(cursor, false, (columnMeta, i) -> {
                // Generate default parameter context for upper bound of empty source table
                ParameterMethod defaultMethod = ParameterMethod.setString;
                Object defaultValue = "0";
                final DataType columnType = columnMeta.getDataType();
                if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.DateType, DataTypes.TimestampType,
                    DataTypes.DatetimeType, DataTypes.TimeType, DataTypes.YearType)) {
                    // For time data type, use number zero as upper bound
                    defaultMethod = ParameterMethod.setLong;
                    defaultValue = 0L;
                }
                return new ParameterContext(defaultMethod, new Object[] {i, defaultValue});
            });
        } finally {
            cursor.close(new ArrayList<>());
        }

        int i = 0;
        for (Map<Integer, ParameterContext> pmap : maps) {
            for (ParameterContext pc : pmap.values()) {
                result.put(i++, pc);
            }
        }
        return result;
    }

    private Cursor convertToCursor(List<Object> lowerBounds, List<Object> upperBounds) {
        ArrayResultCursor result = new ArrayResultCursor(tableName);
        final SchemaManager sm = ec.getSchemaManager(schemaName);
        final TableMeta baseTableMeta = sm.getTable(tableName);
        List<ColumnMeta> pkMetaList = new ArrayList<>(baseTableMeta.getPrimaryKey());
        for (ColumnMeta pkMeta : pkMetaList) {
            result.addColumn(pkMeta);
        }
        if (withLowerBound) {
            result.addRow(convertBoundList(lowerBounds, pkMetaList));
        }
        if (withUpperBound) {
            result.addRow(convertBoundList(upperBounds, pkMetaList));
        }
        return result;
    }

    private Object[] convertBoundList(List<Object> boundList, List<ColumnMeta> metaList) {
        if (boundList.size() != metaList.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC,
                "bound list size:" + boundList.size() + " not equal to meta list size: " + metaList.size());
        }
        Object[] result = new Object[boundList.size()];
        for (int i = 0; i < boundList.size(); i++) {
            ColumnMeta meta = metaList.get(i);
            result[i] = meta.getDataType().convertFrom(boundList.get(i));
        }

        return result;
    }

    private PhyTableOperation preparePlanSelectHashChecker(String schemaName, String tableName,
                                                           List<String> checkColumnList) {
        final SchemaManager sm = ec.getSchemaManager(schemaName);
        final TableMeta baseTableMeta = sm.getTable(tableName);
        if (baseTableMeta == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC,
                "failed to get table meta for table:" + schemaName + "." + tableName);
        }
        final List<String> baseTablePks = FastChecker.getorderedPrimaryKeys(baseTableMeta);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);
        return builder.buildSelectHashCheckForChecker(baseTableMeta, checkColumnList, baseTablePks, withLowerBound,
            withUpperBound);
    }

    private boolean withBound(List<Object> boundValues) {
        return boundValues != null && !boundValues.isEmpty();
    }

}
