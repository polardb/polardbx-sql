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

package com.alibaba.polardbx.executor.columns;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_BACKFILL_EXCEPTION;

public class ColumnBackfillExecutor extends Extractor {
    private static final Logger logger = LoggerFactory.getLogger(ColumnBackfillExecutor.class);

    /*
     * <pre>
     * UPDATE {physical_primary_table}
     * SET {target_column} = {source_column}, {auto_update_column_1} = {auto_update_column_1}, ...
     * WHERE (pk0, ... , pkn) >= (?, ... , ?)
     * AND (pk0, ... , pkn) <= (?, ... , ?)
     * </pre>
     */
    final PhyTableOperation planUpdateWithMinAndMax;

    final PhyTableOperation planUpdateReturningWithMin;
    final PhyTableOperation planUpdateReturningWithMax;
    final PhyTableOperation planUpdateReturningWithMinAndMax;

    final List<String> tableColumns;
    final List<String> primaryKeys;
    final List<String> selectKeys;
    final List<Integer> selectKeysId;
    final Set<Integer> selectKeySet;

    final boolean allExprPushable;
    final PhyTableOperation planUpdateSingleRow;

    final ExecutionContext ec;
    final List<RexNode> rexNodes;
    final List<String> targetColumns;
    final CursorMeta selectCursorMeta;
    final CursorMeta targetCursorMeta;

    public ColumnBackfillExecutor(String schemaName, String tableName, long batchSize, long speedMin, long speedLimit,
                                  long parallelism, PhyTableOperation planSelectWithMax,
                                  PhyTableOperation planSelectWithMin, PhyTableOperation planSelectWithMinAndMax,
                                  PhyTableOperation planUpdateWithMinAndMax, PhyTableOperation planSelectMaxPk,
                                  PhyTableOperation planUpdateReturningWithMin,
                                  PhyTableOperation planUpdateReturningWithMax,
                                  PhyTableOperation planUpdateReturningWithMinAndMax,
                                  PhyTableOperation planSelectSample,
                                  List<Integer> primaryKeysId,
                                  List<String> primaryKeys, List<String> selectKeys, List<String> tableColumns,
                                  boolean allExprPushable, PhyTableOperation planUpdateSingleRow,
                                  List<SqlCall> sourceNodes, List<String> targetColumns, ExecutionContext ec) {
        super(schemaName, tableName, tableName, batchSize, speedMin, speedLimit, parallelism, false, null,
            planSelectWithMax, planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk,
            planSelectSample, primaryKeysId);

        this.planUpdateWithMinAndMax = planUpdateWithMinAndMax;
        this.planUpdateReturningWithMin = planUpdateReturningWithMin;
        this.planUpdateReturningWithMax = planUpdateReturningWithMax;
        this.planUpdateReturningWithMinAndMax = planUpdateReturningWithMinAndMax;
        this.primaryKeys = primaryKeys;
        this.tableColumns = tableColumns;

        this.allExprPushable = allExprPushable;
        this.planUpdateSingleRow = planUpdateSingleRow;
        this.selectKeys = selectKeys;

        this.selectKeysId = new ArrayList<>();
        Map<String, Integer> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < tableColumns.size(); i++) {
            columnMap.put(tableColumns.get(i), i);
        }
        for (String selectKey : selectKeys) {
            selectKeysId.add(columnMap.get(selectKey));
        }
        this.selectKeySet = new HashSet<>();
        selectKeySet.addAll(selectKeysId);

        this.ec = ec;
        this.targetColumns = targetColumns;
        if (allExprPushable) {
            this.rexNodes = null;
            this.selectCursorMeta = null;
            this.targetCursorMeta = null;
        } else {
            SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, ec);
            RelOptCluster cluster = sqlConverter.createRelOptCluster();
            PlannerContext plannerContext = PlannerContext.getPlannerContext(cluster);
            RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

            final TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(sourceTableName);
            final List<RelDataType> fieldTypes = new ArrayList<>();
            final List<String> fieldNames = new ArrayList<>();
            final List<ColumnMeta> columnMetas = new ArrayList<>();

            for (String column : selectKeys) {
                ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(column);
                columnMetas.add(columnMeta);
                fieldTypes.add(columnMeta.getField().getRelType());
                fieldNames.add(column);
            }
            // Add generated column since it may be referenced when adding multiple generated columns
            for (String column : targetColumns) {
                ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(column);
                columnMetas.add(columnMeta);
                fieldTypes.add(columnMeta.getField().getRelType());
                fieldNames.add(column);
            }
            RelDataType rowType = typeFactory.createStructType(fieldTypes, fieldNames);
            this.rexNodes = sqlConverter.
                getRexForGeneratedColumn(rowType, sourceNodes, plannerContext);
            this.selectCursorMeta = CursorMeta.build(columnMetas);
            this.targetCursorMeta = CursorMeta.build(targetColumns.stream().map(tableMeta::getColumnIgnoreCase).collect(
                Collectors.toList()));
        }
    }

    public static ColumnBackfillExecutor create(String schemaName, String tableName, List<SqlCall> sourceNodes,
                                                List<String> targetColumns, boolean forceCnEval, ExecutionContext ec) {
        long batchSize = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_BATCH_SIZE);
        long speedLimit = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_LIMITATION);
        long speedMin = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_MIN);
        long parallelism = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_PARALLELISM);

        // Build select plan
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);
        // tableColumns do not include target column in OMC, it should be hidden at this moment
        final List<String> tableColumns = tableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);
        ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, tableName, tableName, false);
        List<String> primaryKeys = info.getPrimaryKeys();

        List<String> selectKeys = new ArrayList<>(primaryKeys);
        boolean allExprPushable =
            !forceCnEval && sourceNodes.stream().noneMatch(GeneratedColumnUtil::containUnpushableFunction);

        if (!allExprPushable) {
            // Add referenced columns to end of select keys
            Set<String> referencedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (SqlCall sqlNode : sourceNodes) {
                referencedColumns.addAll(GeneratedColumnUtil.getReferencedColumns(sqlNode));
            }

            // Remove primary key since it has been already added
            for (String primaryKey : primaryKeys) {
                referencedColumns.remove(primaryKey);
            }

            // Remove referenced generated column if it's added at the same time
            for (String column : targetColumns) {
                referencedColumns.remove(column);
            }
            selectKeys.addAll(referencedColumns);
        }

        return new ColumnBackfillExecutor(schemaName,
            tableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            builder.buildSelectForBackfill(tableMeta, selectKeys, primaryKeys, false, true,
                SqlSelect.LockMode.EXCLUSIVE_LOCK),
            builder.buildSelectForBackfill(tableMeta, selectKeys, primaryKeys, true, false,
                SqlSelect.LockMode.EXCLUSIVE_LOCK),
            builder.buildSelectForBackfill(tableMeta, selectKeys, primaryKeys, true, true,
                SqlSelect.LockMode.EXCLUSIVE_LOCK),
            builder.buildUpdateForColumnBackfill(tableMeta, sourceNodes, targetColumns, primaryKeys, true, true),
            builder.buildSelectMaxPkForBackfill(tableMeta, primaryKeys),
            builder.buildUpdateReturningForColumnBackfill(tableMeta, sourceNodes, targetColumns, primaryKeys, true,
                false),
            builder.buildUpdateReturningForColumnBackfill(tableMeta, sourceNodes, targetColumns, primaryKeys, false,
                true),
            builder.buildUpdateReturningForColumnBackfill(tableMeta, sourceNodes, targetColumns, primaryKeys, true,
                true),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys()),
            info.getPrimaryKeysId(),
            info.getPrimaryKeys(),
            selectKeys,
            tableColumns,
            allExprPushable,
            builder.buildUpdateSingleRowForColumnBackfill(tableMeta, sourceNodes, targetColumns, primaryKeys),
            sourceNodes,
            targetColumns,
            ec);
    }

    @Override
    protected List<Map<Integer, ParameterContext>> extract(String dbIndex, String phyTableName,
                                                           PhyTableOperation extractPlan, ExecutionContext extractEc,
                                                           BatchConsumer next,
                                                           List<ParameterContext> lowerBound,
                                                           List<ParameterContext> upperBound) {
        final ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
        final boolean allDnUseXDataSource = isAllDnUseXDataSource(topologyHandler);

        boolean canUseReturning =
            allExprPushable && executorContext.getStorageInfoManager().supportsReturning()
                && extractEc.getParamManager()
                .getBoolean(ConnectionParams.OMC_BACK_FILL_USE_RETURNING) && allDnUseXDataSource;

        List<Map<Integer, ParameterContext>> result;

        if (canUseReturning) {
            result = executeAndGetReturning(dbIndex, phyTableName, lowerBound, upperBound, extractEc);
        } else {
            // Extract
            Cursor extractCursor = ExecutorHelper.execute(extractPlan, extractEc);

            // Transform
            try {
                result = buildBatchParam(extractCursor);
            } finally {
                extractCursor.close(new ArrayList<>());
            }
            if (!result.isEmpty()) {
                if (allExprPushable) {
                    // Since source table and target table are same, so primaryKeysId can be used on source table
                    List<ParameterContext> firstPk = buildPk(result, primaryKeysId, true);
                    List<ParameterContext> lastPk = buildPk(result, primaryKeysId, false);

                    FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

                    PhyTableOperation updatePlan = buildUpdatePlanWithParam(dbIndex, phyTableName,
                        Stream.concat(firstPk.stream(), lastPk.stream()).collect(Collectors.toList()));
                    Cursor updateCursor = ExecutorHelper.execute(updatePlan, extractEc);
                    updateCursor.close(new ArrayList<>());
                } else {
                    ExecutionContext tmpEc = ec.copy();
                    tmpEc.setParams(new Parameters());

                    List<List<Object>> objects = new ArrayList<>();
                    for (Map<Integer, ParameterContext> map : result) {
                        List<Object> object = new ArrayList<>();
                        for (int i = 0; i < selectKeys.size(); i++) {
                            object.add(map.get(selectKeysId.get(i) + 1).getValue());
                        }
                        for (int i = 0; i < targetColumns.size(); i++) {
                            object.add(null);
                        }
                        objects.add(object);
                    }

                    for (int i = 0; i < result.size(); i++) {
                        List<Object> object = objects.get(i);
                        List<ParameterContext> parameterContexts = new ArrayList<>();
                        Row row = new ArrayRow(selectCursorMeta, object.toArray());
                        List<Object> targetObject = new ArrayList<>();

                        for (int j = 0; j < rexNodes.size(); j++) {
                            Object value = RexUtils.getValueFromRexNode(rexNodes.get(j), row, tmpEc);
                            targetObject.add(value);
                            row.setObject(selectKeys.size() + j, value);
                        }

                        Row targetRow = new ArrayRow(targetCursorMeta, targetObject.toArray());
                        for (int j = 0; j < targetColumns.size(); j++) {
                            // We do not care param index here, rewrite when building update
                            final ParameterContext parameterContext =
                                com.alibaba.polardbx.executor.gsi.utils.Transformer.buildColumnParam(targetRow, j);
                            parameterContexts.add(parameterContext);
                        }

                        parameterContexts.addAll(buildPkSingleRow(result.get(i), primaryKeysId));

                        PhyTableOperation updatePlan =
                            buildUpdateSingleRowPlanWithParam(dbIndex, phyTableName, parameterContexts);
                        Cursor updateCursor = ExecutorHelper.execute(updatePlan, extractEc);
                        updateCursor.close(new ArrayList<>());
                    }
                }
            }
        }

        // Commit and close extract statement
        extractEc.getTransaction().commit();
        return result;
    }

    private static List<ParameterContext> buildPkSingleRow(Map<Integer, ParameterContext> row,
                                                           List<Integer> primaryKeysId) {
        return primaryKeysId.stream().map(i -> row.get(i + 1)).collect(Collectors.toList());
    }

    private static List<ParameterContext> buildPk(List<Map<Integer, ParameterContext>> batchResult,
                                                  List<Integer> primaryKeysId, boolean isFirst) {
        List<ParameterContext> pk = null;
        if (GeneralUtil.isNotEmpty(batchResult)) {
            Map<Integer, ParameterContext> row = isFirst ? batchResult.get(0) : batchResult.get(batchResult.size() - 1);
            pk = buildPkSingleRow(row, primaryKeysId);
        }
        return pk;
    }

    private PhyTableOperation buildUpdateSingleRowPlanWithParam(String dbIndex, String phyTable,
                                                                List<ParameterContext> params) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));
        for (int j = 0; j < params.size(); ++j) {
            planParams.put(j + 2, new ParameterContext(params.get(j).getParameterMethod(),
                new Object[] {j + 2, params.get(j).getArgs()[1]}));
        }

        PhyTableOperation targetPhyOp = this.planUpdateSingleRow;
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(planParams);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

        return plan;
    }

    private PhyTableOperation buildUpdatePlanWithParam(String dbIndex, String phyTable, List<ParameterContext> params) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        int nextParamIndex = 2;

        // Parameters for where(DNF)
        final int pkNumber = params.size() / 2;

        for (int i = 0; i < pkNumber; ++i) {
            for (int j = 0; j <= i; ++j) {
                planParams.put(nextParamIndex,
                    new ParameterContext(params.get(j).getParameterMethod(),
                        new Object[] {nextParamIndex, params.get(j).getArgs()[1]}));
                nextParamIndex++;
            }
        }

        for (int i = 0; i < pkNumber; ++i) {
            for (int j = 0; j <= i; ++j) {
                planParams.put(nextParamIndex,
                    new ParameterContext(params.get(pkNumber + j).getParameterMethod(),
                        new Object[] {nextParamIndex, params.get(pkNumber + j).getArgs()[1]}));
                nextParamIndex++;
            }
        }

        PhyTableOperation targetPhyOp = this.planUpdateWithMinAndMax;
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(planParams);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

        return plan;
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        return GsiUtils.getPhyTablesForBackFill(schemaName, sourceTableName);
    }

    /**
     * Check whether all dn using XProtocol
     */
    public static boolean isAllDnUseXDataSource(TopologyHandler topologyHandler) {
        return topologyHandler.getGroupNames().stream()
            .allMatch(groupName -> Optional.ofNullable(topologyHandler.get(groupName))
                .map((Function<IGroupExecutor, Object>) IGroupExecutor::getDataSource)
                .map(ds -> ds instanceof TGroupDataSource && ((TGroupDataSource) ds).isXDataSource()).orElse(false));
    }

    private List<Map<Integer, ParameterContext>> executeAndGetReturning(String dbIndex, String phyTable,
                                                                        List<ParameterContext> lowerBound,
                                                                        List<ParameterContext> upperBound,
                                                                        ExecutionContext executionContext) {
        final String currentReturning = executionContext.getReturning();
        // We only need primary key from returning
        executionContext.setReturning(String.join(",", primaryKeys));
        boolean withLowerBound = GeneralUtil.isNotEmpty(lowerBound);
        boolean withUpperBound = GeneralUtil.isNotEmpty(upperBound);
        PhyTableOperation updatePlan = buildUpdateReturningPlanWithParam(dbIndex, phyTable,
            Stream.concat(lowerBound.stream(), upperBound.stream()).collect(Collectors.toList()), withLowerBound,
            withUpperBound);

        try {
            Cursor extractCursor = ExecutorHelper.execute(updatePlan, executionContext);
            final List<Map<Integer, ParameterContext>> result;
            try {
                result = buildBatchParam(extractCursor);
            } finally {
                extractCursor.close(new ArrayList<>());
            }
            return result;
        } finally {
            executionContext.setReturning(currentReturning);
        }
    }

    /**
     * Build plan for physical select.
     *
     * @param params pk column value of last batch
     * @return built plan
     */
    protected PhyTableOperation buildUpdateReturningPlanWithParam(String dbIndex, String phyTable,
                                                                  List<ParameterContext> params, boolean withLowerBound,
                                                                  boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        int nextParamIndex = 2;

        // Parameters for where(DNF)
        final int pkNumber = params.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
        if (withLowerBound) {
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }
        if (withUpperBound) {
            final int base = withLowerBound ? pkNumber : 0;
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(base + j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(base + j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }

        // Parameters for limit
        planParams.put(nextParamIndex,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {nextParamIndex, batchSize}));

        PhyTableOperation targetPhyOp = !withLowerBound ? planUpdateReturningWithMax :
            (withUpperBound ? planUpdateReturningWithMinAndMax : planUpdateReturningWithMin);
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(planParams);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

        return plan;
    }

    /**
     * Build batch insert parameter, and
     * 1. Only add primary key, fill other columns with NULL
     *
     * @param cursor result cursor of select
     * @return batch parameters for insert
     */
    private List<Map<Integer, ParameterContext>> buildBatchParam(Cursor cursor) {
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();

        Row row;
        while ((row = cursor.next()) != null) {
            final Map<Integer, ParameterContext> params = new HashMap<>(tableColumns.size());
            // fill select key
            for (int j = 0; j < selectKeysId.size(); j++) {
                final ParameterContext parameterContext =
                    com.alibaba.polardbx.executor.gsi.utils.Transformer.buildColumnParam(row, j);
                params.put(selectKeysId.get(j) + 1, parameterContext);
            }
            // fill non-select key with NULL
            for (int j = 0; j < tableColumns.size(); j++) {
                if (!selectKeySet.contains(j)) {
                    params.put(j + 1, null);
                }
            }
            batchParams.add(params);
        }
        return batchParams;
    }
}