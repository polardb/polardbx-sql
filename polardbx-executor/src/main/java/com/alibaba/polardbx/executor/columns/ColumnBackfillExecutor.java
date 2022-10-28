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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
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
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
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
    final BitSet primaryKeySet;

    public ColumnBackfillExecutor(String schemaName, String tableName, long batchSize, long speedMin, long speedLimit,
                                  long parallelism, PhyTableOperation planSelectWithMax,
                                  PhyTableOperation planSelectWithMin, PhyTableOperation planSelectWithMinAndMax,
                                  PhyTableOperation planUpdateWithMinAndMax, PhyTableOperation planSelectMaxPk,
                                  PhyTableOperation planUpdateReturningWithMin,
                                  PhyTableOperation planUpdateReturningWithMax,
                                  PhyTableOperation planUpdateReturningWithMinAndMax,
                                  PhyTableOperation planSelectSample,
                                  PhyTableOperation planSelectMinAndMaxSample,
                                  List<Integer> primaryKeysId,
                                  List<String> primaryKeys, BitSet primaryKeySet, List<String> tableColumns) {
        super(schemaName, tableName, tableName, batchSize, speedMin, speedLimit, parallelism, planSelectWithMax,
            planSelectWithMin, planSelectWithMinAndMax, planSelectMaxPk,
            planSelectSample, planSelectMinAndMaxSample, primaryKeysId);

        this.planUpdateWithMinAndMax = planUpdateWithMinAndMax;
        this.planUpdateReturningWithMin = planUpdateReturningWithMin;
        this.planUpdateReturningWithMax = planUpdateReturningWithMax;
        this.planUpdateReturningWithMinAndMax = planUpdateReturningWithMinAndMax;
        this.primaryKeySet = primaryKeySet;
        this.primaryKeys = primaryKeys;
        this.tableColumns = tableColumns;
    }

    public static ColumnBackfillExecutor create(String schemaName, String tableName, String sourceColumn,
                                                String targetColumn, ExecutionContext ec) {
        long batchSize = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_BATCH_SIZE);
        long speedLimit = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_LIMITATION);
        long speedMin = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_SPEED_MIN);
        long parallelism = ec.getParamManager().getLong(ConnectionParams.GSI_BACKFILL_PARALLELISM);

        // Build select plan
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);
        // tableColumns do not include target column, it should be hidden at this moment
        final List<String> tableColumns = tableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);
        ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, tableName, tableName);
        List<String> primaryKeys = info.getPrimaryKeys();
        final BitSet primaryKeySet = new BitSet(tableColumns.size());
        for (String primaryKey : primaryKeys) {
            for (int i = 0; i < tableColumns.size(); i++) {
                if (primaryKey.equalsIgnoreCase(tableColumns.get(i))) {
                    primaryKeySet.set(i);
                }
            }
        }

        return new ColumnBackfillExecutor(schemaName,
            tableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            builder.buildSelectForBackfill(tableMeta, primaryKeys, primaryKeys, false, true,
                SqlSelect.LockMode.EXCLUSIVE_LOCK),
            builder.buildSelectForBackfill(tableMeta, primaryKeys, primaryKeys, true, false,
                SqlSelect.LockMode.EXCLUSIVE_LOCK),
            builder.buildSelectForBackfill(tableMeta, primaryKeys, primaryKeys, true, true,
                SqlSelect.LockMode.EXCLUSIVE_LOCK),
            builder.buildUpdateForColumnBackfill(tableMeta, sourceColumn, targetColumn, primaryKeys, true, true),
            builder.buildSelectMaxPkForBackfill(tableMeta, primaryKeys),
            builder.buildUpdateReturningForColumnBackfill(tableMeta, sourceColumn, targetColumn, primaryKeys, true,
                false),
            builder.buildUpdateReturningForColumnBackfill(tableMeta, sourceColumn, targetColumn, primaryKeys, false,
                true),
            builder.buildUpdateReturningForColumnBackfill(tableMeta, sourceColumn, targetColumn, primaryKeys, true,
                true),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys(), info.getPrimaryKeys(),
                false, false),
            builder.buildSqlSelectForSample(info.getSourceTableMeta(), info.getPrimaryKeys(), info.getPrimaryKeys(),
                true, true),
            info.getPrimaryKeysId(),
            info.getPrimaryKeys(),
            primaryKeySet,
            tableColumns);
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
            executorContext.getStorageInfoManager().supportsReturning() && extractEc.getParamManager()
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
                // Since source table and target table are same, so primaryKeysId can be used on source table
                List<ParameterContext> firstPk = buildPk(result, primaryKeysId, true);
                List<ParameterContext> lastPk = buildPk(result, primaryKeysId, false);

                FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

                PhyTableOperation updatePlan = buildUpdateReturningPlanWithParam(dbIndex, phyTableName,
                    Stream.concat(firstPk.stream(), lastPk.stream()).collect(Collectors.toList()));
                Cursor updateCursor = ExecutorHelper.execute(updatePlan, extractEc);
                updateCursor.close(new ArrayList<>());
            }
        }

        // Commit and close extract statement
        extractEc.getTransaction().commit();
        return result;
    }

    private static List<ParameterContext> buildPk(List<Map<Integer, ParameterContext>> batchResult,
                                                  List<Integer> primaryKeysId, boolean isFirst) {
        List<ParameterContext> pk = null;
        if (GeneralUtil.isNotEmpty(batchResult)) {
            Map<Integer, ParameterContext> row = isFirst ? batchResult.get(0) : batchResult.get(batchResult.size() - 1);
            pk = primaryKeysId.stream().map(i -> row.get(i + 1)).collect(Collectors.toList());
        }
        return pk;
    }

    private PhyTableOperation buildUpdateReturningPlanWithParam(String dbIndex, String phyTable,
                                                                List<ParameterContext> params) {
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

        // Get ExecutionPlan
//        PhyTableOperation plan = new PhyTableOperation(planUpdateWithMinAndMax);
//        plan.setDbIndex(dbIndex);
//        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
//        plan.setParam(planParams);

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
        return GsiUtils.getPhyTables(schemaName, sourceTableName);
    }

    /**
     * Check whether all dn using XProtocol
     */
    private static boolean isAllDnUseXDataSource(TopologyHandler topologyHandler) {
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

        // Get ExecutionPlan
//        PhyTableOperation plan;
//        if (!withLowerBound) {
//            plan = new PhyTableOperation(planUpdateReturningWithMax);
//        } else {
//            plan = new PhyTableOperation(withUpperBound ? planUpdateReturningWithMinAndMax : planUpdateReturningWithMin);
//        }
//        plan.setDbIndex(dbIndex);
//        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
//        plan.setParam(planParams);

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
            // fill primary key
            for (int j = 0; j < primaryKeysId.size(); j++) {
                final ParameterContext parameterContext =
                    com.alibaba.polardbx.executor.gsi.utils.Transformer.buildColumnParam(row, j);
                params.put(primaryKeysId.get(j) + 1, parameterContext);
            }
            // fill non-primary key with NULL
            for (int j = 0; j < tableColumns.size(); j++) {
                if (!primaryKeySet.get(j)) {
                    params.put(j + 1, null);
                }
            }
            batchParams.add(params);
        }
        return batchParams;
    }
}