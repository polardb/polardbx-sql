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

package com.alibaba.polardbx.executor.gsi.backfill;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.PhyOperationBuilderCommon;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Fill update data into index table
 */
public class Updater extends PhyOperationBuilderCommon {

    private final String schemaName;
    private final List<String> tableNames; // For table with multiple clustered GSI.
    private final SqlUpdate sqlUpdate;
    private final List<String> setColumns;
    /**
     * <pre>
     * SELECT pk0, ... , pkn, sk_primary_0,sqlInsertIgnore ... , sk_primary_n, sk_index_0, ... , sk_index_n
     * FROM {logical_index_table}
     * WHERE pk0 <=> ? AND ... AND pkn <=> ?
     *   AND sk_primary_0 <=> ? AND ... AND sk_primary_n <=> ?
     *   AND sk_index_0 <=> ? AND ... AND sk_index_n <=> ?
     * LIMIT 1
     * </pre>
     */
    private final BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc;

    private final ITransactionManager tm;

    private Updater(String schemaName, List<String> tableNames, SqlUpdate sqlUpdate, List<String> setColumns,
                    BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc) {
        this.schemaName = schemaName;
        this.tableNames = tableNames;
        this.sqlUpdate = sqlUpdate;
        this.setColumns = setColumns;
        this.executeFunc = executeFunc;
        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
    }

    public static Updater create(String schemaName, String primaryTable, List<String> indexTables, List<String> columns,
                                 BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc) {
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

        // Construct target table
        final SqlNode targetTableParam = BuildPlanUtils.buildTargetTable();

        // Construct targetColumnList
        final AtomicInteger paramIndex = new AtomicInteger(2);
        final List<SqlIdentifier> targetColumns = new ArrayList<>();
        final List<SqlDynamicParam> sourceExpressions = new ArrayList<>();

        columns.forEach(c -> {
            final int index = paramIndex.getAndIncrement();
            targetColumns.add(new SqlIdentifier(ImmutableList.of(c), SqlParserPos.ZERO));
            sourceExpressions.add(new SqlDynamicParam(index, SqlParserPos.ZERO));
        });

        final TableMeta primaryTableMeta = optimizerContext.getLatestSchemaManager().getTable(primaryTable);
        final List<String> conditionColumns = new ArrayList<>();

        //条件为主表主键相等
        conditionColumns.addAll(GlobalIndexMeta.getPrimaryKeys(primaryTableMeta));

        final SqlNode condition = RelUtils.buildAndCondition(conditionColumns, paramIndex);

        final SqlUpdate sqlUpdate = new SqlUpdate(SqlParserPos.ZERO,
            targetTableParam,
            new SqlNodeList(targetColumns, SqlParserPos.ZERO),
            new SqlNodeList(sourceExpressions, SqlParserPos.ZERO),
            condition,
            null,
            null
        );

        return new Updater(schemaName,
            indexTables,
            sqlUpdate,
            columns,
            executeFunc);
    }

    /**
     * Update into index table
     */
    public int fillUpdateIndex(List<Map<Integer, ParameterContext>> batchParams, ExecutionContext baseEc,
                               Supplier<Boolean> checker) {
        if (batchParams.isEmpty()) {
            return 0;
        }

        return GsiUtils.wrapWithDistributedTrx(tm, baseEc, (updateEc) -> {
            int result = 0;
            try {
                result = applyBatch(batchParams, updateEc.copy());

                return checkBeforeCommit(checker, updateEc, result);
            } catch (TddlNestableRuntimeException e) {
                SQLRecorderLogger.ddlLogger
                    .warn(MessageFormat.format("[{0}] update clustered index data failed first row: {1} cause: {2}",
                        baseEc.getTraceId(),
                        GsiUtils.rowToString(batchParams.get(0)),
                        e.getMessage()));
                throw e;
            }
        });
    }

    private static Integer checkBeforeCommit(Supplier<Boolean> checker, ExecutionContext updateEc, int result) {
        if (checker.get()) {
            updateEc.getTransaction().commit();
            return result;
        } else {
            updateEc.getTransaction().rollback();
            // This means extractor's connection fail. Throw a special error.
            throw new TddlNestableRuntimeException(
                "Updater check error. Fail to commit in backfill extractor. Please retry or recover DDL later.");
        }
    }

    /**
     * Batch update
     *
     * @param batchParams Batch parameters
     * @param newEc Copied ExecutionContext
     * @return Affected rows
     */
    private int applyBatch(List<Map<Integer, ParameterContext>> batchParams, ExecutionContext newEc) {
        // Construct params for each batch
        Parameters parameters = new Parameters();
        parameters.setBatchParams(batchParams);

        newEc.setParams(parameters);

        return executeUpdate(newEc);
    }

    /**
     * Single update
     *
     * @param param Parameter
     * @param newEc Copied ExecutionContext
     * @return Affected rows
     */
    private int applyRow(Map<Integer, ParameterContext> param, ExecutionContext newEc) {
        Parameters parameters = new Parameters();
        parameters.setParams(param);

        newEc.setParams(parameters);

        return executeUpdate(newEc);
    }

    private List<List<Object>> getSelectResults(Parameters parameters) {
        List<List<Object>> results;
        if (parameters.isBatch()) {
            List<Map<Integer, ParameterContext>> batchParams = parameters.getBatchParameters();
            results = new ArrayList<>(batchParams.size());
            for (Map<Integer, ParameterContext> batchParam : batchParams) {
                List<Object> row = new ArrayList<>(batchParam.size());
                for (int i = 1; i <= batchParam.size(); i++) {
                    row.add(batchParam.get(i).getValue());
                }
                results.add(row);
            }
        } else {
            Map<Integer, ParameterContext> params = parameters.getCurrentParameter();
            results = new ArrayList<>();
            List<Object> row = new ArrayList<>(params.size());
            for (int i = 1; i <= params.size(); i++) {
                row.add(params.get(i).getValue());
            }
            results.add(row);
        }
        return results;
    }

    private List<RelNode> buildUpdateForUpdater(ExecutionContext executionContext) {
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

        List<RelNode> newPhysicalPlans = new ArrayList<>();
        final RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = RelOptUtil.createDmlRowType(SqlKind.UPDATE, typeFactory);

        // For each table in target
        for (String tableName : tableNames) {
            final TableMeta indexTableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);
            final List<String> selectColumns = indexTableMeta.getWriteColumns()
                .stream()
                .map(ColumnMeta::getName)
                .map(String::toUpperCase)
                .collect(Collectors.toList());

            List<String> primaryKeyNames = GlobalIndexMeta.getPrimaryKeys(indexTableMeta);
            List<Integer> primaryKeyIndexes = primaryKeyNames.stream()
                .map(selectColumns::indexOf)
                .collect(Collectors.toList());

            // sharding key indexes and ColumnMetas
            List<String> shardingKeyNames = GlobalIndexMeta.getShardingKeys(indexTableMeta, schemaName);
            List<ColumnMeta> shardingKeyMetas = new ArrayList<>(shardingKeyNames.size());
            List<Integer> shardingKeyIndexes = new ArrayList<>(shardingKeyNames.size());
            for (String shardingKey : shardingKeyNames) {
                shardingKeyIndexes.add(selectColumns.indexOf(shardingKey));
                shardingKeyMetas.add(indexTableMeta.getColumnIgnoreCase(shardingKey));
            }
            List<List<Object>> selectedResults = getSelectResults(executionContext.getParams());
            List<String> relShardingKeyNames = BuildPlanUtils.getRelColumnNames(indexTableMeta, shardingKeyMetas);
            Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
                BuildPlanUtils.buildResultForShardingTable(schemaName,
                    indexTableMeta.getTableName(),
                    selectedResults,
                    relShardingKeyNames,
                    shardingKeyIndexes,
                    shardingKeyMetas,
                    primaryKeyIndexes,
                    executionContext);

            List<Integer> setColumnsIndexes = new ArrayList<>();
            setColumns.forEach(c -> {
                setColumnsIndexes.add(selectColumns.indexOf(c.toUpperCase()));
            });

            // build physical plans
            for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
                String targetDb = dbEntry.getKey();
                for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {

                    String targetTb = tbEntry.getKey();
                    List<Pair<Integer, List<Object>>> indexAndPkValues = tbEntry.getValue();

                    for (Pair<Integer, List<Object>> indexAndPkValue : indexAndPkValues) {
                        int rowIndex = indexAndPkValue.getKey();
                        List<Object> pkValues = indexAndPkValue.getValue();

                        Map<Integer, ParameterContext> currentParams = new HashMap<>();

                        int paramIndex = PlannerUtils.TABLE_NAME_PARAM_INDEX + 2;
                        currentParams
                            .put(paramIndex, PlannerUtils.buildParameterContextForTableName(targetTb, paramIndex));
                        final AtomicInteger paramIndexAtomic = new AtomicInteger(2);
                        // put SET clause
                        for (Integer setColumnIndex : setColumnsIndexes) {
                            paramIndex = paramIndexAtomic.getAndIncrement();
                            ParameterContext newPC = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                                paramIndex,
                                selectedResults.get(rowIndex).get(setColumnIndex)});
                            currentParams.put(paramIndex, newPC);
                        }

                        // put WHERE clause
                        for (Object pkValue : pkValues) {
                            paramIndex = paramIndexAtomic.getAndIncrement();
                            ParameterContext newPC = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                                paramIndex,
                                pkValue});
                            currentParams.put(paramIndex, newPC);
                        }

                        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();

                        List<String> logTables = ImmutableList.of(indexTableMeta.getTableName());
                        List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                        BytesSql sql = RelUtils.toNativeBytesSql(sqlUpdate, DbType.MYSQL);

                        buildParams.setSchemaName(schemaName);
                        buildParams.setLogTables(logTables);
                        buildParams.setGroupName(targetDb);
                        buildParams.setPhyTables(tableNames);

                        buildParams.setCluster(cluster);
                        buildParams.setTraitSet(traitSet);
                        buildParams.setRowType(rowType);
                        buildParams.setBytesSql(sql);
                        buildParams.setSqlKind(sqlUpdate.getKind());
                        buildParams.setDbType(DbType.MYSQL);
                        buildParams.setDynamicParams(currentParams);
                        buildParams.setBatchParameters(null);
                        PhyTableOperation operation = PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(buildParams);
                        newPhysicalPlans.add(operation);
                    }
                }
            }
        }

        executionContext.setParams(new Parameters());
        return newPhysicalPlans;
    }

    private int executeUpdate(ExecutionContext executionContext) {
        List<RelNode> sqlUpdates = buildUpdateForUpdater(executionContext);

        List<Cursor> cursors;
        try {
            cursors = executeFunc.apply(sqlUpdates, executionContext);
        } catch (TddlNestableRuntimeException e) {
            throw e;
        }

        return ExecUtils.getAffectRowsByCursors(cursors, false);
    }
}
