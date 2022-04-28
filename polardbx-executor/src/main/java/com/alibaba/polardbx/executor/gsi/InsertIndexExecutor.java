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

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.BuildInsertValuesVisitor;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.buildParameterContextForTableName;

public class InsertIndexExecutor extends IndexExecutor {

    private static final Logger logger = LoggerFactory.getLogger(InsertIndexExecutor.class);

    public InsertIndexExecutor(BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                               String schemaName) {
        super(executeFunc, schemaName);
    }

    /**
     * Sequence calculation is based on SqlInsert, not LogicalInsert, so here we
     * use SqlInsert.
     *
     * @param tableName base table name
     * @param sqlInsert logical INSERT or REPLACE
     * @param physicalPlan physical plans
     * @param shardResults sharding results, corresponding to physicalPlan
     * @return affected rows on the base table
     */
    public int execute(String tableName, SqlInsert sqlInsert, RelNode logicalPlan, List<RelNode> physicalPlan,
                       List<PhyTableInsertSharder.PhyTableShardResult> shardResults,
                       ExecutionContext executionContext) {
        checkUniqueKeyNotNull(sqlInsert, tableName, executionContext);

        List<TableMeta> indexMetas = GlobalIndexMeta.getIndex(tableName, schemaName, executionContext);

        if (sqlInsert.getKind() == SqlKind.INSERT) {
            // UPSERT comes first. In INSERT IGNORE ... ON DUPLICATE KEY UPDATE
            // ..., ON DUPLICATE KEY UPDATE has preference.
            if (sqlInsert.getUpdateList() != null && sqlInsert.getUpdateList().size() > 0) {
                // insert on duplicate key update
                return executeInsertDuplicate(sqlInsert,
                    logicalPlan,
                    physicalPlan,
                    shardResults,
                    indexMetas,
                    executionContext,
                    tableName);
            } else if (sqlInsert.getModifierNode(SqlDmlKeyword.IGNORE) != null) {
                // insert ignore
                return executeInsertIgnore(sqlInsert,
                    logicalPlan,
                    physicalPlan,
                    shardResults,
                    indexMetas,
                    executionContext,
                    tableName);
            } else {
                // insert
                return executeInsert(sqlInsert, logicalPlan, physicalPlan, indexMetas, tableName, executionContext);
            }
        } else {
            // replace
            return executeReplace(sqlInsert, logicalPlan, physicalPlan, indexMetas, executionContext, tableName);
        }
    }

    /**
     * INSERT without IGNORE or ON DUPLICATE KEY UPDATE. Just pick some columns
     * to insert into the index tables.
     *
     * @param sqlInsert the logical insert on the base table
     * @param physicalPlans the physical insert on the base table
     * @return affected rows
     */
    private int executeInsert(SqlInsert sqlInsert, RelNode logicalPlan, List<RelNode> physicalPlans,
                              List<TableMeta> indexMetas,
                              String baseTableName, ExecutionContext executionContext) {
        // insert into the base table
        List<Cursor> cursors = executeFunc.apply(physicalPlans, executionContext);
        int affectRows = ExecUtils.getAffectRowsByCursors(cursors, false);

        for (TableMeta indexMeta : indexMetas) {
            if (GlobalIndexMeta.canWrite(executionContext, indexMeta)) {
                // build new logical insert, with only columns in index
                SqlInsert planOnIndex = buildIndexLogicalPlan(sqlInsert, indexMeta);

                int indexAffectRows = insertIntoTable(logicalPlan, planOnIndex, indexMeta, executionContext, false);
                // check the affected rows
                if (indexAffectRows != affectRows) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_AFFECT_ROWS_DIFFERENT,
                        baseTableName,
                        String.valueOf(affectRows),
                        indexMeta.getTableName(),
                        String.valueOf(indexAffectRows));
                }
            }
        }

        return affectRows;
    }

    /**
     * Insert by a logical insert, by sharding, building physical plan and
     * executing.
     *
     * @param logicalInsert the logical relnode of insert
     * @param sqlInsert logical insert on this table
     * @param indexMeta could be base table or index table
     * @return affected rows
     */
    private int insertIntoTable(RelNode logicalInsert, SqlInsert sqlInsert, TableMeta indexMeta,
                                ExecutionContext executionContext,
                                boolean mayInsertDuplicate) {
        return insertIntoTable(logicalInsert, sqlInsert, indexMeta, schemaName, executionContext, executeFunc,
            mayInsertDuplicate);
    }

    public static int insertIntoTable(RelNode logicalInsert,
                                      SqlInsert sqlInsert, TableMeta tableMeta,
                                      String schemaName,
                                      ExecutionContext executionContext,
                                      BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                      boolean mayInsertDuplicate) {
        return insertIntoTable(logicalInsert, sqlInsert, tableMeta, "", schemaName, executionContext, executeFunc,
            mayInsertDuplicate, true);
    }

    public static int backfillIntoPartitionedTable(RelNode logicalInsert,
                                                   SqlInsert sqlInsert, TableMeta tableMeta,
                                                   String schemaName,
                                                   ExecutionContext executionContext,
                                                   BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                                   boolean mayInsertDuplicate,
                                                   PartitionInfo newPartition) {
        Map<String, Map<String, List<Integer>>> shardResults;
        if (tableMeta.getPartitionInfo().isBroadcastTable()) {
            // { targetDb : { targetTb : [valueIndex1, valueIndex2] } }
            shardResults = new HashMap<>();
            List<Pair<String, String>> groupAndPhyTableList =
                PartitionInfoUtil.getInvisiblePartitionPhysicalLocation(newPartition);
            Parameters parameters = executionContext.getParams();
            int rowCount = 0;
            if (parameters.isBatch()) {
                List<Map<Integer, ParameterContext>> batchParams = parameters.getBatchParameters();
                rowCount = batchParams.size();
            } else {
                rowCount = ((SqlCall) sqlInsert.getSource()).getOperandList().size();
            }
            assert rowCount > 0;
            for (Pair<String, String> dbAndTbPair : groupAndPhyTableList) {
                IntStream stream = IntStream.range(0, rowCount);
                shardResults.computeIfAbsent(dbAndTbPair.getKey(), o -> new HashMap<>())
                    .computeIfAbsent(dbAndTbPair.getValue(), o -> new ArrayList<>()).addAll(stream.boxed().collect(
                    Collectors.toList()));
            }
        } else {
            // shard to get value indices
            shardResults = BuildPlanUtils.shardValues(sqlInsert,
                tableMeta,
                executionContext,
                schemaName, newPartition);
        }
        return insertIntoTable(logicalInsert, sqlInsert, tableMeta, "", schemaName, executionContext, executeFunc,
            mayInsertDuplicate, shardResults);
    }

    public static int insertIntoTable(RelNode logicalInsert, SqlInsert sqlInsert, TableMeta tableMeta,
                                      String targetGroup, String schemaName,
                                      ExecutionContext executionContext,
                                      BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                      boolean mayInsertDuplicate,
                                      boolean mayForGsi, String partName) {
        // shard to get value indices
        Map<String, Map<String, List<Integer>>> shardResults = StringUtils.isEmpty(partName)
            ? BuildPlanUtils.shardValues(sqlInsert,
            tableMeta,
            executionContext,
            schemaName, null)
            : BuildPlanUtils.shardValuesByPartName(sqlInsert,
            tableMeta,
            executionContext,
            schemaName, null, partName);

        final OptimizerContext oc = OptimizerContext.getContext(schemaName);
        assert null != oc;
        final boolean isBroadcast = oc.getRuleManager().isBroadCast(tableMeta.getTableName());
        if (isBroadcast && mayForGsi) {
            // build shardResults for alter table repartition when gsi is broadcast table
            Map<String, List<Integer>> values = new HashMap<>();
            for (Map<String, List<Integer>> entry : shardResults.values()) {
                assert entry.size() == 1;
                String phyTableName = entry.keySet().stream().findFirst().get();
                if (values.containsKey(phyTableName)) {
                    values.get(phyTableName).addAll(entry.get(phyTableName));
                } else {
                    values.putAll(entry);
                }
            }
            List<String> groupNames = HintUtil.allGroup(schemaName);
            shardResults.clear();
            for (String gn : groupNames) {
                shardResults.put(gn, values);
            }
        }

        return insertIntoTable(logicalInsert, sqlInsert, tableMeta, targetGroup, schemaName, executionContext,
            executeFunc,
            mayInsertDuplicate, shardResults);
    }

    public static int insertIntoTable(RelNode logicalInsert, SqlInsert sqlInsert, TableMeta tableMeta,
                                      String targetGroup, String schemaName,
                                      ExecutionContext executionContext,
                                      BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                      boolean mayInsertDuplicate,
                                      boolean mayForGsi) {
        return insertIntoTable(logicalInsert, sqlInsert, tableMeta, targetGroup, schemaName, executionContext,
            executeFunc,
            mayInsertDuplicate, mayForGsi, "");
    }

    public static int insertIntoTable(RelNode logicalInsert, SqlInsert sqlInsert, TableMeta tableMeta,
                                      String targetGroup, String schemaName,
                                      ExecutionContext executionContext,
                                      BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc,
                                      boolean mayInsertDuplicate,
                                      Map<String, Map<String, List<Integer>>> shardResults) {

        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, executionContext).createRelOptCluster();
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelDataType rowType = RelOptUtil.createDmlRowType(SqlKind.INSERT, typeFactory);

        List<RelNode> newPhysicalPlans = new ArrayList<>();
        // build physical plans
        for (Map.Entry<String, Map<String, List<Integer>>> dbEntry : shardResults.entrySet()) {
            String targetDb = StringUtils.isEmpty(targetGroup) ? dbEntry.getKey() : targetGroup;
            for (Map.Entry<String, List<Integer>> tbEntry : dbEntry.getValue().entrySet()) {
                String targetTb = tbEntry.getKey();
                List<Integer> valueIndices = tbEntry.getValue();

                Map<Integer, ParameterContext> outputParams = new HashMap<>();
                outputParams.put(1, buildParameterContextForTableName(targetTb, 1));
                BuildInsertValuesVisitor visitor = new BuildInsertValuesVisitor(valueIndices,
                    executionContext.getParams(),
                    outputParams);
                SqlInsert newSqlInsert = visitor.visit(sqlInsert);

                PhyTableOperation phyTableModify =
                    new PhyTableOperation(cluster, traitSet, rowType, null, logicalInsert);
                phyTableModify.setKind(newSqlInsert.getKind());
                phyTableModify.setDbIndex(targetDb);
                phyTableModify.setTableNames(ImmutableList.of(ImmutableList.of(targetTb)));

                String sql = RelUtils.toNativeSql(newSqlInsert, DbType.MYSQL);
                phyTableModify.setSqlTemplate(sql);
                phyTableModify.setNativeSqlNode(newSqlInsert);
                phyTableModify.setParam(outputParams);
                phyTableModify.setBatchParameters(null);

                newPhysicalPlans.add(phyTableModify);
            }
        }

        // insert into the index table
        List<Cursor> cursors;
        try {
            cursors = executeFunc.apply(newPhysicalPlans, executionContext);
        } catch (TddlNestableRuntimeException e) {
            // If it's in INSERT IGNORE / INSERT ON DUPLICATE KEY UPDATE /
            // REPLACE, duplicate key error shouldn't happen. But we'are using
            // INSERT instead, which causing duplicate key happens. So it
            // happens, we should rephrase the error message to tell the user
            // that it doesn't support duplicate key in the statement.
            if (mayInsertDuplicate && "23000".equals(e.getSQLState())) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INSERT_DUPLICATE_VALUES);
            } else {
                throw e;
            }
        }

        return ExecUtils.getAffectRowsByCursors(cursors, false);
    }

    /**
     * Modify the target table & column list from the original SqlInsert. VALUES
     * is copied from the original SqlInsert, so it could be in batch mode or
     * not.
     *
     * @param oldSqlInsert logical insert on the base table
     * @return new logical insert on the index table
     */
    private SqlInsert buildIndexLogicalPlan(SqlInsert oldSqlInsert, TableMeta indexMeta) {
        // Construct targetColumnList
        List<Integer> chosenColumnIndexes = new ArrayList<>();
        SqlNodeList newColumnList = buildTargetColumnList(oldSqlInsert, indexMeta, chosenColumnIndexes);

        // Construct values
        SqlCall oldValues = (SqlCall) oldSqlInsert.getSource();
        List<SqlNode> oldRowList = oldValues.getOperandList();
        List<SqlNode> newRowList = new ArrayList<>(oldRowList.size());

        // copy all rows
        for (SqlNode row : oldRowList) {
            SqlCall oldRow = (SqlCall) row;
            List<SqlNode> columnList = oldRow.getOperandList();
            // pick interested columns, just copy the reference
            SqlNode[] chosenColumns = chosenColumnIndexes.stream().map(columnList::get).toArray(SqlNode[]::new);

            SqlNode newRow = new SqlBasicCall(oldRow.getOperator(),
                chosenColumns,
                oldRow.getParserPosition(),
                oldRow.isExpanded(),
                oldRow.getFunctionQuantifier());
            newRowList.add(newRow);
        }

        SqlNode newValues = new SqlBasicCall(oldValues.getOperator(),
            newRowList.toArray(new SqlNode[newRowList.size()]),
            oldValues.getParserPosition(),
            oldValues.isExpanded(),
            oldValues.getFunctionQuantifier());

        // Construct targetTable
        SqlNode targetTable = BuildPlanUtils.buildTargetTable();

        // Construct ON DUPLICATE KEY UPDATE
        SqlNodeList oldUpdateList = oldSqlInsert.getUpdateList();
        SqlNodeList newUpdateList = oldUpdateList;
        if (oldUpdateList != null && oldUpdateList.size() > 0) {
            newUpdateList = new SqlNodeList(oldUpdateList.getParserPosition());
            for (SqlNode node : oldUpdateList) {
                // each node is a call, whose left is an identifier and right is
                // an expression
                SqlNode left = ((SqlCall) node).getOperandList().get(0);
                String colName = ((SqlIdentifier) left).getLastName();
                if (indexMeta.getColumnIgnoreCase(colName) != null) {
                    newUpdateList.add(node);
                }
            }
        }

        return new SqlInsert(oldSqlInsert.getParserPosition(),
            SqlNodeList.EMPTY,
            targetTable,
            newValues,
            newColumnList,
            newUpdateList,
            oldSqlInsert.getBatchSize(),
            oldSqlInsert.getHints());
    }

    /**
     * INSERT IGNORE VALUES. Pick distinct values to insert into the base table
     * and the index table. Why not insert into the base table using the
     * original INSERT IGNORE statement: There's a case when the INSERT
     * statement contains two duplicate keys, and the key doesn't exist in the
     * database.
     *
     * @param sqlInsert logical insert
     * @param physicalPlans physical insert
     * @param shardResults shard results of the base table
     * @return affect rows
     */
    private int executeInsertIgnore(SqlInsert sqlInsert, RelNode logicalPlan, List<RelNode> physicalPlans,
                                    List<PhyTableInsertSharder.PhyTableShardResult> shardResults,
                                    List<TableMeta> indexMetas, ExecutionContext executionContext,
                                    String baseTableName) {
        TableMeta baseTableMeta = executionContext.getSchemaManager(schemaName).getTable(baseTableName);
        List<List<String>> uniqueKeys =
            GlobalIndexMeta.getUniqueKeys(baseTableMeta, true, tm -> true, executionContext);
        Set<String> selectKeysMap = new HashSet<>();
        uniqueKeys.forEach(selectKeysMap::addAll);
        List<String> selectKeys = new ArrayList<>(selectKeysMap);

        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
        List<RelNode> selects = builder.buildSelect(baseTableMeta, physicalPlans, selectKeys, uniqueKeys);

        ExecutionContext selectEC = executionContext.copy();
        // just remove the batch flag
        selectEC.setParams(new Parameters(executionContext.getParams().getCurrentParameter(), false));
        List<Cursor> cursors = executeFunc.apply(selects, selectEC);

        List<Integer> distinctRowIndexes = new ArrayList<>();
        DuplicateValueFinder finder = new DuplicateValueFinder(baseTableMeta,
            sqlInsert.getTargetColumnList(),
            selectKeys, executionContext);
        boolean hasDuplicateValue = false;

        try {
            for (int i = 0; i < physicalPlans.size(); i++) {
                Cursor cursor = cursors.get(i);
                List<List<Object>> duplicateValues = new ArrayList<>();

                Row row;
                while ((row = cursor.next()) != null) {
                    duplicateValues.add(row.getValues());
                }

                cursor.close(new ArrayList<>());
                cursors.set(i, null);

                hasDuplicateValue = hasDuplicateValue || !duplicateValues.isEmpty();

                finder.setPhysicalPlan((PhyTableOperation) physicalPlans.get(i), duplicateValues, shardResults.get(i)
                    .getValueIndices());

                distinctRowIndexes.addAll(finder.getDistinctValueIndices());

                finder.clearPhysicalPlan();
            }
        } finally {
            for (Cursor cursor : cursors) {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }
        }

        int affectRowsInBase = 0;

        // Even if there's no duplicate data, we need to remove IGNORE keyword,
        // so a new plan is always needed.
        if (!hasDuplicateValue) {
            // TODO: remove IGNORE on existing logical and physical plans
        }

        // If there's distinct values, insert them
        if (!distinctRowIndexes.isEmpty()) {

            Collections.sort(distinctRowIndexes);
            ExecutionContext insertEC = executionContext.copy();
            SqlInsert newLogicalInsert = buildLogicalPlanWithDistinctData(sqlInsert, distinctRowIndexes, insertEC);

            // insert into the base table
            affectRowsInBase = insertIntoTable(logicalPlan, newLogicalInsert, baseTableMeta, insertEC, true);

            for (TableMeta indexMeta : indexMetas) {
                if (GlobalIndexMeta.canWrite(executionContext, indexMeta)) {
                    // build new logical insert, with only columns in index
                    SqlInsert planOnIndex = buildIndexLogicalPlan(newLogicalInsert, indexMeta);

                    int affectRowsInIndex = insertIntoTable(logicalPlan, planOnIndex, indexMeta, insertEC, true);
                    // check the affected rows
                    if (affectRowsInIndex != affectRowsInBase) {
                        // because IGNORE is removed, it's impossible here
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_AFFECT_ROWS_DIFFERENT,
                            baseTableName,
                            String.valueOf(affectRowsInBase),
                            indexMeta.getTableName(),
                            String.valueOf(affectRowsInIndex));
                    }
                }
            }
        }

        return affectRowsInBase;
    }

    /**
     * Build a new logical insert with distinct rows.
     *
     * @param oldSqlInsert the original logical insert with all rows
     * @param selectedRowIndices the indices or distinct rows
     * @param executionContext shallow copied ExecutionContext
     * @return the new logical insert
     */
    protected static SqlInsert buildLogicalPlanWithDistinctData(SqlInsert oldSqlInsert,
                                                                List<Integer> selectedRowIndices,
                                                                ExecutionContext executionContext) {
        SqlCall values = (SqlCall) oldSqlInsert.getSource();
        List<SqlNode> rows = values.getOperandList();

        Parameters oldParameters = executionContext.getParams();
        SqlNode[] selectedRows;
        if (oldParameters.isBatch()) {
            // select some rows to put into the new params
            List<Map<Integer, ParameterContext>> newBatchParams = selectedRowIndices.stream()
                .map(oldParameters.getBatchParameters()::get)
                .collect(Collectors.toList());
            Parameters newParameters = new Parameters(oldParameters.getCurrentParameter(), true);
            newParameters.setBatchParams(newBatchParams);
            executionContext.setParams(newParameters);

            selectedRows = rows.toArray(new SqlNode[rows.size()]);
        } else {
            selectedRows = selectedRowIndices.stream().map(rows::get).toArray(SqlNode[]::new);
        }

        SqlNode newValues = new SqlBasicCall(values.getOperator(), selectedRows, values.getParserPosition());

        return new SqlInsert(oldSqlInsert.getParserPosition(),
            SqlNodeList.EMPTY,
            oldSqlInsert.getTargetTable(),
            newValues,
            oldSqlInsert.getTargetColumnList(),
            SqlNodeList.EMPTY,
            oldSqlInsert.getBatchSize(),
            oldSqlInsert.getHints());
    }

    /**
     * Build a new logical INSERT ON DUPLICATE KEY UPDATE with rows that
     * conflict with existing data. That requires that VALUES list contains
     * sharding keys and unique keys, whose values are consistent with existing
     * data, instead of original INSERT statement. Because our goal is to
     * replace those values.
     *
     * @param oldSqlInsert the original logical insert with all rows
     * @param duplicateColumnNames column names that corresponds to
     * duplicateRows
     * @param duplicateRows existing data, [(rowIndex, duplicateValues)]
     * @param executionContext shallow copied ExecutionContext
     * @return the new logical insert
     */
    protected static SqlInsert buildLogicalPlanWithDuplicateData(SqlInsert oldSqlInsert,
                                                                 List<String> duplicateColumnNames,
                                                                 List<Pair<Integer, List<Object>>> duplicateRows,
                                                                 ExecutionContext executionContext) {
        SqlCall values = (SqlCall) oldSqlInsert.getSource();
        List<SqlNode> rows = values.getOperandList();
        List<Integer> columnIndexes = BuildPlanUtils.getColumnIndexesInInsert(oldSqlInsert, duplicateColumnNames);

        Parameters oldParameters = executionContext.getParams();
        SqlNode[] selectedRows;
        if (oldParameters.isBatch()) {

            List<Map<Integer, ParameterContext>> oldBatchParams = oldParameters.getBatchParameters();
            // use the max index of old batch params as the next index
            int maxIndex = Collections.max(oldBatchParams.get(0).keySet());
            SqlCall oldRow = (SqlCall) rows.get(0);
            List<SqlNode> oldRowExpressions = oldRow.getOperandList();
            SqlNode[] newRowExpressions = new SqlNode[oldRowExpressions.size()];

            // change original node to new DynamicParam node
            int nextIndex = maxIndex;
            for (int i = 0; i < oldRowExpressions.size(); i++) {
                if (columnIndexes.contains(i)) {
                    newRowExpressions[i] = new SqlDynamicParam(nextIndex++, SqlParserPos.ZERO);
                } else {
                    newRowExpressions[i] = oldRowExpressions.get(i);
                }
            }
            selectedRows = new SqlNode[] {new SqlBasicCall(oldRow.getOperator(), newRowExpressions, SqlParserPos.ZERO)};

            // add ParameterContext to the batch params
            List<Map<Integer, ParameterContext>> newBatchParams = new ArrayList<>(oldBatchParams.size());
            for (Pair<Integer, List<Object>> rowInfo : duplicateRows) {
                int rowIndex = rowInfo.getKey();
                List<Object> duplicateValues = rowInfo.getValue();
                Map<Integer, ParameterContext> params = new HashMap<>(oldBatchParams.get(rowIndex));
                nextIndex = maxIndex + 1;
                for (int i = 0; i < oldRowExpressions.size(); i++) {
                    int columnIndex = columnIndexes.indexOf(i);
                    if (columnIndex >= 0) {
                        params.put(nextIndex, new ParameterContext(ParameterMethod.setObject1, new Object[] {
                            nextIndex, duplicateValues.get(columnIndex)}));
                        nextIndex++;
                    }
                }
                newBatchParams.add(params);
            }

            Parameters newParameters = new Parameters(oldParameters.getCurrentParameter(), true);
            newParameters.setBatchParams(newBatchParams);
            executionContext.setParams(newParameters);

        } else {
            Map<Integer, ParameterContext> oldParams = oldParameters.getCurrentParameter();
            Map<Integer, ParameterContext> newParams = new HashMap<>(oldParams);
            int nextIndex = Collections.max(oldParams.keySet());
            List<SqlNode> newRows = new ArrayList<>(duplicateRows.size());

            for (Pair<Integer, List<Object>> rowInfo : duplicateRows) {
                int rowIndex = rowInfo.getKey();
                List<Object> duplicateValues = rowInfo.getValue();
                SqlCall oldRow = (SqlCall) rows.get(rowIndex);
                List<SqlNode> oldRowExpressions = oldRow.getOperandList();
                SqlNode[] newRowExpressions = new SqlNode[oldRowExpressions.size()];

                for (int i = 0; i < oldRowExpressions.size(); i++) {
                    int columnIndex = columnIndexes.indexOf(i);
                    if (columnIndex >= 0) {
                        // use new DynamicParam
                        newRowExpressions[i] = new SqlDynamicParam(nextIndex, SqlParserPos.ZERO);
                        // add new ParameterContext
                        newParams.put(nextIndex + 1, new ParameterContext(ParameterMethod.setObject1, new Object[] {
                            nextIndex + 1, duplicateValues.get(columnIndex)}));
                        nextIndex++;
                    } else {
                        newRowExpressions[i] = oldRowExpressions.get(i);
                    }
                }
                newRows.add(new SqlBasicCall(oldRow.getOperator(), newRowExpressions, SqlParserPos.ZERO));
            }

            selectedRows = newRows.toArray(new SqlNode[newRows.size()]);

            Parameters newParameters = new Parameters(newParams, false);
            executionContext.setParams(newParameters);
        }

        SqlNode newValues = new SqlBasicCall(values.getOperator(), selectedRows, values.getParserPosition());

        return new SqlInsert(oldSqlInsert.getParserPosition(),
            SqlNodeList.EMPTY,
            oldSqlInsert.getTargetTable(),
            newValues,
            oldSqlInsert.getTargetColumnList(),
            oldSqlInsert.getUpdateList(), // push down ON DUPLICATE KEY UPDATE
            // clause
            oldSqlInsert.getBatchSize(),
            oldSqlInsert.getHints());
    }

    /**
     * Pick some columns from an existing insert (insert on the base table),
     * build target column list for a new insert (insert on the index table).
     *
     * @param oldSqlInsert insert on the base table
     * @param indexMeta the index
     * @param chosenColumnIndexes output parameter, the chosen columns
     * @return target column list for the new insert
     */
    protected static SqlNodeList buildTargetColumnList(SqlInsert oldSqlInsert, TableMeta indexMeta,
                                                       List<Integer> chosenColumnIndexes) {
        SqlNodeList oldColumnList = oldSqlInsert.getTargetColumnList();
        SqlNodeList newColumnList = new SqlNodeList(SqlParserPos.ZERO);
        for (int i = 0; i < oldColumnList.size(); i++) {
            SqlIdentifier targetColumn = (SqlIdentifier) oldColumnList.get(i);
            String colName = targetColumn.getLastName();
            if (indexMeta.getColumnIgnoreCase(colName) != null) {
                chosenColumnIndexes.add(i);
                newColumnList.add(targetColumn);
            }
        }

        return newColumnList;
    }

    /**
     * INSERT ON DUPLICATE KEY UPDATE = INSERT IGNORE with unique data + INSERT
     * existing data ON DUPLICATE KEY UPDATE. Why not use UPDATE: every row need
     * a UPDATE statement, but PhyTableOperation doesn't support multiple
     * statements. But we must guarantee that the second INSERT only updates
     * data, no new data will be inserted. And the approach to guarantee this is
     * to use existing data (including sharding keys and unique keys) in the
     * VALUES list.
     *
     * @param sqlInsert original logical INSERT
     * @param physicalPlans physical INSERT on the base table
     * @param shardResults corresponding sharding results on the base table
     * @return affected rows on the base table
     */
    private int executeInsertDuplicate(SqlInsert sqlInsert, RelNode logicalPlan, List<RelNode> physicalPlans,
                                       List<PhyTableInsertSharder.PhyTableShardResult> shardResults,
                                       List<TableMeta> indexMetas, ExecutionContext executionContext,
                                       String baseTableName) {
        TableMeta baseTableMeta = executionContext.getSchemaManager(schemaName).getTable(baseTableName);
        checkUpdatingShardingKey(sqlInsert, baseTableMeta, indexMetas, executionContext);

        List<String> selectKeyNames = GlobalIndexMeta.getUniqueAndShardingKeys(baseTableMeta, indexMetas, schemaName);
        List<List<String>> conditionKeys =
            GlobalIndexMeta.getUniqueKeys(baseTableMeta, true, tm -> true, executionContext);

        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
        List<RelNode> selects = builder.buildSelect(baseTableMeta, physicalPlans, selectKeyNames, conditionKeys);

        ExecutionContext selectEC = executionContext.copy();
        // just remove the batch flag
        selectEC.setParams(new Parameters(executionContext.getParams().getCurrentParameter(), false));
        List<Cursor> cursors = executeFunc.apply(selects, selectEC);

        DuplicateValueFinder finder = new DuplicateValueFinder(baseTableMeta,
            sqlInsert.getTargetColumnList(),
            selectKeyNames, executionContext);
        List<Integer> distinctRowIndexes = new ArrayList<>();
        // [(row index in logical insert, conflicting values in database)]
        List<org.apache.calcite.util.Pair<Integer, List<Object>>> duplicateRows = new ArrayList<>();

        try {
            for (int i = 0; i < physicalPlans.size(); i++) {
                Cursor cursor = cursors.get(i);
                List<List<Object>> duplicateValues = new ArrayList<>();

                Row row;
                while ((row = cursor.next()) != null) {
                    duplicateValues.add(row.getValues());
                }

                cursor.close(new ArrayList<>());
                cursors.set(i, null);

                finder.setPhysicalPlan((PhyTableOperation) physicalPlans.get(i), duplicateValues, shardResults.get(i)
                    .getValueIndices());

                distinctRowIndexes.addAll(finder.getDistinctValueIndices());
                duplicateRows.addAll(finder.getDuplicateRows());

                finder.clearPhysicalPlan();
            }
        } finally {
            for (Cursor cursor : cursors) {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }
        }

        int affectRowsInBase = 0;

        // for distinct rows, just insert
        if (!distinctRowIndexes.isEmpty()) {
            Collections.sort(distinctRowIndexes);
            ExecutionContext insertEC = executionContext.copy();
            // just like in INSERT IGNORE
            SqlInsert newLogicalInsert = buildLogicalPlanWithDistinctData(sqlInsert, distinctRowIndexes, insertEC);

            affectRowsInBase += insertIntoTable(logicalPlan, newLogicalInsert, baseTableMeta, insertEC, true);

            for (TableMeta indexMeta : indexMetas) {
                if (GlobalIndexMeta.canWrite(executionContext, indexMeta)) {
                    // build new logical insert, with only columns in index
                    SqlInsert planOnIndex = buildIndexLogicalPlan(newLogicalInsert, indexMeta);

                    int affectRowsInIndex = insertIntoTable(logicalPlan, planOnIndex, indexMeta, insertEC, true);
                    // check the affected rows
                    if (affectRowsInIndex != affectRowsInBase) {
                        // because ON DUPLICATE KEY UPDATE is removed, it's
                        // impossible here
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_AFFECT_ROWS_DIFFERENT,
                            baseTableName,
                            String.valueOf(affectRowsInBase),
                            indexMeta.getTableName(),
                            String.valueOf(affectRowsInIndex));
                    }
                }
            }
        }

        // Change to use UPDATE, because in backfill phase, data in GSI is less
        // than that in the base table.
        if (!duplicateRows.isEmpty()) {
            // sorting is necessary, because following update must occur after
            // preceding update
            duplicateRows.sort(Comparator.comparing(Pair<Integer, List<Object>>::getKey));
            ExecutionContext updateEC = executionContext.copy();
            BatchUpdateBuilder updateBuilder = new BatchUpdateBuilder(schemaName, updateEC);
            List<RelNode> sqlUpdates = updateBuilder.buildUpdate(sqlInsert,
                baseTableMeta,
                duplicateRows,
                selectKeyNames,
                physicalPlans.get(0));

            cursors = executeFunc.apply(sqlUpdates, executionContext);
            // For update, if modified, affectedRows=1, if not, affectedRows=0;
            // For upsert, it's 2 and 0, respectively.
            affectRowsInBase += ExecUtils.getAffectRowsByCursors(cursors, false) * 2;

            for (TableMeta indexMeta : indexMetas) {
                if (GlobalIndexMeta.canWrite(executionContext, indexMeta)) {
                    updateBuilder = new BatchUpdateBuilder(schemaName, updateEC);
                    sqlUpdates = updateBuilder.buildUpdate(sqlInsert,
                        indexMeta,
                        duplicateRows,
                        selectKeyNames,
                        physicalPlans.get(0));

                    if (!sqlUpdates.isEmpty()) {
                        cursors = executeFunc.apply(sqlUpdates, executionContext);
                        ExecUtils.getAffectRowsByCursors(cursors, false);
                    }
                }
            }
        }

        // for rows that duplicate, use insert on duplicate key update
        if (false) {
            // sorting is necessary, because following update must occur after
            // preceding update
            duplicateRows.sort(Comparator.comparing(Pair<Integer, List<Object>>::getKey));
            ExecutionContext insertEC = executionContext.copy();
            // build a INSERT ON DUPLICATE KEY UPDATE statement, whose VALUES
            // list contains existing data, to ensure that all rows conflict and
            // trigger the UPDATE clause
            SqlInsert newLogicalInsert = buildLogicalPlanWithDuplicateData(sqlInsert,
                selectKeyNames,
                duplicateRows,
                insertEC);

            affectRowsInBase += insertIntoTable(logicalPlan, newLogicalInsert, baseTableMeta, insertEC, false);

            for (TableMeta indexMeta : indexMetas) {
                if (GlobalIndexMeta.canWrite(executionContext, indexMeta)) {
                    // build new logical insert, with only columns in index
                    SqlInsert planOnIndex = buildIndexLogicalPlan(newLogicalInsert, indexMeta);
                    if (planOnIndex.getUpdateList().size() == 0) {
                        continue;
                    }

                    insertIntoTable(logicalPlan, planOnIndex, indexMeta, insertEC, false);

                    // Why not check affected rows: affected rows differ from
                    // index
                    // to index, because if updated value doesn't change,
                    // affected
                    // rows is 1, otherwise it's 2. But update list in index is
                    // pruned, so it's 2 on the base table but may be 1 on the
                    // index.
                }
            }
        }

        return affectRowsInBase;
    }

    /**
     * Check if sharding keys or unique keys are to be updated.
     */
    private void checkUpdatingShardingKey(SqlInsert sqlInsert, TableMeta baseTableMeta, List<TableMeta> indexMetas,
                                          ExecutionContext ec) {
        List<TableMeta> allTables = new ArrayList<>(indexMetas.size() + 1);
        allTables.add(baseTableMeta);
        allTables.addAll(indexMetas);

        // column names are already in upper case
        Map<String, String> keyToTable = new HashMap<>();
        for (TableMeta tableMeta : allTables) {
            List<String> shardingKeys = GlobalIndexMeta.getShardingKeys(tableMeta, schemaName);
            for (String key : shardingKeys) {
                keyToTable.putIfAbsent(key, tableMeta.getTableName());
            }
        }

        // check sharding key
        SqlNodeList columnList = sqlInsert.getUpdateColumnList();
        for (SqlNode column : columnList) {
            String columnName = ((SqlIdentifier) column).getLastName().toUpperCase();
            String tableName = keyToTable.get(columnName);
            if (tableName != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_MODIFY_SHARD_COLUMN, columnName, tableName);
            }
        }

        // check unique key
        List<List<String>> uniqueKeys = GlobalIndexMeta.getUniqueKeys(baseTableMeta, true, tm -> true, ec);
        Set<String> uniqueKeySet = new HashSet<>();
        uniqueKeys.forEach(uniqueKeySet::addAll);
        for (SqlNode column : columnList) {
            String columnName = ((SqlIdentifier) column).getLastName().toUpperCase();
            if (uniqueKeySet.contains(columnName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_MODIFY_UNIQUE_KEY, columnName);
            }
        }
    }

    /**
     * Replace statement = delete + insert, rather than replacing some columns
     * in the old row. So find the conflicting data in the database and delete
     * them, then insert the whole statement just like it in the INSERT
     * statement.
     *
     * @param sqlInsert the logical REPLACE
     * @param physicalPlans the physical replace plans
     * @return affected rows
     */
    private int executeReplace(SqlInsert sqlInsert, RelNode logicalPlan, List<RelNode> physicalPlans,
                               List<TableMeta> indexMetas,
                               ExecutionContext executionContext, String baseTableName) {

        TableMeta baseTableMeta = executionContext.getSchemaManager(schemaName).getTable(baseTableName);
        List<String> selectKeyNames = GlobalIndexMeta.getPrimaryAndShardingKeys(baseTableMeta, indexMetas, schemaName);
        List<List<String>> conditionKeys =
            GlobalIndexMeta.getUniqueKeys(baseTableMeta, true, tm -> true, executionContext);

        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);
        List<RelNode> selects = builder.buildSelect(baseTableMeta, physicalPlans, selectKeyNames, conditionKeys);

        ExecutionContext selectEC = executionContext.copy();
        // just remove the batch flag
        selectEC.setParams(new Parameters(executionContext.getParams().getCurrentParameter(), false));
        List<Cursor> cursors = executeFunc.apply(selects, selectEC);

        List<List<Object>> duplicateValues = new ArrayList<>();

        try {
            for (int i = 0; i < physicalPlans.size(); i++) {
                Cursor cursor = cursors.get(i);

                Row row;
                while ((row = cursor.next()) != null) {
                    duplicateValues.add(row.getValues());
                }

                cursor.close(new ArrayList<>());
                cursors.set(i, null);
            }
        } finally {
            for (Cursor cursor : cursors) {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }
        }

        int deleteAffectRows = 0;
        if (!duplicateValues.isEmpty()) {
            PhyTableOperation oneOldPhysicalPlan = (PhyTableOperation) physicalPlans.get(0);

            // update the base table
            deleteAffectRows = deleteDuplicateOnTable(baseTableMeta,
                executionContext,
                duplicateValues,
                selectKeyNames,
                oneOldPhysicalPlan);

            // update index tables
            for (TableMeta indexMeta : indexMetas) {
                if (GlobalIndexMeta.canDelete(executionContext, indexMeta)) {
                    int indexAffectRows = deleteDuplicateOnTable(indexMeta,
                        executionContext,
                        duplicateValues,
                        selectKeyNames,
                        oneOldPhysicalPlan);

                    if (indexAffectRows != deleteAffectRows) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_AFFECT_ROWS_DIFFERENT,
                            baseTableName,
                            String.valueOf(deleteAffectRows),
                            indexMeta.getTableName(),
                            String.valueOf(indexAffectRows));
                    }
                }
            }
        }

        // Just change 'REPLACE' to 'INSERT'
        SqlInsert newLogicalInsert = new SqlInsert(sqlInsert.getParserPosition(),
            SqlNodeList.EMPTY,
            sqlInsert.getTargetTable(),
            sqlInsert.getSource(),
            sqlInsert.getTargetColumnList(),
            SqlNodeList.EMPTY,
            sqlInsert.getBatchSize(),
            sqlInsert.getHints());

        int insertAffectRows = insertIntoTable(logicalPlan, newLogicalInsert, baseTableMeta, executionContext, true);

        for (TableMeta indexMeta : indexMetas) {
            if (GlobalIndexMeta.canWrite(executionContext, indexMeta)) {
                // build new logical insert, with only columns in index
                SqlInsert planOnIndex = buildIndexLogicalPlan(newLogicalInsert, indexMeta);

                int indexAffectRows = insertIntoTable(logicalPlan, planOnIndex, indexMeta, executionContext, true);
                // check the affected rows
                if (indexAffectRows != insertAffectRows) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_AFFECT_ROWS_DIFFERENT,
                        baseTableName,
                        String.valueOf(insertAffectRows),
                        indexMeta.getTableName(),
                        String.valueOf(indexAffectRows));
                }
            }
        }

        // In MySQL document, affected rows should be deleteAffectRows +
        // insertAffectRows. But in MySQL, if the table has no unique keys,
        // actual affected rows is insertAffectRows. For now, we just conform to
        // the document.
        return deleteAffectRows + insertAffectRows;
    }

    /**
     * Build a DELETE plan and execute it. This function is used in executing
     * REPLACE.
     *
     * @param tableMeta may be primary table or index table
     * @param duplicateValues found conflicting data in the database
     * @param selectKeyNames column names corresponding to duplicateValues
     * @param oneOldPhysicalPlan any physical plan, used to create new ones
     * @return affected rows
     */
    private int deleteDuplicateOnTable(TableMeta tableMeta, ExecutionContext executionContext,
                                       List<List<Object>> duplicateValues, List<String> selectKeyNames,
                                       PhyTableOperation oneOldPhysicalPlan) {
        PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, executionContext);

        List<RelNode> phyTableOperations = builder.buildDelete(tableMeta,
            duplicateValues,
            selectKeyNames,
            oneOldPhysicalPlan);

        List<Cursor> cursors = executeFunc.apply(phyTableOperations, executionContext);
        return ExecUtils.getAffectRowsByCursors(cursors, false);
    }

    /**
     * For now, unique keys cannot be NULL. Because two unique keys are both
     * NULL, it's not considered conflicting.
     *
     * @param sqlInsert logical insert
     */
    private void checkUniqueKeyNotNull(SqlInsert sqlInsert, String tableName, ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        List<TableMeta> indexTableMetas = GlobalIndexMeta.getIndex(tableName, schemaName, executionContext);
        List<String> uniqueKeys = GlobalIndexMeta.getUniqueKeyColumnList(tableMeta, indexTableMetas, false);
        if (uniqueKeys.isEmpty()) {
            return;
        }

        List<List<Object>> keyValues = BuildPlanUtils.pickValuesFromInsert(sqlInsert,
            uniqueKeys,
            executionContext.getParams(),
            true, new ArrayList<>());

        for (List<Object> rowValues : keyValues) {
            for (int i = 0; i < rowValues.size(); i++) {
                if (rowValues.get(i) == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_INSERT_UNIQUE_KEY_NULL,
                        uniqueKeys.get(i));
                }
            }
        }
    }
}
