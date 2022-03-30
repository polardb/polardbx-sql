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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyOperationBuilderCommon;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlHashCheckAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PhysicalPlanBuilder extends PhyOperationBuilderCommon {

    protected final ExecutionContext ec;
    protected RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    protected SqlNode targetTableNode;
    protected SqlNode keyNameNodeForInClause;
    protected SqlNodeList columnList;
    protected SqlNodeList expressionList;
    // next available index for DynamicParam
    protected int nextDynamicIndex;
    protected Map<Integer, ParameterContext> currentParams;
    protected String schemaName;

    public PhysicalPlanBuilder(String schemaName, ExecutionContext ec) {
        this.schemaName = schemaName;
        this.ec = ec;
    }

    protected void initParams(int newIndex) {
        // don't use clear()
        currentParams = new HashMap<>();
        nextDynamicIndex = newIndex;
    }

    /**
     * If oldParams comes from physical plan, DynamicParam.index + 2 =
     * ParameterContext.index, otherwise DynamicParam.index + 1 =
     * ParameterContext.index
     */
    protected void buildParams(Map<Integer, ParameterContext> oldParams, SqlNode sqlNode, boolean isOldParamsLogical) {
        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlNode);

        buildParams(oldParams, paramIndex, isOldParamsLogical);
    }

    protected void buildParams(Map<Integer, ParameterContext> oldParams, List<Integer> paramIndex,
                               boolean isOldParamsLogical) {
        int indexDiff = isOldParamsLogical ? 1 : 2;

        for (int i : paramIndex) {
            // including target table
            currentParams.put(nextDynamicIndex + 2,
                PlannerUtils.changeParameterContextIndex(oldParams.get(i + indexDiff), nextDynamicIndex + 2));
            nextDynamicIndex++;
        }
    }

    /**
     * build PhyTableOperation according to SqlNode
     *
     * @param relNode old PhyTableOperation or LogicalView or whatever, it
     * doesn't matter
     * @param rowType returned data types in one row
     */
    protected static PhyTableOperation buildPhyTableOperation(SqlNode sqlNode, RelNode relNode, String dbIndex,
                                                              List<List<String>> tableNames, RelDataType rowType,
                                                              Map<Integer, ParameterContext> params,
                                                              LockMode lockMode) {
        String sql = RelUtils.toNativeSql(sqlNode);
        PhyTableOperation operation =
            new PhyTableOperation(relNode.getCluster(), relNode.getTraitSet(), rowType, null, relNode);
        operation.setDbIndex(dbIndex);
        operation.setTableNames(tableNames);
        operation.setSqlTemplate(sql);
        operation.setKind(sqlNode.getKind());
        operation.setDbType(DbType.MYSQL);
        operation.setParam(params);
        operation.setLockMode(lockMode);
        return operation;
    }

    protected void buildTargetTable() {
        targetTableNode = BuildPlanUtils.buildTargetTable();
    }

    protected void buildTargetTableParams(String tableName) {
        int tableIndex = PlannerUtils.TABLE_NAME_PARAM_INDEX + 2;
        currentParams.put(tableIndex, PlannerUtils.buildParameterContextForTableName(tableName, tableIndex));
    }

    protected RelDataType buildRowTypeForSelect(List<String> selectKeys, TableMeta tableMeta, SqlNodeList selectList) {
        return buildRowTypeForSelect(selectKeys, null, tableMeta, selectList);
    }

    /**
     * Build RelDataType for row type and SqlNodeList for select list
     *
     * @param selectKeys column names to be selected
     * @param selectList output params
     * @return row type
     */
    protected RelDataType buildRowTypeForSelect(List<String> selectKeys, ColumnMeta rowIndex, TableMeta tableMeta,
                                                SqlNodeList selectList) {
        final List<ColumnMeta> selectColumns = new ArrayList<>();
        for (String keyName : selectKeys) {
            selectList.add(new SqlIdentifier(keyName, SqlParserPos.ZERO));
            selectColumns.add(tableMeta.getColumn(keyName));
        }

        if (null != rowIndex) {
            selectColumns.add(rowIndex);
        }

        return CalciteUtils.switchRowType(selectColumns, typeFactory);
    }

    protected static SqlNode buildKeyNameNodeForInClause(List<String> keyNames) {
        // keyNameNode
        if (keyNames.size() > 1) {
            SqlNode[] keyNameNodes = keyNames.stream()
                .map(keyName -> new SqlIdentifier(keyName, SqlParserPos.ZERO))
                .toArray(SqlNode[]::new);
            return new SqlBasicCall(SqlStdOperatorTable.ROW, keyNameNodes, SqlParserPos.ZERO);
        } else {
            return new SqlIdentifier(keyNames.get(0), SqlParserPos.ZERO);
        }
    }

    /**
     * Build the condition for SqlUpdate or SqlDelete or SqlSelect. Format: IN(
     * ROW( SqlIdentifier, SqlIdentifier ), ROW( ROW( SqlDynamicParam,
     * SqlDynamicParam), ROW( SqlDynamicParam, SqlDynamicParam ) )
     *
     * @param pkValues each row stands for a composed primary key value
     * @param keyNameNode (pk1, pk2)
     * @return the condition node
     */
    protected SqlNode buildInCondition(List<List<Object>> pkValues, SqlNode keyNameNode) {
        SqlNode[] allValues = new SqlNode[pkValues.size()];

        for (int i = 0; i < pkValues.size(); i++) {
            List<Object> pkRow = pkValues.get(i);

            if (pkRow.size() > 1) {
                SqlNode[] rowValues = new SqlNode[pkRow.size()];

                for (int j = 0; j < pkRow.size(); j++) {
                    // Why not use SqlLiteral: constructor of SqlLiteral is
                    // protected
                    rowValues[j] = buildParam(ParameterMethod.setObject1, pkRow.get(j));
                }

                allValues[i] = new SqlBasicCall(SqlStdOperatorTable.ROW, rowValues, SqlParserPos.ZERO);
            } else {
                allValues[i] = buildParam(ParameterMethod.setObject1, pkRow.get(0));
            }
        }

        SqlNode outerRow = new SqlBasicCall(SqlStdOperatorTable.ROW, allValues, SqlParserPos.ZERO);

        return new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[] {keyNameNode, outerRow}, SqlParserPos.ZERO);
    }

    protected SqlNode buildConstant(Object value, String name) {
        SqlIdentifier sqlIdentifier = new SqlIdentifier(name, SqlParserPos.ZERO);
        SqlNode param = buildParam(ParameterMethod.setObject1, value);
        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, param, sqlIdentifier);
    }

    private SqlDynamicParam buildParam(ParameterMethod method, Object value) {
        final SqlDynamicParam result = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);

        final int paramIndex = nextDynamicIndex + 2;
        currentParams.put(paramIndex, new ParameterContext(method, new Object[] {paramIndex, value}));
        nextDynamicIndex++;

        return result;
    }

    /**
     * Build a new TargetColumnList and ExpressionList. That is, rephrase the
     * SET sub-statement in the UPDATE statement. If the field to be modified is
     * not in the index table, remove that field. TODO: check if the expression
     * contains reference to the fields not in the index table; check if it's
     * modifying sharding keys of index.
     *
     * @param oldColumnList identifier list, containing the fields to be
     * modified
     * @param oldExpressionList expression list, containing the expressions to
     * be set to the fields
     * @param tableMeta the base table or the index table
     * @return [newColumnList, newExpressionList]
     */
    private void buildTargetColumnList(SqlNodeList oldColumnList, SqlNodeList oldExpressionList, TableMeta tableMeta) {
        columnList = new SqlNodeList(SqlParserPos.ZERO);
        expressionList = new SqlNodeList(SqlParserPos.ZERO);

        for (int i = 0; i < oldColumnList.size(); i++) {
            SqlIdentifier column = (SqlIdentifier) oldColumnList.get(i);
            if (tableMeta.getColumnIgnoreCase(column.getLastName()) != null) {
                columnList.add(column);
                expressionList.add(oldExpressionList.get(i));
            }
        }
    }

    /**
     * @return {targetDb: {targetTb: [[index, pk]]}}
     */
    protected Map<String, Map<String, List<Pair<Integer, List<Object>>>>> buildCommonInUpdateAndDelete(
        TableMeta tableMeta,
        List<List<Object>> selectedResults,
        List<String> selectKeys) {
        buildTargetTable();

        // primary key indexes and ColumnMetas
        List<String> primaryKeyNames = GlobalIndexMeta.getPrimaryKeys(tableMeta);
        List<Integer> primaryKeyIndexes = primaryKeyNames.stream()
            .map(selectKeys::indexOf)
            .collect(Collectors.toList());

        // sharding key indexes and ColumnMetas
        List<String> shardingKeyNames = GlobalIndexMeta.getShardingKeys(tableMeta, schemaName);
        List<ColumnMeta> shardingKeyMetas = new ArrayList<>(shardingKeyNames.size());
        List<Integer> shardingKeyIndexes = new ArrayList<>(shardingKeyNames.size());
        for (String shardingKey : shardingKeyNames) {
            shardingKeyIndexes.add(selectKeys.indexOf(shardingKey));
            shardingKeyMetas.add(tableMeta.getColumnIgnoreCase(shardingKey));
        }

        // keyNameNode
        keyNameNodeForInClause = buildKeyNameNodeForInClause(primaryKeyNames);

        List<String> relShardingKeyNames = BuildPlanUtils.getRelColumnNames(tableMeta, shardingKeyMetas);
        // targetDb: { targetTb: [[pk1], [pk2]] }
        return BuildPlanUtils.buildResultForShardingTable(schemaName,
            tableMeta.getTableName(),
            selectedResults,
            relShardingKeyNames,
            shardingKeyIndexes,
            shardingKeyMetas,
            primaryKeyIndexes,
            ec);
    }

    /**
     * build new physical UPDATE / DELETE plans for base table and index tables
     *
     * @param oldSqlUpdate old logical UPDATE / DELETE plan on the base table
     * @param tableMeta base table or index table that to be updated / deleted
     * @param selectedResults primary keys / sharding keys that need to be
     * updated / deleted
     * @param oldPhysicalPlan old UPDATE / DELETE physical plan, whose some
     * fields are used to build new plans
     * @return new physical plans
     */
    public List<RelNode> buildUpdate(SqlUpdate oldSqlUpdate, TableMeta tableMeta,
                                     List<List<Object>> selectedResults, List<String> selectKeys,
                                     PhyTableOperation oldPhysicalPlan) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
            buildCommonInUpdateAndDelete(tableMeta,
                selectedResults,
                selectKeys);

        // new plans to be returned
        List<RelNode> newPhysicalPlans = new ArrayList<>();

        // columnList / expressionList
        buildTargetColumnList(oldSqlUpdate.getTargetColumnList(), oldSqlUpdate.getSourceExpressionList(), tableMeta);

        // if all updated columns are not in the index table, do nothing.
        if (columnList.size() == 0) {
            return newPhysicalPlans;
        }

        List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(expressionList);

        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {
                String targetTb = tbEntry.getKey();
                List<List<Object>> pkValues = tbEntry.getValue()
                    .stream()
                    .map(Pair::getValue)
                    .collect(Collectors.toList());

                initParams(0);
                buildTargetTableParams(targetTb);

                buildParams(ec.getParams().getCurrentParameter(), paramIndex, true);

                SqlNode inCondition = buildInCondition(pkValues, keyNameNodeForInClause);

                SqlNode newNode = new SqlUpdate(SqlParserPos.ZERO,
                    targetTableNode,
                    columnList,
                    expressionList,
                    inCondition,
                    null,
                    null);

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                PhyTableOperation operation = buildPhyTableOperation(newNode,
                    oldPhysicalPlan,
                    targetDb,
                    tableNames,
                    oldPhysicalPlan.getRowType(),
                    currentParams,
                    LockMode.UNDEF);

                newPhysicalPlans.add(operation);
            }
        }

        return newPhysicalPlans;
    }

    public List<RelNode> buildDelete(TableMeta tableMeta,
                                     List<List<Object>> selectedResults, List<String> selectKeys,
                                     PhyTableOperation oldPhysicalPlan) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
            buildCommonInUpdateAndDelete(tableMeta,
                selectedResults,
                selectKeys);

        // new plans to be returned
        List<RelNode> newPhysicalPlans = new ArrayList<>();

        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {
                String targetTb = tbEntry.getKey();
                List<List<Object>> pkValues = tbEntry.getValue()
                    .stream()
                    .map(Pair::getValue)
                    .collect(Collectors.toList());

                initParams(0);
                buildTargetTableParams(targetTb);
                SqlNode inCondition = buildInCondition(pkValues, keyNameNodeForInClause);
                SqlNode newNode = new SqlDelete(SqlParserPos.ZERO, targetTableNode, inCondition, null, null);

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                PhyTableOperation operation = buildPhyTableOperation(newNode,
                    oldPhysicalPlan,
                    targetDb,
                    tableNames,
                    oldPhysicalPlan.getRowType(),
                    currentParams,
                    LockMode.UNDEF);

                newPhysicalPlans.add(operation);
            }
        }

        return newPhysicalPlans;
    }

    /**
     * Two choices: buildSelectIn or buildSelectUnion.
     */
    public List<RelNode> buildSelect(TableMeta baseTableMeta, List<RelNode> oldPhysicalPlans, List<String> selectKeys,
                                     List<List<String>> conditionKeys) {
        return buildSelectUnion(baseTableMeta, oldPhysicalPlans, selectKeys, conditionKeys);
    }

    /**
     * Create a physical SELECT that corresponds to the physical INSERT, to find
     * conflicting data to the INSERT. Format: SELECT PRIMARY_KEY FROM
     * PHYSICAL_TABLE WHERE (UNIQUE_KEY1 IN VALUE_LIST) OR (UNIQUE_KEY2 IN
     * VALUE_LIST). Why not create a logical SELECT: no need to shard again. Why
     * not use (SELECT 1, PK FROM TB WHERE UK1 = X1 OR UK2 = X2) UNION (SELECT
     * 2, PK FROM TB WHERE UK1 = X3 OR UK2 = X4) to find the duplicate value
     * indexes: the SQL may exceed the MAX_PACKET_SIZE, and then it should be
     * split into several SQLs, which increases RTs.
     *
     * @param oldPhysicalPlans old physical INSERT plan on the base table
     * @param selectKeys the columns to be queried
     * @param conditionKeys ((key1, key2) IN ((xx, xx)) OR (key3, key4) IN ((xx,
     * xx)))
     * @return the physical SELECT corresponding to the physical INSERT
     */
    private List<RelNode> buildSelectIn(TableMeta baseTableMeta, List<RelNode> oldPhysicalPlans,
                                        List<String> selectKeys, List<List<String>> conditionKeys) {
        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, baseTableMeta, selectList);

        // build target table
        buildTargetTable();

        // keyNameNode, [(key1, key2), (key3, key4)]
        List<SqlNode> keyNameNodeList = conditionKeys.stream()
            .map(PhysicalPlanBuilder::buildKeyNameNodeForInClause)
            .collect(Collectors.toList());

        // to implement concurrent query, gather all selects and apply once.
        List<RelNode> selects = new ArrayList<>(oldPhysicalPlans.size());

        for (RelNode oldPhysicalPlan : oldPhysicalPlans) {
            PhyTableOperation insertPlan = (PhyTableOperation) oldPhysicalPlan;
            SqlInsert sqlInsert = (SqlInsert) insertPlan.getNativeSqlNode();

            initParams(0);
            buildTargetTableParams(insertPlan.getTableNames().get(0).get(0));

            // build condition
            List<SqlNode> inClauses = new ArrayList<>(conditionKeys.size());
            for (int i = 0; i < conditionKeys.size(); i++) {
                List<String> conditionKey = conditionKeys.get(i);
                SqlNode keyNameNode = keyNameNodeList.get(i);

                List<List<Object>> keyValues = BuildPlanUtils.pickValuesFromInsert(sqlInsert,
                    conditionKey,
                    new Parameters(insertPlan.getParam(), false),
                    false, new ArrayList<>());

                inClauses.add(buildInCondition(keyValues, keyNameNode));
            }

            // if there are more than one unique keys, construct OR
            SqlNode condition = PlannerUtils.buildOrTree(inClauses);

            SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
                null,
                selectList,
                targetTableNode,
                condition,
                null,
                null,
                null,
                null,
                null,
                null);
            sqlSelect.setLockMode(LockMode.EXCLUSIVE_LOCK);

            PhyTableOperation select = buildPhyTableOperation(sqlSelect,
                insertPlan,
                insertPlan.getDbIndex(),
                insertPlan.getTableNames(),
                rowType,
                currentParams,
                LockMode.EXCLUSIVE_LOCK);

            selects.add(select);
        }

        return selects;
    }

    /**
     * (SELECT PK FROM TB WHERE UK1 = X1) UNION (SELECT PK FROM TB WHERE UK2 =
     * X2) UNION (SELECT PK FROM TB WHERE UK1 = X3) UNION (SELECT PK FROM TB
     * WHERE UK2 = X4). Advantage: less lock retained.
     *
     * @param oldPhysicalPlans old physical INSERT plan on the base table
     * @param selectKeys the columns to be queried
     * @param conditionKeys ((key1, key2) IN ((xx, xx)) OR (key3, key4) IN ((xx,
     * xx)))
     * @return the physical SELECT corresponding to the physical INSERT
     */
    private List<RelNode> buildSelectUnion(TableMeta baseTableMeta, List<RelNode> oldPhysicalPlans,
                                           List<String> selectKeys, List<List<String>> conditionKeys) {
        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, baseTableMeta, selectList);

        // build target table
        buildTargetTable();

        // keyNameNode, [(key1, key2), (key3, key4)]
        List<SqlNode> keyNameNodeList = conditionKeys.stream()
            .map(PhysicalPlanBuilder::buildKeyNameNodeForInClause)
            .collect(Collectors.toList());

        // to implement concurrent query, gather all selects and apply once.
        List<RelNode> selects = new ArrayList<>(oldPhysicalPlans.size());

        for (RelNode oldPhysicalPlan : oldPhysicalPlans) {
            PhyTableOperation insertPlan = (PhyTableOperation) oldPhysicalPlan;
            SqlInsert sqlInsert = (SqlInsert) insertPlan.getNativeSqlNode();

            initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);
            List<Integer> tableIndex = Lists.newArrayList(PlannerUtils.TABLE_NAME_PARAM_INDEX);

            // Build sql directly, because UNION node can't be executed.
            StringBuilder unionSql = new StringBuilder();

            // for each key
            for (int i = 0; i < conditionKeys.size(); i++) {
                List<String> conditionKey = conditionKeys.get(i);

                List<List<Object>> keyValues = BuildPlanUtils.pickValuesFromInsert(sqlInsert,
                    conditionKey,
                    new Parameters(insertPlan.getParam(), false),
                    false, new ArrayList<>());

                SqlNode keyNameNode = keyNameNodeList.get(i);
                // for each row
                for (List<Object> row : keyValues) {
                    // build target table
                    buildParams(insertPlan.getParam(), tableIndex, false);

                    // build condition
                    SqlNode condition = buildInCondition(ImmutableList.of(row), keyNameNode);

                    SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
                        null,
                        selectList,
                        targetTableNode,
                        condition,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
                    sqlSelect.setLockMode(LockMode.EXCLUSIVE_LOCK);

                    if (unionSql.length() > 0) {
                        unionSql.append(" UNION ");
                    }
                    unionSql.append("(").append(RelUtils.toNativeSql(sqlSelect, DbType.MYSQL)).append(")");
                }
            }

            PhyTableOperation select =
                new PhyTableOperation(insertPlan.getCluster(), insertPlan.getTraitSet(), rowType, null, insertPlan);
            select.setDbIndex(insertPlan.getDbIndex());
            select.setTableNames(insertPlan.getTableNames());
            select.setSqlTemplate(unionSql.toString());
            select.setKind(SqlKind.SELECT);
            select.setDbType(DbType.MYSQL);
            select.setParam(currentParams);
            select.setLockMode(LockMode.EXCLUSIVE_LOCK);

            selects.add(select);
        }

        return selects;
    }

    /**
     * Build a physical plan for selecting the data to be updated or deleted.
     * Where clause in the SELECT is exactly the same with that in the UPDATE /
     * DELETE statement.
     *
     * @param oldPhysicalPlans old physical UPDATE / DELETE plan on the base
     * table
     * @param selectKeys the columns to be queried
     */
    public List<RelNode> buildSelect(TableMeta baseTableMeta, List<RelNode> oldPhysicalPlans, List<String> selectKeys) {
        // construct row type for selecting
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, baseTableMeta, selectList);

        List<RelNode> selects = new ArrayList<>(oldPhysicalPlans.size());

        for (RelNode oldPhysicalPlan : oldPhysicalPlans) {
            PhyTableOperation plan = (PhyTableOperation) oldPhysicalPlan;

            // get SqlSelect below SqlUpdate/SqlDelete
            SqlNode sqlNode = plan.getNativeSqlNode();
            SqlSelect sqlSelect;
            if (sqlNode.getKind() == SqlKind.UPDATE) {
                SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
                sqlSelect = new SqlSelect(SqlParserPos.ZERO,
                    null,
                    selectList,
                    sqlUpdate.getTargetTable(),
                    sqlUpdate.getCondition(),
                    null,
                    null,
                    null,
                    sqlUpdate.getOrderList(),
                    null,
                    sqlUpdate.getFetch());
            } else {
                SqlDelete sqlDelete = (SqlDelete) sqlNode;
                sqlSelect = new SqlSelect(SqlParserPos.ZERO,
                    null,
                    selectList,
                    sqlDelete.getTargetTable(),
                    sqlDelete.getCondition(),
                    null,
                    null,
                    null,
                    sqlDelete.getOrderList(),
                    null,
                    sqlDelete.getFetch());
            }

            initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);

            buildParams(plan.getParam(), sqlSelect, false);
            sqlSelect.setLockMode(LockMode.EXCLUSIVE_LOCK);

            PhyTableOperation select = buildPhyTableOperation(sqlSelect,
                plan,
                plan.getDbIndex(),
                plan.getTableNames(),
                rowType,
                currentParams,
                LockMode.EXCLUSIVE_LOCK);

            selects.add(select);
        }

        return selects;
    }

    /**
     * <pre>
     *  SELECT pk0, ... , pkn
     *  FROM ?
     *  ORDER BY pk0 DESC, ... , pkn DESC LIMIT 1
     * </pre>
     *
     * @param tableMeta Table meta
     * @param primaryKeys Primary keys
     * @return Query plan
     */
    public PhyTableOperation buildSelectMaxPkForBackfill(TableMeta tableMeta, List<String> primaryKeys) {
        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(primaryKeys, tableMeta, selectList);

        // build target table
        buildTargetTable();

        // order by primary keys desc
        SqlNodeList orderBy = new SqlNodeList(
            primaryKeys.stream()
                .map(key -> new SqlBasicCall(SqlStdOperatorTable.DESC,
                    new SqlNode[] {new SqlIdentifier(key, SqlParserPos.ZERO)},
                    SqlParserPos.ZERO))
                .collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // limit 1
        SqlNode fetch = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

        // inner select
        SqlSelect select = new SqlSelect(SqlParserPos.ZERO,
            null,
            selectList,
            targetTableNode,
            null,
            null,
            null,
            null,
            orderBy,
            null,
            fetch);

        // create PhyTableOperation
        return buildPhyTableOperation(select, rowType, LockMode.UNDEF, ec);
    }

    /**
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_primary_table}
     * WHERE (pk0, ... , pkn) > (?, ... , ?)
     *   AND (pk0, ... , pkn) <= (?, ... , ?)
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE
     * </pre>
     *
     * @param tableMeta Primary table meta
     * @param selectKeys Select list
     * @param primaryKeys Primary key list, for condition building
     * @param withLowerBound With lower bound condition
     * @param withUpperBound With upper bound condition
     * @param lockMode Lock mode
     * @return ExecutionPlan for extracting data from primary table
     */
    public PhyTableOperation buildSelectForBackfill(TableMeta tableMeta, List<String> selectKeys,
                                                    List<String> primaryKeys, boolean withLowerBound,
                                                    boolean withUpperBound, LockMode lockMode) {
        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

        // build target table
        buildTargetTable();

        // build where
        SqlNode condition = null;
        if (withLowerBound) {
            // WHERE (pk0, ... , pkn) > (?, ... , ?)
            condition = buildCondition(primaryKeys, SqlStdOperatorTable.GREATER_THAN);
        }

        if (withUpperBound) {
            // WHERE (pk0, ... , pkn) <= (?, ... , ?)
            final SqlNode upperBound = buildCondition(primaryKeys, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

            condition = null == condition ? upperBound : PlannerUtils
                .buildAndTree(ImmutableList.of(condition, upperBound));
        }

        // order by primary keys
        SqlNodeList orderBy = new SqlNodeList(primaryKeys.stream()
            .map(key -> new SqlIdentifier(key, SqlParserPos.ZERO))
            .collect(Collectors.toList()), SqlParserPos.ZERO);

        // limit ?
        SqlNode fetch = new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO);

        final SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            selectList,
            targetTableNode,
            condition,
            null,
            null,
            null,
            orderBy,
            null,
            fetch);

        // lock mode
        sqlSelect.setLockMode(lockMode);

        // create PhyTableOperation
        return buildPhyTableOperation(sqlSelect, rowType, lockMode, ec);
    }

    /**
     * Get rows within pk list.
     * <p>
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_table}
     * WHERE (pk0, ... , pkn) in ((?, ... , ?), ...) -- Dynamic generated.
     * ORDER BY pk0, ... , pkn
     * -- No shared lock on checked table
     * </pre>
     *
     * @param tableMeta Primary table meta
     * @param selectKeys Select list
     * @param primaryKeys Primary key list, for condition building
     */
    public Pair<SqlSelect, PhyTableOperation> buildSelectWithInForChecker(TableMeta tableMeta, List<String> selectKeys,
                                                                          List<String> primaryKeys,
                                                                          String forceIndex) {
        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

        // build target table
        buildTargetTable();

        // build where
        final SqlNode pks;
        if (1 == primaryKeys.size()) {
            pks = new SqlIdentifier(primaryKeys.get(0), SqlParserPos.ZERO);
        } else {
            final SqlNode[] pkNodes = primaryKeys.stream()
                .map(col -> new SqlIdentifier(col, SqlParserPos.ZERO))
                .toArray(SqlNode[]::new);
            pks = new SqlBasicCall(SqlStdOperatorTable.ROW, pkNodes, SqlParserPos.ZERO);
        }

        final SqlNode condition = new SqlBasicCall(SqlStdOperatorTable.IN,
            new SqlNode[] {pks, new SqlNodeList(SqlParserPos.ZERO)},
            SqlParserPos.ZERO);

        // order by primary keys
        SqlNodeList orderBy = new SqlNodeList(
            primaryKeys.stream().map(key -> new SqlIdentifier(key, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        /**
         * Note: Experiments show that MySQL will spend a lot of time on statistics if
         * using force index, but performance is bad for small table if not use force
         * index. So the only way to optimize it is to prune the PK before select.
         */
        // from ? as `tb` force index(PRIMARY)
        final SqlNode from;
        if (forceIndex != null) {
            final SqlIdentifier asNode = new SqlIdentifier("tb", SqlParserPos.ZERO);
            asNode.indexNode = new SqlNodeList(
                ImmutableList.of(new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO),
                    null,
                    new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString(forceIndex, SqlParserPos.ZERO)),
                        SqlParserPos.ZERO),
                    SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
            from = new SqlBasicCall(SqlStdOperatorTable.AS,
                new SqlNode[] {targetTableNode, asNode},
                SqlParserPos.ZERO);
        } else {
            from = null;
        }

        final SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            selectList,
            from != null ? from : targetTableNode,
            condition,
            null,
            null,
            null,
            orderBy,
            null,
            null);

        // create PhyTableOperation
        return new Pair<>(sqlSelect, buildPhyTableOperation(sqlSelect, rowType, LockMode.UNDEF, ec));
    }

    /**
     * <pre>
     *  SELECT HASHCKECK({all_select_keys})
     *  FROM ?
     * </pre>
     *
     * @param tableMeta Table meta
     * @param selectKeys Keys that need to be hash
     * @return Query plan
     */
    public PhyTableOperation buildSelectHashCheckForChecker(TableMeta tableMeta, List<String> selectKeys) {

        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

        // build target table
        buildTargetTable();

        final SqlNode functionCallNode =
            new SqlBasicCall(new SqlHashCheckAggFunction(), selectList.toArray(), SqlParserPos.ZERO);

        final SqlNodeList selectListWithFunctionCall = new SqlNodeList(SqlParserPos.ZERO);
        selectListWithFunctionCall.add(functionCallNode);
        final SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            selectListWithFunctionCall,
            targetTableNode,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        return buildPhyTableOperation(sqlSelect, rowType, LockMode.UNDEF, ec);
    }

    /**
     * <pre>
     *  SELECT {all_select_keys}
     *  FROM ?
     *  LIMIT 1
     * </pre>
     *
     * @param tableMeta Table meta
     * @param selectKeys Keys to be select
     * @return Query plan
     */
    public PhyTableOperation buildIdleSelectForChecker(TableMeta tableMeta, List<String> selectKeys) {
        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

        // build target table
        buildTargetTable();

        // limit 1
        final SqlNode fetch = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

        final SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            selectList,
            targetTableNode,
            null,
            null,
            null,
            null,
            null,
            null,
            fetch);

        return buildPhyTableOperation(sqlSelect, rowType, LockMode.UNDEF, ec);
    }

    public PhyTableOperation buildDeleteForCorrector(List<String> primaryKeys) {
        initParams(0);

        // build target table
        buildTargetTable();

        // build PKs equals values condition
        final SqlNode pks, values;
        if (1 == primaryKeys.size()) {
            pks = new SqlIdentifier(primaryKeys.get(0), SqlParserPos.ZERO);
            values = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);
            ++nextDynamicIndex;
        } else {
            final SqlNode[] pkNodes = primaryKeys.stream()
                .map(col -> new SqlIdentifier(col, SqlParserPos.ZERO))
                .toArray(SqlNode[]::new);
            pks = new SqlBasicCall(SqlStdOperatorTable.ROW, pkNodes, SqlParserPos.ZERO);
            final SqlNode[] valueNodes = new SqlNode[primaryKeys.size()];
            for (int i = 0; i < valueNodes.length; ++i) {
                valueNodes[i] = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);
                ++nextDynamicIndex;
            }
            values = new SqlBasicCall(SqlStdOperatorTable.ROW, valueNodes, SqlParserPos.ZERO);
        }

        final SqlNode condition = new SqlBasicCall(SqlStdOperatorTable.EQUALS,
            new SqlNode[] {pks, values},
            SqlParserPos.ZERO);

        final SqlDelete sqlDelete = new SqlDelete(SqlParserPos.ZERO,
            targetTableNode,
            condition,
            null,
            null);
        return buildPhyTableOperation(sqlDelete);
    }

    private PhyTableOperation buildPhyTableOperation(SqlNode sqlNode) {
        final RelOptCluster cluster = SqlConverter.getInstance(ec).createRelOptCluster();
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        PhyTableOperation operation = new PhyTableOperation(cluster,
            traitSet,
            CalciteUtils.switchRowType(Collections.emptyList(), typeFactory));
        String sql = RelUtils.toNativeSql(sqlNode);
        operation.setSqlTemplate(sql);
        operation.setKind(sqlNode.getKind());
        operation.setDbType(DbType.MYSQL);
        return operation;
    }

    private static PhyTableOperation buildPhyTableOperation(SqlSelect sqlSelect, RelDataType rowType,
                                                            LockMode lockMode, ExecutionContext ec) {
        final RelOptCluster cluster = SqlConverter.getInstance(ec).createRelOptCluster();
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        PhyTableOperation operation = new PhyTableOperation(cluster, traitSet, rowType);
        String sql = RelUtils.toNativeSql(sqlSelect);
        operation.setSqlTemplate(sql);
        operation.setKind(SqlKind.SELECT);
        operation.setDbType(DbType.MYSQL);
        operation.setLockMode(lockMode);
        return operation;
    }

    /**
     * Build condition with row expression
     * Caution: Row expression will lead to full table scan in MySQL, so convert to DNF.
     *
     * @param columnNames Columns in row expression
     * @param operator Operator
     * @return Condition expression
     */
    private SqlNode buildCondition(List<String> columnNames, SqlOperator operator) {
        // Infer non-final operator.
        final SqlOperator nonFinalOp;
        if (SqlStdOperatorTable.LESS_THAN == operator || SqlStdOperatorTable.LESS_THAN_OR_EQUAL == operator) {
            nonFinalOp = SqlStdOperatorTable.LESS_THAN;
        } else if (SqlStdOperatorTable.GREATER_THAN == operator
            || SqlStdOperatorTable.GREATER_THAN_OR_EQUAL == operator) {
            nonFinalOp = SqlStdOperatorTable.GREATER_THAN;
        } else {
            throw new TddlNestableRuntimeException(MessageFormat.format("buildCondition not support SqlOperator {0}",
                operator.toString()));
        }

        // Gen operator tree.
        SqlNode root = null;
        for (int orIdx = 0; orIdx < columnNames.size(); ++orIdx) {
            SqlNode item = null;
            for (int andIdx = 0; andIdx <= orIdx; ++andIdx) {
                SqlNode comp = new SqlBasicCall(andIdx < orIdx ? SqlStdOperatorTable.EQUALS :
                    (orIdx != columnNames.size() - 1 ? nonFinalOp : operator),
                    new SqlNode[] {
                        new SqlIdentifier(columnNames.get(andIdx), SqlParserPos.ZERO),
                        new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO)},
                    SqlParserPos.ZERO);
                if (null == item) {
                    item = comp;
                } else {
                    item = new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[] {item, comp}, SqlParserPos.ZERO);
                }
            }
            if (null == root) {
                root = item;
            } else {
                root = new SqlBasicCall(SqlStdOperatorTable.OR, new SqlNode[] {root, item}, SqlParserPos.ZERO);
            }
        }

        // Done.
        return root;
    }

    public static ExecutionPlan buildPlanForBackfillDuplicateCheck(String schemaName, TableMeta tableMeta,
                                                                   List<String> outputColumns,
                                                                   List<String> filterColumns,
                                                                   boolean useHint, ExecutionContext ec) {
        return new PhysicalPlanBuilder(schemaName, ec)
            .buildPlanForBackfillDuplicateCheck(tableMeta, outputColumns, filterColumns, useHint);
    }

    /**
     * Build select plan for duplication check
     *
     * @param tableMeta Index table meta
     * @param outputColumns Select list
     * @param filterColumns Where column list
     * @return Select plan
     */
    private ExecutionPlan buildPlanForBackfillDuplicateCheck(TableMeta tableMeta, List<String> outputColumns,
                                                             List<String> filterColumns,
                                                             boolean useHint) {
        initParams(0);

        // Build select list
        final SqlNodeList selectList = new SqlNodeList(
            outputColumns.stream().map(c -> new SqlIdentifier(c, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // Build where
        final List<SqlNode> compareNodes = new ArrayList<>(filterColumns.size());
        // keep primary keys in the order as defined
        for (int i = 0; i < filterColumns.size(); i++) {
            final String columnName = filterColumns.get(i);
            final SqlIdentifier left = new SqlIdentifier(columnName, SqlParserPos.ZERO);
            final SqlDynamicParam right = new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO);

            // Null-safe comparison
            final SqlNode compareNode = new SqlBasicCall(TddlOperatorTable.NULL_SAFE_EQUAL,
                new SqlNode[] {left, right},
                SqlParserPos.ZERO);

            compareNodes.add(compareNode);
        }
        final SqlNode condition = PlannerUtils.buildAndTree(compareNodes);

        SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
            null,
            selectList,
            new SqlIdentifier(tableMeta.getTableName(), SqlParserPos.ZERO),
            condition,
            null,
            null,
            null,
            null,
            null,
            null);

        PlannerContext plannerContext = new PlannerContext(ec);
        plannerContext.getExecutionContext().setUseHint(useHint);
        return Planner.getInstance().getPlan(sqlSelect, plannerContext);
    }

    public ColumnMeta buildRowIndexColumnMeta(String targetTableName) {
        final Field rowIndexField = new Field(typeFactory.createSqlType(SqlTypeName.BIGINT));
        return new ColumnMeta(targetTableName, "__row__index__", null, rowIndexField);
    }

    /**
     * @return {targetDb: {targetTb: [[index, pk]]}}
     */
    protected Map<String, Map<String, List<Pair<Integer, List<Object>>>>> getShardResults(TableMeta tableMeta,
                                                                                          List<List<Object>> values,
                                                                                          List<String> columns) {

        // primary key indexes and ColumnMetas, columns must be upper case
        List<String> primaryKeyNames = GlobalIndexMeta.getPrimaryKeys(tableMeta);
        List<Integer> primaryKeyIndexes = primaryKeyNames.stream()
            .map(columns::indexOf)
            .collect(Collectors.toList());

        // sharding key indexes and ColumnMetas
        List<String> shardingKeyNames = GlobalIndexMeta.getShardingKeys(tableMeta, schemaName);
        List<ColumnMeta> shardingKeyMetas = new ArrayList<>(shardingKeyNames.size());
        List<Integer> shardingKeyIndexes = new ArrayList<>(shardingKeyNames.size());
        for (String shardingKey : shardingKeyNames) {
            shardingKeyIndexes.add(columns.indexOf(shardingKey));
            shardingKeyMetas.add(tableMeta.getColumnIgnoreCase(shardingKey));
        }

        List<String> relShardingKeyNames = BuildPlanUtils.getRelColumnNames(tableMeta, shardingKeyMetas);
        return BuildPlanUtils.buildResultForShardingTable(schemaName, tableMeta.getTableName(), values,
            relShardingKeyNames, shardingKeyIndexes, shardingKeyMetas, primaryKeyIndexes, ec);
    }

    /**
     * Build shard result for full table scan, we just add all values to each physical table
     *
     * @return {targetDb: {targetTb: [[index, pk]]}}
     */
    protected Map<String, Map<String, List<Pair<Integer, List<Object>>>>> getShardResultsFullTableScan(
        TableMeta tableMeta, Integer valuesCount) {
        String tableName = tableMeta.getTableName();
        String schemaName = tableMeta.getSchemaName();

        TddlRuleManager tddlRuleManager = ec.getSchemaManager(schemaName).getTddlRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();

        final Map<String, List<List<String>>> topology;
        if (partitionInfoManager.isNewPartDbTable(tableName)) {
            PartitionPruneStep partitionPruneStep = PartitionPruneStepBuilder.generateFullScanPruneStepInfo(schemaName,
                tableName, ec);
            PartPrunedResult partPrunedResult =
                PartitionPruner.doPruningByStepInfo(partitionPruneStep, ec);
            List<PartPrunedResult> resultList = new ArrayList<>();
            resultList.add(partPrunedResult);
            topology = PartitionPrunerUtils.buildTargetTablesByPartPrunedResults(resultList);
        } else {
            topology = HintPlanner.fullTableScan(ImmutableList.of(tableName), schemaName, ec);
        }

        // We do not need PK in result
        List<Pair<Integer, List<Object>>> valueList =
            IntStream.range(0, valuesCount).mapToObj(i -> new Pair<Integer, List<Object>>(i, null))
                .collect(Collectors.toList());
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> result = new HashMap<>();

        topology.forEach((group, phyTablesList) -> phyTablesList.stream()
            .map(phyTables -> phyTables.get(0))
            .forEach(phyTable -> result.computeIfAbsent(group, v -> new HashMap<>()).put(phyTable, valueList)));

        return result;
    }

    /**
     * Build SELECT for INSERT IGNORE / REPLACE / UPSERT
     * <p>
     * (SELECT ((value_index, uk_index), [select_key]) FROM DUAL WHERE (key1_column1, key1_column2) IN ((?, ?), ...) FOR UPDATE)
     * UNION
     * (SELECT ((value_index, uk_index), [select_key]) FROM DUAL WHERE (key2_column1, key2_column2) IN ((?, ?), ...) FOR UPDATE)
     */
    public List<RelNode> buildSelectUnionAndParam(LogicalInsert insert, List<List<String>> conditionKeys,
                                                  TableMeta tableMeta, LockMode lockMode,
                                                  List<List<Object>> values, List<String> insertColumns,
                                                  final List<Map<GroupKey, List<Object>>> deduplicated,
                                                  final List<Map<GroupKey, Pair<Integer, Integer>>> deduplicatedIndex,
                                                  final List<String> selectKey, boolean withValueIndex,
                                                  int maxSqlUnionCount, boolean isFullTableScan) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = isFullTableScan ?
            getShardResultsFullTableScan(tableMeta, values.size()) :
            getShardResults(tableMeta, values, insertColumns);

        // Build select row type
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        int columnOffset = withValueIndex ? 2 : 0;

        if (withValueIndex) {
            columns.add(new RelDataTypeFieldImpl("value_index", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("uk_index", 1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        }

        for (int i = 0; i < selectKey.size(); i++) {
            ColumnMeta columnMeta = tableMeta.getColumn(selectKey.get(i));
            columns.add(
                new RelDataTypeFieldImpl(columnMeta.getName(), i + columnOffset, columnMeta.getField().getRelType()));
        }
        RelDataType rowType = typeFactory.createStructType(columns);

        // Build condition key nodes, [(key1, key2), (key3, key4)]
        List<SqlNode> conditionKeyNodes = conditionKeys.stream()
            .map(PhysicalPlanBuilder::buildKeyNameNodeForInClause)
            .collect(Collectors.toList());

        List<RelNode> results = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbResult : shardResults.entrySet()) {
            String dbIndex = dbResult.getKey();
            Map<String, List<Pair<Integer, List<Object>>>> tbResults = dbResult.getValue();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbResult : tbResults.entrySet()) {
                String tbName = tbResult.getKey();
                Set<Integer> includeValueIndexes =
                    tbResult.getValue().stream().map(p -> p.left).collect(Collectors.toCollection(HashSet::new));

                // Build target table parameter
                buildTargetTable();
                initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);

                // Build sql directly, because UNION node can't be executed.
                StringBuilder unionSql = new StringBuilder();
                int sqlUnionCount = 0;
                final List<Integer> tableParamIndexes = new ArrayList<>();

                for (int i = 0; i < deduplicated.size(); i++) {
                    final SqlNode keyNameNode = conditionKeyNodes.get(i);
                    for (Map.Entry<GroupKey, List<Object>> dedup : deduplicated.get(i).entrySet()) {
                        GroupKey groupKey = dedup.getKey();
                        Pair<Integer, Integer> index = deduplicatedIndex.get(i).get(groupKey);
                        if (!isFullTableScan && !includeValueIndexes.contains(index.left)) {
                            // Value not in current shard, just skip
                            continue;
                        }
                        List<Object> key = dedup.getValue();

                        // Build select (value_index, uk_index, [pk])
                        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
                        if (withValueIndex) {
                            selectList.add(buildConstant(index.left, "value_index"));
                            selectList.add(buildConstant(index.right, "uk_index"));
                        }
                        for (String s : selectKey) {
                            selectList.add(new SqlIdentifier(s, SqlParserPos.ZERO));
                        }

                        // Build target table
                        final int tableParamIndex = buildParam(ParameterMethod.setTableName, "DUAL").getIndex() + 2;
                        tableParamIndexes.add(tableParamIndex);

                        // (key_column1, key_column2) = (?, ?)
                        SqlNode condition = buildInCondition(ImmutableList.of(key), keyNameNode);

                        // SELECT (value_index, uk_index) FROM DUAL WHERE (key_column1, key_column2) IN ((?, ?), ...) FOR UPDATE
                        SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
                            null,
                            selectList,
                            targetTableNode,
                            condition,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
                        sqlSelect.setLockMode(lockMode);

                        // (SELECT (value_index, uk_index) FROM DUAL WHERE (key1_column1, key1_column2) IN ((?, ?), ...) FOR UPDATE)
                        // UNION
                        // (SELECT (value_index, uk_index) FROM DUAL WHERE (key2_column1, key2_column2) IN ((?, ?), ...) FOR UPDATE)
                        if (unionSql.length() > 0) {
                            unionSql.append(" UNION ");
                        }
                        unionSql.append("(").append(RelUtils.toNativeSql(sqlSelect, DbType.MYSQL)).append(")");

                        sqlUnionCount++;
                        if (maxSqlUnionCount != 0 && sqlUnionCount >= maxSqlUnionCount) {
                            final List<String> targetTables = IntStream.range(0, sqlUnionCount).mapToObj(j -> {
                                // Replace DUAL with physical table name
                                tableParamIndexes.forEach(paramIndex -> currentParams.put(paramIndex,
                                    new ParameterContext(ParameterMethod.setTableName,
                                        new Object[] {paramIndex, tbName})));
                                return tbName;
                            }).collect(Collectors.toList());

                            final PhyTableOperation select = new PhyTableOperation(insert, rowType);
                            select.setDbIndex(dbIndex);
                            select.setTableNames(ImmutableList.of(targetTables));
                            select.setSqlTemplate(unionSql.toString());
                            select.setKind(SqlKind.SELECT);
                            select.setDbType(DbType.MYSQL);
                            select.setParam(new HashMap<>(currentParams));
                            select.setLockMode(lockMode);

                            results.add(select);

                            // reset
                            unionSql.setLength(0);
                            sqlUnionCount = 0;
                            tableParamIndexes.clear();

                            buildTargetTable();
                            initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);
                        }
                    }
                }

                if (sqlUnionCount != 0) {
                    final List<String> targetTables = IntStream.range(0, sqlUnionCount).mapToObj(j -> {
                        // Replace DUAL with physical table name
                        tableParamIndexes.forEach(paramIndex -> currentParams.put(paramIndex,
                            new ParameterContext(ParameterMethod.setTableName,
                                new Object[] {paramIndex, tbName})));
                        return tbName;
                    }).collect(Collectors.toList());

                    final PhyTableOperation select = new PhyTableOperation(insert, rowType);
                    select.setDbIndex(dbIndex);
                    select.setTableNames(ImmutableList.of(targetTables));
                    select.setSqlTemplate(unionSql.toString());
                    select.setKind(SqlKind.SELECT);
                    select.setDbType(DbType.MYSQL);
                    select.setParam(new HashMap<>(currentParams));
                    select.setLockMode(lockMode);

                    results.add(select);
                }
            }
        }

        return results;
    }
}
