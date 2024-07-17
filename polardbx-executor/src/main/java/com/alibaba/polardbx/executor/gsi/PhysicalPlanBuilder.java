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
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.utils.GroupKey;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyOperationBuilderCommon;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDmlKeyword;
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
import org.apache.calcite.sql.SqlSelectWithPartition;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlBinaryFunction;
import org.apache.calcite.sql.fun.SqlHashCheckAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
    public SqlNode sqlNode;
    private boolean convertToBinary;
    private final Set<String> notConvertColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    // char/varchar 不转hex，用于 fast checker，以及一些不需要做转换的场景
    public PhysicalPlanBuilder(String schemaName, ExecutionContext ec) {
        this.schemaName = schemaName;
        this.ec = ec;
        this.convertToBinary = false;
    }

    // char/varchar 转hex，用于 extractor, checker，以及需要做转换的场景
    public PhysicalPlanBuilder(String schemaName, boolean convertToBinary, ExecutionContext ec) {
        this.schemaName = schemaName;
        this.ec = ec;
        this.convertToBinary = convertToBinary;
    }

    public PhysicalPlanBuilder(String schemaName, boolean convertToBinary, List<String> notConvertColumns,
                               ExecutionContext ec) {
        this.schemaName = schemaName;
        this.ec = ec;
        this.convertToBinary = convertToBinary;
        if (CollectionUtils.isNotEmpty(notConvertColumns)) {
            this.notConvertColumns.addAll(notConvertColumns);
        }
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

    protected void buildCascadeParams(Map<String, ParameterContext> updateParamList, List<String> updateColumnList) {
        for (String column : updateColumnList) {
            // including target table
            currentParams.put(nextDynamicIndex + 2,
                PlannerUtils.changeParameterContextIndex(updateParamList.get(column), nextDynamicIndex + 2));
            nextDynamicIndex++;
        }
    }

    protected void buildNullParams(List<String> updateColumnList) {
        for (int i = 0; i < updateColumnList.size(); i++) {
            // including target table
            currentParams.put(nextDynamicIndex + 2,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {nextDynamicIndex + 2, null}));
            nextDynamicIndex++;
        }
    }

    /**
     * build PhyTableOperation according to SqlNode
     *
     * @param primaryTblPhyOp old PhyTableOperation or LogicalView or whatever, it
     * doesn't matter
     * @param rowType returned data types in one row
     */
    protected static PhyTableOperation buildPhyTableOperation(SqlNode sqlNode, RelNode primaryTblPhyOp, String dbIndex,
                                                              List<List<String>> tableNames, RelDataType rowType,
                                                              Map<Integer, ParameterContext> params, LockMode lockMode,
                                                              TableMeta tableMeta) {

        BytesSql sql = RelUtils.toNativeBytesSql(sqlNode, DbType.MYSQL);
//        PhyTableOperation operation =
//            new PhyTableOperation(primaryTblPhyOp.getCluster(), primaryTblPhyOp.getTraitSet(), rowType, null,
//                primaryTblPhyOp);
//        operation.setLogicalTableNames(ImmutableList.of(tableMeta.getTableName()));
//        operation.setSchemaName(tableMeta.getSchemaName());
//        operation.setDbIndex(dbIndex);
//        operation.setTableNames(tableNames);
//        operation.setBytesSql(sql);
//        operation.setKind(sqlNode.getKind());
//        operation.setDbType(DbType.MYSQL);
//        operation.setParam(params);
//        operation.setLockMode(lockMode);

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setSchemaName(tableMeta.getSchemaName());
        buildParams.setLogTables(ImmutableList.of(tableMeta.getTableName()));
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(tableNames);
        buildParams.setSqlKind(sqlNode.getKind());
        buildParams.setLockMode(lockMode);

        buildParams.setLogicalPlan(primaryTblPhyOp);
        buildParams.setCluster(primaryTblPhyOp.getCluster());
        buildParams.setTraitSet(primaryTblPhyOp.getTraitSet());
        buildParams.setRowType(rowType);
        buildParams.setCursorMeta(null);

        buildParams.setBytesSql(sql);
        buildParams.setDbType(DbType.MYSQL);
        buildParams.setDynamicParams(params);
        buildParams.setBatchParameters(null);

        PhyTableOperation operation = PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(buildParams);
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
            ColumnMeta columnMeta = tableMeta.getColumn(keyName);
            if (convertToBinary && !notConvertColumns.contains(keyName) && DataTypeUtil.isStringType(
                columnMeta.getDataType())) {
                selectList.add(buildBinaryFunction(keyName));
            } else {
                selectList.add(new SqlIdentifier(keyName, SqlParserPos.ZERO));
            }
            selectColumns.add(columnMeta);
        }

        return CalciteUtils.switchRowType(selectColumns, typeFactory);
    }

    private static SqlNode buildBinaryFunction(String columnName) {
        SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
        nodeList.add(new SqlIdentifier(columnName, SqlParserPos.ZERO));
        return new SqlBasicCall(new SqlBinaryFunction(), nodeList.toArray(), SqlParserPos.ZERO);
    }

    protected static SqlNode buildKeyNameNodeForInClause(List<String> keyNames) {
        // keyNameNode
        if (keyNames.size() > 1) {
            SqlNode[] keyNameNodes =
                keyNames.stream().map(keyName -> new SqlIdentifier(keyName, SqlParserPos.ZERO)).toArray(SqlNode[]::new);
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
            if (tableMeta.containsColumn(column.getLastName())) {
                columnList.add(column);
                expressionList.add(oldExpressionList.get(i));
            }
        }
    }

    private void buildFkTargetColumnList(SqlNodeList oldColumnList, SqlNodeList oldExpressionList,
                                         List<String> columns, Map<String, String> columnMap) {
        columnList = new SqlNodeList(SqlParserPos.ZERO);
        expressionList = new SqlNodeList(SqlParserPos.ZERO);
        nextDynamicIndex = 0;

        for (int i = 0; i < oldColumnList.size(); i++) {
            SqlIdentifier oldColumn = (SqlIdentifier) oldColumnList.get(i);
            for (String column : columns) {
                if (column.equalsIgnoreCase(columnMap.get(oldColumn.getLastName()))) {
                    columnList.add(new SqlIdentifier(column, SqlParserPos.ZERO));
                    SqlNode exp = new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO);
                    expressionList.add(exp);
//                    expressionList.add(oldExpressionList.get(i));
                }
            }
        }
    }

    /**
     * @return {targetDb: {targetTb: [[index, pk]]}}
     */
    protected Map<String, Map<String, List<Pair<Integer, List<Object>>>>> buildCommonInUpdateAndDelete(
        TableMeta tableMeta, List<List<Object>> selectedResults, List<String> selectKeys) {
        buildTargetTable();

        // primary key indexes and ColumnMetas
        List<String> primaryKeyNames = GlobalIndexMeta.getPrimaryKeys(tableMeta);
        List<Integer> primaryKeyIndexes =
            primaryKeyNames.stream().map(selectKeys::indexOf).collect(Collectors.toList());

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
        return BuildPlanUtils.buildResultForShardingTable(schemaName, tableMeta.getTableName(), selectedResults,
            relShardingKeyNames, shardingKeyIndexes, shardingKeyMetas, primaryKeyIndexes, ec);
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
    public List<RelNode> buildUpdate(SqlUpdate oldSqlUpdate, TableMeta tableMeta, List<List<Object>> selectedResults,
                                     List<String> selectKeys, PhyTableOperation oldPhysicalPlan) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
            buildCommonInUpdateAndDelete(tableMeta, selectedResults, selectKeys);

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
                List<List<Object>> pkValues =
                    tbEntry.getValue().stream().map(Pair::getValue).collect(Collectors.toList());

                initParams(0);
                buildTargetTableParams(targetTb);

                buildParams(ec.getParams().getCurrentParameter(), paramIndex, true);

                SqlNode inCondition = buildInCondition(pkValues, keyNameNodeForInClause);

                SqlNode newNode =
                    new SqlUpdate(SqlParserPos.ZERO, targetTableNode, columnList, expressionList, inCondition, null,
                        null);

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                PhyTableOperation operation =
                    buildPhyTableOperation(newNode, oldPhysicalPlan, targetDb, tableNames, oldPhysicalPlan.getRowType(),
                        currentParams, LockMode.UNDEF, tableMeta);

                newPhysicalPlans.add(operation);
            }
        }

        return newPhysicalPlans;
    }

    public List<RelNode> buildDelete(TableMeta tableMeta, List<List<Object>> selectedResults, List<String> selectKeys,
                                     PhyTableOperation oldPhysicalPlan) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
            buildCommonInUpdateAndDelete(tableMeta, selectedResults, selectKeys);

        // new plans to be returned
        List<RelNode> newPhysicalPlans = new ArrayList<>();

        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {
                String targetTb = tbEntry.getKey();
                List<List<Object>> pkValues =
                    tbEntry.getValue().stream().map(Pair::getValue).collect(Collectors.toList());

                initParams(0);
                buildTargetTableParams(targetTb);
                SqlNode inCondition = buildInCondition(pkValues, keyNameNodeForInClause);
                SqlNode newNode = new SqlDelete(SqlParserPos.ZERO, targetTableNode, inCondition, null, null);

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                PhyTableOperation operation =
                    buildPhyTableOperation(newNode, oldPhysicalPlan, targetDb, tableNames, oldPhysicalPlan.getRowType(),
                        currentParams, LockMode.UNDEF, tableMeta);

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
        List<SqlNode> keyNameNodeList =
            conditionKeys.stream().map(PhysicalPlanBuilder::buildKeyNameNodeForInClause).collect(Collectors.toList());

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

                List<List<Object>> keyValues = BuildPlanUtils.pickValuesFromInsert(sqlInsert, conditionKey,
                    new Parameters(insertPlan.getParam(), false), false, new ArrayList<>());

                SqlNode keyNameNode = keyNameNodeList.get(i);
                // for each row
                for (List<Object> row : keyValues) {
                    // build target table
                    buildParams(insertPlan.getParam(), tableIndex, false);

                    // build condition
                    SqlNode condition = buildInCondition(ImmutableList.of(row), keyNameNode);

                    SqlSelect sqlSelect =
                        new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, condition, null, null, null,
                            null, null, null);
                    sqlSelect.setLockMode(LockMode.EXCLUSIVE_LOCK);

                    if (unionSql.length() > 0) {
                        unionSql.append(" UNION ");
                    }
                    unionSql.append("(").append(RelUtils.toNativeSql(sqlSelect, DbType.MYSQL)).append(")");
                }
            }

            BytesSql sql = BytesSql.getBytesSql(unionSql.toString());

//            PhyTableOperation select =
//                new PhyTableOperation(insertPlan.getCluster(), insertPlan.getTraitSet(), rowType, null, insertPlan);
//            select.setDbIndex(insertPlan.getDbIndex());
//            select.setTableNames(insertPlan.getTableNames());
//            select.setBytesSql(BytesSql.getBytesSql(unionSql.toString()));
//            select.setKind(SqlKind.SELECT);
//            select.setDbType(DbType.MYSQL);
//            select.setParam(currentParams);
//            select.setLockMode(LockMode.EXCLUSIVE_LOCK);

            PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
            buildParams.setSchemaName(baseTableMeta.getSchemaName());
            buildParams.setLogTables(ImmutableList.of(baseTableMeta.getTableName()));
            buildParams.setGroupName(insertPlan.getDbIndex());
            buildParams.setPhyTables(insertPlan.getTableNames());
            buildParams.setSqlKind(SqlKind.SELECT);
            buildParams.setLockMode(LockMode.EXCLUSIVE_LOCK);

            buildParams.setLogicalPlan(insertPlan);
            buildParams.setCluster(insertPlan.getCluster());
            buildParams.setTraitSet(insertPlan.getTraitSet());
            buildParams.setRowType(rowType);
            buildParams.setCursorMeta(null);

            buildParams.setBytesSql(sql);
            buildParams.setDbType(DbType.MYSQL);
            buildParams.setDynamicParams(currentParams);
            buildParams.setBatchParameters(null);

            PhyTableOperation select = PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(buildParams);
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
                sqlSelect = new SqlSelect(SqlParserPos.ZERO, null, selectList, sqlUpdate.getTargetTable(),
                    sqlUpdate.getCondition(), null, null, null, sqlUpdate.getOrderList(), null, sqlUpdate.getFetch());
            } else {
                SqlDelete sqlDelete = (SqlDelete) sqlNode;
                sqlSelect = new SqlSelect(SqlParserPos.ZERO, null, selectList, sqlDelete.getTargetTable(),
                    sqlDelete.getCondition(), null, null, null, sqlDelete.getOrderList(), null, sqlDelete.getFetch());
            }

            initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);

            buildParams(plan.getParam(), sqlSelect, false);
            sqlSelect.setLockMode(LockMode.EXCLUSIVE_LOCK);

            PhyTableOperation select =
                buildPhyTableOperation(sqlSelect, plan, plan.getDbIndex(), plan.getTableNames(), rowType, currentParams,
                    LockMode.EXCLUSIVE_LOCK, baseTableMeta);

            selects.add(select);
        }

        return selects;
    }

    /**
     * <pre>
     * UPDATE {physical_primary_table}
     * SET {target_column} = {source_column}, {auto_update_column_1} = {auto_update_column_1}, ...
     * WHERE (pk0, ... , pkn) >= (?, ... , ?)
     * AND (pk0, ... , pkn) <= (?, ... , ?)
     * </pre>
     *
     * @param tableMeta Primary table meta
     * @param sourceNodes Source expressions
     * @param targetColumnNames Target column names
     * @return ExecutionPlan for extracting data from primary table
     */
    public PhyTableOperation buildUpdateForColumnBackfill(TableMeta tableMeta, List<SqlCall> sourceNodes,
                                                          List<String> targetColumnNames, List<String> primaryKeys,
                                                          boolean withLowerBound, boolean withUpperBound) {
        initParams(0);

        // build target table
        buildTargetTable();
        final List<SqlIdentifier> targetColumns = new ArrayList<>();
        final List<SqlNode> sourceExpressions = new ArrayList<>();

        for (int i = 0; i < sourceNodes.size(); i++) {
            targetColumns.add(new SqlIdentifier(targetColumnNames.get(i), SqlParserPos.ZERO));
            sourceExpressions.add(sourceNodes.get(i));
        }

        // Add auto update columns
        tableMeta.getAutoUpdateColumns().forEach(c -> {
            targetColumns.add(new SqlIdentifier(c.getName(), SqlParserPos.ZERO));
            sourceExpressions.add(new SqlIdentifier(c.getName(), SqlParserPos.ZERO));
        });

        // build where
        SqlNode condition = null;
        if (withLowerBound) {
            // WHERE (pk0, ... , pkn) >= (?, ... , ?)
            // HERE IS DIFFERENT FROM buildSelectMaxPkForBackfill!!!
            condition = buildCondition(primaryKeys, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
        }

        if (withUpperBound) {
            // WHERE (pk0, ... , pkn) <= (?, ... , ?)
            final SqlNode upperBound = buildCondition(primaryKeys, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

            condition =
                null == condition ? upperBound : PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
        }

        final SqlUpdate sqlUpdate =
            new SqlUpdate(SqlParserPos.ZERO, targetTableNode, new SqlNodeList(targetColumns, SqlParserPos.ZERO),
                new SqlNodeList(sourceExpressions, SqlParserPos.ZERO), condition, null, null);

        // create PhyTableOperation
        return buildDmlPhyTblOpTemplate(tableMeta.getSchemaName(), sqlUpdate, tableMeta);
    }

    /**
     * <pre>
     * INSERT INTO {target_physical_table} {target_columns}
     * select {source_columns} from {source_physical_table}
     * WHERE (pk0, ... , pkn) > (?, ... , ?)
     * AND (pk0, ... , pkn) <= (?, ... , ?)
     * ORDER BY pk0, ... , pkn
     * </pre>
     */
    public PhyTableOperation buildInsertSelectForOMCBackfill(TableMeta tableMeta,
                                                             List<String> targetColumnNames,
                                                             List<String> sourceColumnNames,
                                                             List<String> primaryKeys,
                                                             boolean withLowerBound, boolean withUpperBound,
                                                             LockMode lockMode, boolean isInsertIgnore) {
        SqlInsert sqlInsert = buildSqlInsertSelectForOMCBackfill(targetColumnNames, sourceColumnNames,
            primaryKeys, withLowerBound, withUpperBound, lockMode, isInsertIgnore, tableMeta.isHasPrimaryKey());
        return buildDmlPhyTblOpTemplate(tableMeta.getSchemaName(), sqlInsert, tableMeta);
    }

    public SqlInsert buildSqlInsertSelectForOMCBackfill(List<String> targetColumnNames,
                                                        List<String> sourceColumnNames,
                                                        List<String> primaryKeys,
                                                        boolean withLowerBound, boolean withUpperBound,
                                                        LockMode lockMode, boolean isInsertIgnore,
                                                        boolean forceIndex) {
        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        for (String columnName : sourceColumnNames) {
            selectList.add(new SqlIdentifier(columnName, SqlParserPos.ZERO));
        }

        // build target table
        buildTargetTable();

        final SqlIdentifier asNode = new SqlIdentifier("tb", SqlParserPos.ZERO);
        asNode.indexNode = new SqlNodeList(ImmutableList.of(
            new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO), null,
                new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString("PRIMARY", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO), SqlParserPos.ZERO)), SqlParserPos.ZERO);
        final SqlNode from =
            new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {targetTableNode, asNode}, SqlParserPos.ZERO);

        // build where
        SqlNode condition = null;
        if (withLowerBound) {
            // WHERE (pk0, ... , pkn) > (?, ... , ?)
            condition = buildCondition(primaryKeys, SqlStdOperatorTable.GREATER_THAN);
        }

        if (withUpperBound) {
            // WHERE (pk0, ... , pkn) <= (?, ... , ?)
            final SqlNode upperBound = buildCondition(primaryKeys, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

            condition =
                null == condition ? upperBound : PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
        }

        SqlNode target = forceIndex ? from : targetTableNode;
        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, target, condition, null, null, null, null,
                null, null);

        sqlSelect.setLockMode(lockMode);

        final SqlNode targetTableParam = BuildPlanUtils.buildTargetTable();

        final SqlNodeList targetColumnList =
            new SqlNodeList(targetColumnNames.stream().map(e -> new SqlIdentifier(e, SqlParserPos.ZERO)).collect(
                Collectors.toList()), SqlParserPos.ZERO);

        final SqlNodeList keywords = isInsertIgnore ?
            new SqlNodeList(ImmutableList.of(SqlDmlKeyword.IGNORE.symbol(SqlParserPos.ZERO)), SqlParserPos.ZERO)
            : SqlNodeList.EMPTY;

        return new SqlInsert(SqlParserPos.ZERO,
            keywords,
            targetTableParam,
            sqlSelect,
            targetColumnList,
            SqlNodeList.EMPTY,
            0,
            null);
    }

    /**
     * <pre>
     * UPDATE {physical_primary_table}
     * SET {target_column} = ?, {auto_update_column_1} = {auto_update_column_1}, ...
     * WHERE (pk0, ... , pkn) = (?, ... , ?)
     * </pre>
     *
     * @param tableMeta Primary table meta
     * @param sourceNodes Source expressions
     * @param targetColumnNames Target column names
     * @return ExecutionPlan for extracting data from primary table
     */
    public PhyTableOperation buildUpdateSingleRowForColumnBackfill(TableMeta tableMeta, List<SqlCall> sourceNodes,
                                                                   List<String> targetColumnNames,
                                                                   List<String> primaryKeys) {
        initParams(0);

        // build target table
        buildTargetTable();
        final List<SqlIdentifier> targetColumns = new ArrayList<>();
        final List<SqlNode> sourceExpressions = new ArrayList<>();

        for (int i = 0; i < sourceNodes.size(); i++) {
            targetColumns.add(new SqlIdentifier(targetColumnNames.get(i), SqlParserPos.ZERO));
            sourceExpressions.add(new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO));
        }

        // Add auto update columns
        tableMeta.getAutoUpdateColumns().forEach(c -> {
            targetColumns.add(new SqlIdentifier(c.getName(), SqlParserPos.ZERO));
            sourceExpressions.add(new SqlIdentifier(c.getName(), SqlParserPos.ZERO));
        });

        // build where
        SqlNode condition = buildEqualsCondition(primaryKeys);

        final SqlUpdate sqlUpdate = new SqlUpdate(SqlParserPos.ZERO,
            targetTableNode,
            new SqlNodeList(targetColumns, SqlParserPos.ZERO),
            new SqlNodeList(sourceExpressions, SqlParserPos.ZERO),
            condition,
            null,
            null
        );

        // create PhyTableOperation
        return buildDmlPhyTblOpTemplate(tableMeta.getSchemaName(), sqlUpdate, tableMeta);
    }

    /**
     * <pre>
     * UPDATE {physical_primary_table}
     * SET {target_column} = {source_column}, {auto_update_column_1} = {auto_update_column_1}, ...
     * WHERE (pk0, ... , pkn) > (?, ... , ?)
     * AND (pk0, ... , pkn) <= (?, ... , ?)
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * </pre>
     *
     * @param tableMeta Primary table meta
     * @param sourceNodes Source expressions
     * @param targetColumnNames Target column names
     * @return ExecutionPlan for extracting data from primary table
     */
    public PhyTableOperation buildUpdateReturningForColumnBackfill(TableMeta tableMeta, List<SqlCall> sourceNodes,
                                                                   List<String> targetColumnNames,
                                                                   List<String> primaryKeys, boolean withLowerBound,
                                                                   boolean withUpperBound) {
        initParams(0);

        // build target table
        buildTargetTable();
        final List<SqlIdentifier> targetColumns = new ArrayList<>();
        final List<SqlNode> sourceExpressions = new ArrayList<>();

        for (int i = 0; i < sourceNodes.size(); i++) {
            targetColumns.add(new SqlIdentifier(targetColumnNames.get(i), SqlParserPos.ZERO));
            sourceExpressions.add(sourceNodes.get(i));
        }

        // Add auto update columns
        tableMeta.getAutoUpdateColumns().forEach(c -> {
            targetColumns.add(new SqlIdentifier(c.getName(), SqlParserPos.ZERO));
            sourceExpressions.add(new SqlIdentifier(c.getName(), SqlParserPos.ZERO));
        });

        // build where
        SqlNode condition = null;
        if (withLowerBound) {
            // WHERE (pk0, ... , pkn) > (?, ... , ?)
            condition = buildCondition(primaryKeys, SqlStdOperatorTable.GREATER_THAN);
        }

        if (withUpperBound) {
            // WHERE (pk0, ... , pkn) <= (?, ... , ?)
            final SqlNode upperBound = buildCondition(primaryKeys, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);

            condition =
                null == condition ? upperBound : PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
        }

        // order by primary keys
        SqlNodeList orderBy = new SqlNodeList(
            primaryKeys.stream().map(key -> new SqlIdentifier(key, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        SqlNode fetch = new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO);

        final SqlUpdate sqlUpdate =
            new SqlUpdate(SqlParserPos.ZERO, targetTableNode, new SqlNodeList(targetColumns, SqlParserPos.ZERO),
                new SqlNodeList(sourceExpressions, SqlParserPos.ZERO), condition, null, null, orderBy, fetch, null);

        // create PhyTableOperation
        return buildDmlPhyTblOpTemplate(tableMeta.getSchemaName(), sqlUpdate, tableMeta);
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
        SqlNodeList orderBy = new SqlNodeList(primaryKeys.stream().map(
            key -> new SqlBasicCall(SqlStdOperatorTable.DESC, new SqlNode[] {new SqlIdentifier(key, SqlParserPos.ZERO)},
                SqlParserPos.ZERO)).collect(Collectors.toList()), SqlParserPos.ZERO);

        // limit 1
        SqlNode fetch = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

        // inner select
        SqlSelect select =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, null, null, null, null, orderBy, null,
                fetch);

        // create PhyTableOperation
        return buildSelectPhyTblOpTemplate(select, rowType, tableMeta, LockMode.UNDEF, ec);
    }

    public PhyTableOperation buildSelectForBackfill(TableMeta tableMeta, List<String> selectKeys,
                                                    List<String> primaryKeys, boolean withLowerBound,
                                                    boolean withUpperBound, LockMode lockMode) {
        return buildSelectForBackfill(tableMeta, selectKeys, primaryKeys, withLowerBound, withUpperBound,
            lockMode, null);
    }

    public PhyTableOperation buildSelectForBackfill(Extractor.ExtractorInfo info, boolean withLowerBound,
                                                    boolean withUpperBound, LockMode lockMode,
                                                    String physicalPartition) {
        return buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(), info.getPrimaryKeys(),
            withLowerBound, withUpperBound,
            lockMode, physicalPartition);
    }

    /**
     * <pre>
     *  SELECT pk0, ... , pkn, sourceColumn, targetColumn
     *  FROM ?
     *  WHERE ...
     *  ORDER BY pk0, ... , pkn
     *  LIMIT batchSize
     * </pre>
     *
     * @return Query plan
     */
    public PhyTableOperation buildSelectForColumnCheck(TableMeta tableMeta, List<String> primaryKeys, long batchSize,
                                                       String checkerColumn, String sourceColumn, String targetColumn,
                                                       boolean simpleChecker) {
        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        List<String> selectColumns = new ArrayList<>(primaryKeys);
        selectColumns.add(sourceColumn);
        selectColumns.add(targetColumn);
        RelDataType rowType = buildRowTypeForSelect(selectColumns, tableMeta, selectList);

        // build target table
        buildTargetTable();

        // order by
        SqlNodeList orderBy = new SqlNodeList(
            primaryKeys.stream().map(key -> new SqlIdentifier(key, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // limit
        SqlNode fetch = SqlLiteral.createExactNumeric(String.valueOf(batchSize), SqlParserPos.ZERO);

        // where
        SqlNode condition;
        SqlIdentifier checkerColumnNode = new SqlIdentifier(checkerColumn, SqlParserPos.ZERO);
        SqlIdentifier targetColumnNode = new SqlIdentifier(targetColumn, SqlParserPos.ZERO);
        if (simpleChecker) {
            SqlNode condition1 = new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[] {
                new SqlBasicCall(SqlStdOperatorTable.IS_NULL, new SqlNode[] {checkerColumnNode}, SqlParserPos.ZERO),
                new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, new SqlNode[] {targetColumnNode}, SqlParserPos.ZERO)},
                SqlParserPos.ZERO);
            SqlNode condition2 = new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[] {
                new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, new SqlNode[] {checkerColumnNode}, SqlParserPos.ZERO),
                new SqlBasicCall(SqlStdOperatorTable.IS_NULL, new SqlNode[] {targetColumnNode}, SqlParserPos.ZERO)},
                SqlParserPos.ZERO);
            condition =
                new SqlBasicCall(SqlStdOperatorTable.OR, new SqlNode[] {condition1, condition2}, SqlParserPos.ZERO);
        } else {
            condition = new SqlBasicCall(SqlStdOperatorTable.NOT, new SqlNode[] {
                new SqlBasicCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    new SqlNode[] {checkerColumnNode, targetColumnNode}, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
        }

        // inner select
        SqlSelect select =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, condition, null, null, null, orderBy,
                null, fetch);

        // create PhyTableOperation
        return buildSelectPhyTblOpTemplate(select, rowType, tableMeta, LockMode.UNDEF, ec);
    }

    /**
     * <pre>
     *  SELECT pk0, ... , pkn
     *  FROM ?
     *  WHERE ...
     * </pre>
     *
     * @return Query plan
     */
    public List<RelNode> buildSelectForCascade(TableMeta tableMeta,
                                               Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults,
                                               List<String> selectKeys, RelNode oldPhysicalPlan,
                                               List<String> refColumns, List<List<Object>> conditionValues) {

        // new plans to be returned
        List<RelNode> newPhysicalPlans = new ArrayList<>();

        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {
                String targetTb = tbEntry.getKey();

                initParams(0);
                buildTargetTable();
                buildTargetTableParams(targetTb);

                SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
                RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

                keyNameNodeForInClause = buildKeyNameNodeForInClause(refColumns);

                // where
                SqlNode inCondition = buildInCondition(conditionValues, keyNameNodeForInClause);

                // inner select
                SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
                    null,
                    selectList,
                    targetTableNode,
                    inCondition,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                PhyTableOperation operation = buildPhyTableOperation(sqlSelect,
                    oldPhysicalPlan,
                    targetDb,
                    tableNames,
                    rowType,
                    currentParams,
                    LockMode.EXCLUSIVE_LOCK,
                    tableMeta);

                newPhysicalPlans.add(operation);
            }
        }

        return newPhysicalPlans;
    }

    /**
     * <pre>
     *  ( SELECT pk0, ... , pkn
     *  FROM ?
     *  WHERE ...
     *  LIMIT 1 )
     *  UNION
     *  ( SELECT pk0, ... , pkn
     *  FROM ?
     *  WHERE ...
     *  LIMIT 1 )
     *  UNION ...
     * </pre>
     *
     * @return Query plan
     */
    public List<RelNode> buildSelectForCascadeUnionAll(TableMeta tableMeta,
                                                       Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults,
                                                       List<String> selectKeys, RelNode oldPhysicalPlan,
                                                       List<String> refColumns, List<List<Object>> conditionValues) {

        // new plans to be returned
        List<RelNode> newPhysicalPlans = new ArrayList<>();

        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {
                String targetTb = tbEntry.getKey();

                SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
                RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

                // Build sql directly, because UNION node can't be executed.
                StringBuilder unionSql = new StringBuilder();

                SqlNode keyNameNode = buildKeyNameNodeForInClause(refColumns);

                initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);

                // for each row
                for (List<Object> row : conditionValues) {

                    buildTargetTable();
                    buildParam(ParameterMethod.setTableName, targetTb);

                    // build condition
                    SqlNode inCondition = buildInCondition(ImmutableList.of(row), keyNameNode);

                    // limit 1
                    SqlNode fetch = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

                    // inner select
                    SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO,
                        null,
                        selectList,
                        targetTableNode,
                        inCondition,
                        null,
                        null,
                        null,
                        null,
                        null,
                        fetch);

                    sqlSelect.setLockMode(LockMode.EXCLUSIVE_LOCK);

                    if (unionSql.length() > 0) {
                        unionSql.append(" UNION ALL ");
                    }
                    unionSql.append("(").append(RelUtils.toNativeSql(sqlSelect, DbType.MYSQL)).append(")");

                }

                BytesSql sql = BytesSql.getBytesSql(unionSql.toString());

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));

                PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
                buildParams.setSchemaName(tableMeta.getSchemaName());
                buildParams.setLogTables(ImmutableList.of(tableMeta.getTableName()));
                buildParams.setGroupName(targetDb);
                buildParams.setPhyTables(tableNames);
                buildParams.setSqlKind(SqlKind.SELECT);
                buildParams.setLockMode(LockMode.EXCLUSIVE_LOCK);

                buildParams.setLogicalPlan(oldPhysicalPlan);
                buildParams.setCluster(oldPhysicalPlan.getCluster());
                buildParams.setTraitSet(oldPhysicalPlan.getTraitSet());
                buildParams.setRowType(rowType);
                buildParams.setCursorMeta(null);

                buildParams.setBytesSql(sql);
                buildParams.setDbType(DbType.MYSQL);
                buildParams.setDynamicParams(currentParams);
                buildParams.setBatchParameters(null);

                PhyTableOperation select = PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(buildParams);

                newPhysicalPlans.add(select);
            }
        }

        return newPhysicalPlans;
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
                                                    boolean withUpperBound, LockMode lockMode,
                                                    String physicalPartition) {
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

            condition =
                null == condition ? upperBound : PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
        }

        // order by primary keys
        SqlNodeList orderBy = new SqlNodeList(
            primaryKeys.stream().map(key -> new SqlIdentifier(key, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // limit ?
        SqlNode fetch = new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO);

        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, condition, null, null, null, orderBy,
                null, fetch);

        // lock mode
        sqlSelect.setLockMode(lockMode);

        SqlNode physicalSelect;
        if (physicalPartition != null && physicalPartition.length() > 0) {
            SqlSelectWithPartition sqlSelectWithPartition = SqlSelectWithPartition.create(sqlSelect, physicalPartition);
            physicalSelect = sqlSelectWithPartition;
        } else {
            physicalSelect = sqlSelect;
        }

        // create PhyTableOperation
        return buildSelectPhyTblOpTemplate(physicalSelect, rowType, tableMeta, lockMode, ec);
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
     * @return ExecutionPlan for extracting data from primary table
     */
    public PhyTableOperation buildSelectUpperBoundForInsertSelectBackfill(TableMeta tableMeta, List<String> selectKeys,
                                                                          List<String> primaryKeys,
                                                                          boolean withLowerBound,
                                                                          boolean withUpperBound) {
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

            condition =
                null == condition ? upperBound : PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
        }

        // order by primary keys
        SqlNodeList orderBy = new SqlNodeList(
            primaryKeys.stream().map(key -> new SqlIdentifier(key, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // limit ?
        SqlNode fetch = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
        SqlNode offset = new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO);

        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, condition, null, null, null, orderBy,
                offset, fetch);

        // lock mode
        sqlSelect.setLockMode(LockMode.UNDEF);

        // create PhyTableOperation
        return buildSelectPhyTblOpTemplate(sqlSelect, rowType, tableMeta, LockMode.UNDEF, ec);
    }

    public PhyTableOperation buildSelectForBackfillNotLimit(TableMeta tableMeta, List<String> selectKeys,
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

            condition =
                null == condition ? upperBound : PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
        }

        // order by primary keys
        SqlNodeList orderBy = new SqlNodeList(
            primaryKeys.stream().map(key -> new SqlIdentifier(key, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, condition, null, null, null, orderBy,
                null, null);

        // lock mode
        sqlSelect.setLockMode(lockMode);

        // create PhyTableOperation
        return buildSelectPhyTblOpTemplate(sqlSelect, rowType, tableMeta, lockMode, ec);
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
                                                                          List<String> primaryKeys, String forceIndex) {
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
            final SqlNode[] pkNodes =
                primaryKeys.stream().map(col -> new SqlIdentifier(col, SqlParserPos.ZERO)).toArray(SqlNode[]::new);
            pks = new SqlBasicCall(SqlStdOperatorTable.ROW, pkNodes, SqlParserPos.ZERO);
        }

        final SqlNode condition =
            new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[] {pks, new SqlNodeList(SqlParserPos.ZERO)},
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
            asNode.indexNode = new SqlNodeList(ImmutableList.of(
                new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO), null,
                    new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString(forceIndex, SqlParserPos.ZERO)),
                        SqlParserPos.ZERO), SqlParserPos.ZERO)), SqlParserPos.ZERO);
            from = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {targetTableNode, asNode}, SqlParserPos.ZERO);
        } else {
            from = null;
        }

        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, from != null ? from : targetTableNode, condition, null,
                null, null, orderBy, null, null);

        // create PhyTableOperation
        return new Pair<>(sqlSelect, buildSelectPhyTblOpTemplate(sqlSelect, rowType, tableMeta, LockMode.UNDEF, ec));
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
    public PhyTableOperation buildSelectHashCheckForChecker(TableMeta tableMeta, List<String> selectKeys,
                                                            List<String> conditionKeys, boolean withLowerBound,
                                                            boolean withUpperBound) {

        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

        // build target table
        buildTargetTable();

        //build where
        SqlNode condition = null;
        if (withLowerBound) {
            //where (k0, ..., kn) > (?, ..., ?)
            condition = buildCondition(conditionKeys, SqlStdOperatorTable.GREATER_THAN);
        }
        if (withUpperBound) {
            // where (k0, ..., kn) <= (?, ..., ?)
            final SqlNode upperBound = buildCondition(conditionKeys, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
            if (condition == null) {
                condition = upperBound;
            } else {
                condition = PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
            }
        }

        final SqlNode functionCallNode =
            new SqlBasicCall(new SqlHashCheckAggFunction(), selectList.toArray(), SqlParserPos.ZERO);

        final SqlNodeList selectListWithFunctionCall = new SqlNodeList(SqlParserPos.ZERO);
        selectListWithFunctionCall.add(functionCallNode);
        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectListWithFunctionCall, targetTableNode, condition, null, null,
                null, null, null, null);

        PhyTableOperation phyTableOperation =
            buildSelectPhyTblOpTemplate(sqlSelect, rowType, tableMeta, LockMode.UNDEF, ec);
        phyTableOperation.setNativeSqlNode(sqlSelect);
        return phyTableOperation;
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
    public PhyTableOperation buildSelectHashCheckForGSIChecker(TableMeta tableMeta,
                                                               List<String> selectKeys,
                                                               Map<String, String> virtualSelectKeys,
                                                               List<String> conditionKeys,
                                                               boolean withLowerBound,
                                                               boolean withUpperBound) {

        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

        // for modify partition key check
        if (MapUtils.isNotEmpty(virtualSelectKeys)) {
            for (int i = 0; i < selectList.size(); ++i) {
                SqlIdentifier colName = (SqlIdentifier) selectList.get(i);
                String colNameStr = colName.getLastName().toLowerCase();
                if (virtualSelectKeys.containsKey(colNameStr)) {
                    selectList.set(i, new SqlIdentifier(virtualSelectKeys.get(colNameStr), SqlParserPos.ZERO));
                }
            }
        }

        // build target table
        buildTargetTable();

        //build where
        SqlNode condition = null;
        if (withLowerBound) {
            //where (k0, ..., kn) > (?, ..., ?)
            condition = buildCondition(conditionKeys, SqlStdOperatorTable.GREATER_THAN);
        }
        if (withUpperBound) {
            // where (k0, ..., kn) <= (?, ..., ?)
            final SqlNode upperBound = buildCondition(conditionKeys, SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
            if (condition == null) {
                condition = upperBound;
            } else {
                condition = PlannerUtils.buildAndTree(ImmutableList.of(condition, upperBound));
            }
        }

        final SqlNode functionCallNode =
            new SqlBasicCall(new SqlHashCheckAggFunction(), selectList.toArray(), SqlParserPos.ZERO);

        final SqlNodeList selectListWithFunctionCall = new SqlNodeList(SqlParserPos.ZERO);
        selectListWithFunctionCall.add(functionCallNode);
        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectListWithFunctionCall, targetTableNode, condition, null, null,
                null, null, null, null);

        PhyTableOperation phyTableOperation =
            buildSelectPhyTblOpTemplate(sqlSelect, rowType, tableMeta, LockMode.UNDEF, ec);
        phyTableOperation.setNativeSqlNode(sqlSelect);
        return phyTableOperation;
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

        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, null, null, null, null, null, null,
                fetch);

        return buildSelectPhyTblOpTemplate(sqlSelect, rowType, tableMeta, LockMode.UNDEF, ec);
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
            final SqlNode[] pkNodes =
                primaryKeys.stream().map(col -> new SqlIdentifier(col, SqlParserPos.ZERO)).toArray(SqlNode[]::new);
            pks = new SqlBasicCall(SqlStdOperatorTable.ROW, pkNodes, SqlParserPos.ZERO);
            final SqlNode[] valueNodes = new SqlNode[primaryKeys.size()];
            for (int i = 0; i < valueNodes.length; ++i) {
                valueNodes[i] = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);
                ++nextDynamicIndex;
            }
            values = new SqlBasicCall(SqlStdOperatorTable.ROW, valueNodes, SqlParserPos.ZERO);
        }

        final SqlNode condition =
            new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[] {pks, values}, SqlParserPos.ZERO);

        final SqlDelete sqlDelete = new SqlDelete(SqlParserPos.ZERO, targetTableNode, condition, null, null);
        return buildDmlPhyTblOpTemplate(schemaName, sqlDelete, null);
    }

    private SqlNode buildEqualsCondition(List<String> primaryKeys) {
        // build PKs equals values condition
        final SqlNode pks, values;
        if (1 == primaryKeys.size()) {
            pks = new SqlIdentifier(primaryKeys.get(0), SqlParserPos.ZERO);
            values = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);
            ++nextDynamicIndex;
        } else {
            final SqlNode[] pkNodes =
                primaryKeys.stream().map(col -> new SqlIdentifier(col, SqlParserPos.ZERO)).toArray(SqlNode[]::new);
            pks = new SqlBasicCall(SqlStdOperatorTable.ROW, pkNodes, SqlParserPos.ZERO);
            final SqlNode[] valueNodes = new SqlNode[primaryKeys.size()];
            for (int i = 0; i < valueNodes.length; ++i) {
                valueNodes[i] = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);
                ++nextDynamicIndex;
            }
            values = new SqlBasicCall(SqlStdOperatorTable.ROW, valueNodes, SqlParserPos.ZERO);
        }

        return new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[] {pks, values}, SqlParserPos.ZERO);
    }

    public Pair<SqlDelete, PhyTableOperation> buildDeleteForChangeSet(TableMeta tableMeta, List<String> primaryKeys) {
        initParams(0);

        // build target table
        buildTargetTable();

        // build where
        final SqlNode pks;
        if (1 == primaryKeys.size()) {
            pks = new SqlIdentifier(primaryKeys.get(0), SqlParserPos.ZERO);
        } else {
            final SqlNode[] pkNodes =
                primaryKeys.stream().map(col -> new SqlIdentifier(col, SqlParserPos.ZERO)).toArray(SqlNode[]::new);
            pks = new SqlBasicCall(SqlStdOperatorTable.ROW, pkNodes, SqlParserPos.ZERO);
        }

        final SqlNode condition = new SqlBasicCall(SqlStdOperatorTable.IN,
            new SqlNode[] {pks, new SqlNodeList(SqlParserPos.ZERO)},
            SqlParserPos.ZERO);

        final SqlDelete sqlDelete = new SqlDelete(SqlParserPos.ZERO,
            targetTableNode,
            condition,
            null,
            null);
        return new Pair<>(sqlDelete, buildDmlPhyTblOpTemplate(schemaName, sqlDelete, tableMeta));
    }

    public Pair<SqlInsert, PhyTableOperation> buildReplaceForChangeSet(TableMeta tableMeta, List<String> columns) {
        // Construct targetColumnList
        final SqlNodeList targetColumnList = new SqlNodeList(
            columns.stream().map(col -> new SqlIdentifier(col, SqlParserPos.ZERO)).collect(Collectors.toList()),
            SqlParserPos.ZERO);

        // Construct target table
        final SqlNode targetTableParam = BuildPlanUtils.buildTargetTable();

        // Construct values
        final SqlNode[] dynamics = new SqlNode[targetColumnList.size()];
        for (int i = 0; i < targetColumnList.size(); i++) {
            dynamics[i] = new SqlDynamicParam(i, SqlParserPos.ZERO);
        }
        final SqlNode row = new SqlBasicCall(SqlStdOperatorTable.ROW, dynamics, SqlParserPos.ZERO);
        final SqlNode[] rowList = new SqlNode[] {row};
        final SqlNode values = new SqlBasicCall(SqlStdOperatorTable.VALUES, rowList, SqlParserPos.ZERO);

        final SqlInsert replacePlan =
            SqlInsert.create(SqlKind.REPLACE, SqlParserPos.ZERO, new SqlNodeList(SqlParserPos.ZERO), targetTableParam,
                values, targetColumnList, SqlNodeList.EMPTY, 0, null);

        return new Pair<>(replacePlan, buildDmlPhyTblOpTemplate(schemaName, replacePlan, tableMeta));
    }

    public PhyTableOperation buildDmlPhyTblOpTemplate(String schemaName, SqlNode sqlNode, TableMeta primTblMeta) {
        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, ec).createRelOptCluster();
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        RelDataType rowType = CalciteUtils.switchRowType(Collections.emptyList(), typeFactory);
        BytesSql sql = RelUtils.toNativeBytesSql(sqlNode);

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setSchemaName(schemaName);
        if (primTblMeta != null) {
            buildParams.setLogTables(ImmutableList.of(primTblMeta.getTableName()));
        }
        buildParams.setPhyTables(ImmutableList.of());
        buildParams.setSqlKind(sqlNode.getKind());

        buildParams.setCluster(cluster);
        buildParams.setTraitSet(traitSet);
        buildParams.setRowType(rowType);
        buildParams.setCursorMeta(null);

        buildParams.setBytesSql(sql);
        buildParams.setDbType(DbType.MYSQL);
        buildParams.setDynamicParams(new HashMap<>());
        buildParams.setBatchParameters(null);

        return PhyTableOperationFactory.getInstance().buildPhyTblOpTemplate(buildParams);
    }

    private PhyTableOperation buildSelectPhyTblOpTemplate(SqlNode sqlNode, RelDataType rowType, TableMeta logTableMeta,
                                                          LockMode lockMode, ExecutionContext ec) {

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        final RelOptCluster cluster = SqlConverter.getInstance(logTableMeta.getSchemaName(), ec).createRelOptCluster();
        RelTraitSet traitSet = RelTraitSet.createEmpty();

//        PhyTableOperation operation = new PhyTableOperation(cluster, traitSet, rowType);
//        BytesSql sql = RelUtils.toNativeBytesSql(sqlNode);
//        operation.setLogicalTableNames(ImmutableList.of(logTableMeta.getTableName()));
//        operation.setSchemaName(logTableMeta.getSchemaName());
//        operation.setBytesSql(sql);
//        operation.setKind(SqlKind.SELECT);
//        operation.setDbType(DbType.MYSQL);
//        operation.setLockMode(lockMode);

        List<String> logTables = ImmutableList.of(logTableMeta.getTableName());
        BytesSql sql = RelUtils.toNativeBytesSql(sqlNode);

        buildParams.setSchemaName(logTableMeta.getSchemaName());
        buildParams.setLogTables(logTables);

        buildParams.setCluster(cluster);
        buildParams.setTraitSet(traitSet);
        buildParams.setRowType(rowType);
        buildParams.setBytesSql(sql);
        buildParams.setLockMode(lockMode);
        buildParams.setSqlKind(SqlKind.SELECT);
        buildParams.setDbType(DbType.MYSQL);
        PhyTableOperation operation = PhyTableOperationFactory.getInstance().buildPhyTblOpTemplate(buildParams);
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
            throw new TddlNestableRuntimeException(
                MessageFormat.format("buildCondition not support SqlOperator {0}", operator.toString()));
        }

        // Gen operator tree.
        SqlNode root = null;
        for (int orIdx = 0; orIdx < columnNames.size(); ++orIdx) {
            SqlNode item = null;
            for (int andIdx = 0; andIdx <= orIdx; ++andIdx) {
                SqlNode comp = new SqlBasicCall(andIdx < orIdx ? SqlStdOperatorTable.EQUALS :
                    (orIdx != columnNames.size() - 1 ? nonFinalOp : operator), new SqlNode[] {
                    new SqlIdentifier(columnNames.get(andIdx), SqlParserPos.ZERO),
                    new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
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
                                                                   List<String> filterColumns, boolean useHint,
                                                                   ExecutionContext ec) {
        return new PhysicalPlanBuilder(schemaName, ec).buildPlanForBackfillDuplicateCheck(tableMeta, outputColumns,
            filterColumns, useHint);
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
                                                             List<String> filterColumns, boolean useHint) {
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
            final SqlNode compareNode =
                new SqlBasicCall(TddlOperatorTable.NULL_SAFE_EQUAL, new SqlNode[] {left, right}, SqlParserPos.ZERO);

            compareNodes.add(compareNode);
        }
        final SqlNode condition = PlannerUtils.buildAndTree(compareNodes);

        SqlSelect sqlSelect = new SqlSelect(SqlParserPos.ZERO, null, selectList,
            new SqlIdentifier(tableMeta.getTableName(), SqlParserPos.ZERO), condition, null, null, null, null, null,
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
    public Map<String, Map<String, List<Pair<Integer, List<Object>>>>> getShardResults(TableMeta tableMeta,
                                                                                       List<List<Object>> values,
                                                                                       List<String> columns,
                                                                                       boolean needPrimaryKeys) {

        // primary key indexes and ColumnMetas, columns must be upper case
        List<Integer> primaryKeyIndexes = new ArrayList<>();
        if (needPrimaryKeys) {
            List<String> primaryKeyNames = GlobalIndexMeta.getPrimaryKeys(tableMeta);
            primaryKeyIndexes = primaryKeyNames.stream()
                .map(columns::indexOf)
                .collect(Collectors.toList());
        }

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
    public Map<String, Map<String, List<Pair<Integer, List<Object>>>>> getShardResultsFullTableScan(
        TableMeta tableMeta, Integer valuesCount) {
        String tableName = tableMeta.getTableName();
        String schemaName = tableMeta.getSchemaName();

        TddlRuleManager tddlRuleManager = ec.getSchemaManager(schemaName).getTddlRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();

        final Map<String, List<List<String>>> topology;
        if (partitionInfoManager.isNewPartDbTable(tableName)) {
            PartitionPruneStep partitionPruneStep =
                PartitionPruneStepBuilder.genFullScanAllPhyPartsStepInfoByDbNameAndTbName(schemaName,
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

        topology.forEach((group, phyTablesList) -> phyTablesList.stream().map(phyTables -> phyTables.get(0))
            .forEach(phyTable -> result.computeIfAbsent(group, v -> new HashMap<>()).put(phyTable, valueList)));

        return result;
    }

    /**
     * Build SELECT for INSERT IGNORE / REPLACE / UPSERT
     * <p>
     * (SELECT ((value_index, uk_index), [select_key]) FROM DUAL WHERE (key1_column1, key1_column2) IN ((?, ?)) FOR UPDATE)
     * UNION
     * (SELECT ((value_index, uk_index), [select_key]) FROM DUAL WHERE (key2_column1, key2_column2) IN ((?, ?)) FOR UPDATE)
     */
    public List<RelNode> buildSelectUnionAndParam(LogicalInsert insert, List<List<String>> conditionKeys,
                                                  TableMeta tableMeta, LockMode lockMode, List<List<Object>> values,
                                                  List<String> insertColumns,
                                                  final List<List<List<Object>>> lookUpUniqueKey,
                                                  final List<List<Pair<Integer, Integer>>> lookUpUniqueKeyIndex,
                                                  final List<String> selectKey, boolean withValueIndex,
                                                  int maxSqlUnionCount, boolean isFullTableScan) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = isFullTableScan ?
            getShardResultsFullTableScan(tableMeta, values.size()) :
            getShardResults(tableMeta, values, insertColumns, true);

        Map<String, Set<String>> topology = new HashMap<>();
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbResult : shardResults.entrySet()) {
            String dbIndex = dbResult.getKey();
            topology.put(dbIndex, dbResult.getValue().keySet());
        }

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
        List<SqlNode> conditionKeyNodes =
            conditionKeys.stream().map(PhysicalPlanBuilder::buildKeyNameNodeForInClause).collect(Collectors.toList());

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

                for (int i = 0; i < lookUpUniqueKey.size(); i++) {
                    final SqlNode keyNameNode = conditionKeyNodes.get(i);
                    for (int j = 0; j < lookUpUniqueKey.get(i).size(); j++) {
                        Pair<Integer, Integer> index = lookUpUniqueKeyIndex.get(i).get(j);
                        if (!isFullTableScan && !includeValueIndexes.contains(index.left)) {
                            // Value not in current shard, just skip
                            continue;
                        }
                        List<Object> key = lookUpUniqueKey.get(i).get(j);

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

                        // SELECT (value_index, uk_index) FROM DUAL WHERE (key_column1, key_column2) IN ((?, ?)) FOR UPDATE
                        SqlSelect sqlSelect =
                            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, condition, null, null,
                                null, null, null, null);
                        sqlSelect.setLockMode(lockMode);

                        // (SELECT (value_index, uk_index) FROM DUAL WHERE (key1_column1, key1_column2) IN ((?, ?)) FOR UPDATE)
                        // UNION
                        // (SELECT (value_index, uk_index) FROM DUAL WHERE (key2_column1, key2_column2) IN ((?, ?)) FOR UPDATE)
                        if (unionSql.length() > 0) {
                            unionSql.append(" UNION ");
                        }
                        unionSql.append("(").append(RelUtils.toNativeSql(sqlSelect, DbType.MYSQL)).append(")");

                        sqlUnionCount++;
                        if (maxSqlUnionCount > 0 && sqlUnionCount >= maxSqlUnionCount) {
                            Map<String, Set<String>> singleTable = ImmutableMap.of(dbIndex, ImmutableSet.of(tbName));
                            results.addAll(buildSelectOnTable(insert, rowType, isFullTableScan ? topology : singleTable,
                                sqlUnionCount, tableParamIndexes, unionSql.toString(), lockMode, tableMeta));

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
                    Map<String, Set<String>> singleTable = ImmutableMap.of(dbIndex, ImmutableSet.of(tbName));
                    results.addAll(
                        buildSelectOnTable(insert, rowType, isFullTableScan ? topology : singleTable, sqlUnionCount,
                            tableParamIndexes, unionSql.toString(), lockMode, tableMeta));
                }

                // If it's a full table scan, then we have built all selects at once
                if (isFullTableScan) {
                    return results;
                }
            }
        }

        return results;
    }

    /**
     * Build SELECT for INSERT IGNORE / REPLACE / UPSERT that does not need valueIndex,
     * Use IN for same key, use UNION for different key
     * <p>
     * (SELECT ([select_key]) FROM DUAL WHERE (key1_column1, key1_column2) IN ((?, ?), ...) FOR UPDATE)
     * UNION
     * (SELECT ([select_key]) FROM DUAL WHERE (key2_column1, key2_column2) IN ((?, ?), ...) FOR UPDATE)
     */
    public List<RelNode> buildSelectInAndParam(LogicalInsert insert, List<List<String>> conditionKeys,
                                               List<String> ukNameList, TableMeta tableMeta, LockMode lockMode,
                                               List<List<Object>> values, List<String> insertColumns,
                                               final List<List<List<Object>>> lookUpUniqueKey,
                                               final List<List<Pair<Integer, Integer>>> lookUpUniqueKeyIndex,
                                               final List<String> selectKey, int maxSqlInCount,
                                               boolean isFullTableScan) {
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults = isFullTableScan ?
            getShardResultsFullTableScan(tableMeta, values.size()) :
            getShardResults(tableMeta, values, insertColumns, true);

        Map<String, Set<String>> topology = new HashMap<>();
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbResult : shardResults.entrySet()) {
            String dbIndex = dbResult.getKey();
            topology.put(dbIndex, dbResult.getValue().keySet());
        }

        // Build select row type
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        for (int i = 0; i < selectKey.size(); i++) {
            ColumnMeta columnMeta = tableMeta.getColumn(selectKey.get(i));
            columns.add(new RelDataTypeFieldImpl(columnMeta.getName(), i, columnMeta.getField().getRelType()));
        }
        RelDataType rowType = typeFactory.createStructType(columns);

        // Build condition key nodes, [(key1, key2), (key3, key4)]
        List<SqlNode> conditionKeyNodes =
            conditionKeys.stream().map(PhysicalPlanBuilder::buildKeyNameNodeForInClause).collect(Collectors.toList());

        List<RelNode> results = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbResult : shardResults.entrySet()) {
            String dbIndex = dbResult.getKey();
            Map<String, List<Pair<Integer, List<Object>>>> tbResults = dbResult.getValue();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbResult : tbResults.entrySet()) {
                String tbName = tbResult.getKey();
                Set<Integer> includeValueIndexes =
                    tbResult.getValue().stream().map(p -> p.left).collect(Collectors.toCollection(HashSet::new));

                final List<Integer> tableParamIndexes = new ArrayList<>();
                buildTargetTable();
                initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);

                StringBuilder unionSql = new StringBuilder();
                int sqlUnionCount = 0;
                int sqlInCount = 0;
                boolean newSelect = true;

                // Build batch SELECT for each UK using IN
                for (int i = 0; i < lookUpUniqueKey.size(); i++) {
                    final SqlNode keyNameNode = conditionKeyNodes.get(i);
                    final String localIndexName = ukNameList.get(i);

                    // Build select ([select_key])
                    SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
                    for (String s : selectKey) {
                        selectList.add(new SqlIdentifier(s, SqlParserPos.ZERO));
                    }

                    // build condition
                    List<List<Object>> keys = new ArrayList<>();

                    for (int j = 0; j < lookUpUniqueKey.get(i).size(); j++) {
                        if (newSelect) {
                            // We created a new select, so add table param
                            tableParamIndexes.add(buildParam(ParameterMethod.setTableName, "DUAL").getIndex() + 2);
                            newSelect = false;
                        }

                        Pair<Integer, Integer> index = lookUpUniqueKeyIndex.get(i).get(j);
                        if (!isFullTableScan && !includeValueIndexes.contains(index.left)) {
                            // Value not in current shard, just skip
                            continue;
                        }
                        List<Object> key = lookUpUniqueKey.get(i).get(j);

                        // (key_column1, key_column2) = (?, ?)
                        keys.add(key);
                        sqlInCount++;

                        if (maxSqlInCount > 0 && sqlInCount >= maxSqlInCount) {
                            SqlSelect sqlSelect =
                                buildSqlSelect(keys, keyNameNode, selectList, localIndexName, lockMode);
                            if (unionSql.length() > 0) {
                                unionSql.append(" UNION ");
                            }
                            unionSql.append("(").append(RelUtils.toNativeSql(sqlSelect, DbType.MYSQL)).append(")");
                            sqlUnionCount++;
                            Map<String, Set<String>> singleTable = ImmutableMap.of(dbIndex, ImmutableSet.of(tbName));
                            results.addAll(buildSelectOnTable(insert, rowType, isFullTableScan ? topology : singleTable,
                                sqlUnionCount, tableParamIndexes, unionSql.toString(), lockMode, tableMeta));

                            // reset
                            unionSql.setLength(0);
                            sqlUnionCount = 0;
                            sqlInCount = 0;
                            newSelect = true;
                            keys.clear();
                            tableParamIndexes.clear();

                            buildTargetTable();
                            initParams(PlannerUtils.TABLE_NAME_PARAM_INDEX);
                        }
                    }

                    if (keys.size() > 0) {
                        SqlSelect sqlSelect = buildSqlSelect(keys, keyNameNode, selectList, localIndexName, lockMode);
                        if (unionSql.length() > 0) {
                            unionSql.append(" UNION ");
                        }
                        unionSql.append("(").append(RelUtils.toNativeSql(sqlSelect, DbType.MYSQL)).append(")");
                        sqlUnionCount++;
                        newSelect = true;
                    }
                }

                // Union all select in one physical table
                if (sqlUnionCount > 0) {
                    Map<String, Set<String>> singleTable = ImmutableMap.of(dbIndex, ImmutableSet.of(tbName));
                    results.addAll(
                        buildSelectOnTable(insert, rowType, isFullTableScan ? topology : singleTable, sqlUnionCount,
                            tableParamIndexes, unionSql.toString(), lockMode, tableMeta));
                }
                // If it's a full table scan, then we have built all selects at once
                if (isFullTableScan) {
                    return results;
                }
            }
        }

        return results;
    }

    private List<RelNode> buildSelectOnTable(LogicalInsert insert, RelDataType rowType,
                                             Map<String, Set<String>> topology, int sqlUnionCount,
                                             List<Integer> tableParamIndexes, String sql, LockMode lockMode,
                                             TableMeta tableMeta) {
        List<RelNode> results = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            String dbIndex = entry.getKey();
            for (String phyTbName : entry.getValue()) {
                final List<String> targetTables = IntStream.range(0, sqlUnionCount).mapToObj(j -> {
                    // Replace DUAL with physical table name
                    tableParamIndexes.forEach(paramIndex -> currentParams.put(paramIndex,
                        new ParameterContext(ParameterMethod.setTableName, new Object[] {paramIndex, phyTbName})));
                    return phyTbName;
                }).collect(Collectors.toList());

//                final PhyTableOperation select = new PhyTableOperation(insert, rowType);
//                select.setLogicalTableNames(ImmutableList.of(tableMeta.getTableName()));
//                select.setSchemaName(tableMeta.getSchemaName());
//                select.setDbIndex(dbIndex);
//                select.setTableNames(ImmutableList.of(targetTables));
//                select.setBytesSql(BytesSql.getBytesSql(sql));
//                select.setKind(SqlKind.SELECT);
//                select.setDbType(DbType.MYSQL);
//                select.setParam(new HashMap<>(currentParams));
//                select.setLockMode(lockMode);

                PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
                buildParams.setSchemaName(tableMeta.getSchemaName());
                buildParams.setLogTables(ImmutableList.of(tableMeta.getTableName()));
                buildParams.setGroupName(dbIndex);
                buildParams.setPhyTables(ImmutableList.of(targetTables));
                buildParams.setSqlKind(SqlKind.SELECT);
                buildParams.setLockMode(lockMode);

                buildParams.setLogicalPlan(insert);
                buildParams.setCluster(insert.getCluster());
                buildParams.setTraitSet(insert.getTraitSet());
                buildParams.setRowType(rowType);
                buildParams.setCursorMeta(null);

                buildParams.setBytesSql(BytesSql.getBytesSql(sql));
                buildParams.setDbType(DbType.MYSQL);
                buildParams.setDynamicParams(new HashMap<>(currentParams));
                buildParams.setBatchParameters(null);

                final PhyTableOperation select =
                    PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(buildParams);
                results.add(select);
            }
        }
        return results;
    }

    private SqlSelect buildSqlSelect(List<List<Object>> keys, SqlNode keyNameNode, SqlNodeList selectList,
                                     String localIndexName, LockMode lockMode) {
        SqlNode condition = buildInCondition(keys, keyNameNode);

        final SqlNode from;
        if (localIndexName != null) {
            final SqlIdentifier asNode = new SqlIdentifier("tb", SqlParserPos.ZERO);
            asNode.indexNode = new SqlNodeList(ImmutableList.of(
                new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO), null,
                    new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString(localIndexName, SqlParserPos.ZERO)),
                        SqlParserPos.ZERO), SqlParserPos.ZERO)), SqlParserPos.ZERO);
            from = new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[] {targetTableNode, asNode}, SqlParserPos.ZERO);
        } else {
            from = null;
        }

        // SELECT ([select_key]) FROM DUAL WHERE (key_column1, key_column2) IN ((?, ?), ...) FOR UPDATE
        SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, from != null ? from : targetTableNode, condition, null,
                null, null, null, null, null);
        sqlSelect.setLockMode(lockMode);
        return sqlSelect;
    }

    public PhyTableOperation buildSqlSelectForSample(TableMeta tableMeta, List<String> selectKeys) {
        initParams(0);

        // build select list
        SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        RelDataType rowType = buildRowTypeForSelect(selectKeys, tableMeta, selectList);

        // build target table
        buildTargetTable();

        final SqlSelect sqlSelect =
            new SqlSelect(SqlParserPos.ZERO, null, selectList, targetTableNode, null, null, null, null, null,
                null, null);

        // create PhyTableOperation
        PhyTableOperation phyTableOperation =
            buildSelectPhyTblOpTemplate(sqlSelect, rowType, tableMeta, LockMode.UNDEF, ec);

        phyTableOperation.setNativeSqlNode(sqlSelect);
        return phyTableOperation;
    }
}
