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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author minggong.zm 2018-01-18
 */
public class PhyTableModifyBuilder extends PhyOperationBuilderCommon {

    private int nextDynamicIndex;
    private Map<Integer, ParameterContext> currentParams;

    /**
     * Build physical plan for insert, do not handle sequence
     *
     * @return physical insert plan
     */
    public static List<RelNode> buildInsert(LogicalInsert insert, List<List<Object>> values,
                                            ExecutionContext insertEc, boolean isGetShardResultForReplicationTable) {
        if (null == values || values.isEmpty()) {
            return new ArrayList<>();
        }
        final List<Map<Integer, ParameterContext>> batchParams = BuildPlanUtils.buildInsertBatchParam(values);
        final Parameters parameterSettings = insertEc.getParams();
        parameterSettings.setBatchParams(batchParams);

        // Separate values by sharding key
        final PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert,
            insertEc.getParams(),
            SequenceAttribute.getAutoValueOnZero(insertEc.getSqlMode()));

        final List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>(
            insertPartitioner.shardValues(insert.getInput(), insert.getLogicalTableName(), insertEc,
                isGetShardResultForReplicationTable));

        final PhyTableInsertBuilder phyTableInsertbuilder =
            new PhyTableInsertBuilder(insertPartitioner.getSqlTemplate(),
                insertEc,
                insert,
                insert.getDbType(),
                insert.getSchemaName());
        if (isGetShardResultForReplicationTable) {
            TableMeta tableMeta =
                insertEc.getSchemaManager(insert.getSchemaName()).getTable(insert.getLogicalTableName());
            PhyTableModifyBuilder.removeNonReplicateShardResultForInsert(shardResults, tableMeta);
        }

        // Build PhyTableOperation for insert
        return phyTableInsertbuilder.build(shardResults);
    }

    /**
     * Build batch update for each table: UPDATE tb SET c1=? WHERE pk1=? AND
     * pk2=?
     *
     * @param sourceExpressionBeginIndex index in selectList for
     * sourceExpression
     * @param shardResults targetDb: {targetTb: [[index1, pk1], [index2, pk2]]}
     * @param values selected result
     * @return physical plans
     */
    public List<RelNode> buildUpdate(LogicalModify logicalModify, String tableNameAlias, List<String> primaryKeyNames,
                                     int sourceExpressionBeginIndex,
                                     Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults,
                                     List<List<Object>> values) {
        SqlNode targetTableNode = RelUtils.buildTargetNode();

        List<Integer> pickedColumnIndexes = new ArrayList<>();
        SqlUpdate oldUpdate = (SqlUpdate) logicalModify.getOriginalSqlNode();
        SqlNodeList targetColumnList = buildTargetColumn(oldUpdate, tableNameAlias, pickedColumnIndexes);

        List<Integer> sourceExpressionIndexes = new ArrayList<>(pickedColumnIndexes.size());
        SqlNodeList sourceExpressionList = buildSourceExpression(pickedColumnIndexes,
            sourceExpressionBeginIndex,
            sourceExpressionIndexes);
        SqlNode condition = buildAndCondition(primaryKeyNames);

        List<RelNode> newPhysicalPlans = new ArrayList<>();
        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();

            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {

                final String targetTb = tbEntry.getKey();
                List<Pair<Integer, List<Object>>> indexAndPkValues = tbEntry.getValue();
                List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(indexAndPkValues.size());

                for (Pair<Integer, List<Object>> indexAndPkValue : indexAndPkValues) {
                    int rowIndex = indexAndPkValue.getKey();
                    List<Object> pkValue = indexAndPkValue.getValue();

                    resetParams();
                    buildTargetTableParams(targetTb);

                    buildSourceExpressionParams(values.get(rowIndex), sourceExpressionIndexes);
                    buildAndConditionParams(pkValue);

                    batchParams.add(currentParams);
                }

                SqlNode newNode = new SqlUpdate(SqlParserPos.ZERO,
                    targetTableNode,
                    targetColumnList,
                    sourceExpressionList,
                    condition,
                    null,
                    null);

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                RelDataType returnType = RelOptUtil.createDmlRowType(SqlKind.UPDATE, logicalModify.getCluster()
                    .getTypeFactory());
                PhyTableOperation operation = buildPhyTableOperation(newNode,
                    logicalModify,
                    targetDb,
                    tableNames,
                    returnType,
                    null,
                    batchParams);

                newPhysicalPlans.add(operation);
            }
        }

        return newPhysicalPlans;
    }

    /**
     * Build update for each table: UPDATE ? AS xx SET WHERE pk1 = ? AND pk2 = ? AND ...
     *
     * @param shardResults targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
     * @return physical plans
     */
    public List<RelNode> buildUpdateWithPk(LogicalModify logicalModify, List<List<Object>> distinctRows,
                                           Mapping updateSetMapping,
                                           Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults) {
        final SqlUpdate update = (SqlUpdate) logicalModify.getOriginalSqlNode();

        // Build physical plans
        final List<RelNode> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            // Target physical db
            final String targetDb = dbEntry.getKey();

            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {

                // Target physical tb
                final String targetTb = tbEntry.getKey();
                final List<Pair<Integer, List<Object>>> indexAndPkValues = tbEntry.getValue();
                final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(indexAndPkValues.size());

                for (Pair<Integer, List<Object>> indexAndPkValue : indexAndPkValues) {
                    final int rowIndex = indexAndPkValue.getKey();
                    final List<Object> pkValue = indexAndPkValue.getValue();

                    resetParams();

                    // TABLE NAME param
                    buildTargetTableParams(targetTb);

                    // SET param
                    Mappings.permute(distinctRows.get(rowIndex), updateSetMapping).forEach(this::addParam);

                    // CONDITION param
                    buildAndConditionParams(pkValue);

                    batchParams.add(currentParams);
                }

                final List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                final RelDataType returnType = RelOptUtil.createDmlRowType(SqlKind.UPDATE, logicalModify.getCluster()
                    .getTypeFactory());
                final PhyTableOperation operation = buildPhyTableOperation(update,
                    logicalModify,
                    targetDb,
                    tableNames,
                    returnType,
                    null,
                    batchParams);

                result.add(operation);
            }
        }

        return result;
    }

    /**
     * Build delete for each table: DELETE FROM tb WHERE (pk1, pk2) IN ((?, ?))
     *
     * @param shardResults targetDb: {targetTb: [[index1, pk1], [index2, pk2]]}
     * @return physical plans
     */
    public List<RelNode> buildDelete(LogicalModify logicalModify, List<String> primaryKeyNames,
                                     Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults) {
        SqlNode targetTableNode = RelUtils.buildTargetNode();

        List<RelNode> newPhysicalPlans = new ArrayList<>();
        SqlNode keyNameNodeForInClause = buildKeyNameNodeForInClause(primaryKeyNames);
        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();

            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {

                resetParams();

                // target table
                final String targetTb = tbEntry.getKey();
                buildTargetTableParams(targetTb);

                // in condition
                List<List<Object>> pkValues = tbEntry.getValue()
                    .stream()
                    .map(Pair::getValue)
                    .collect(Collectors.toList());
                SqlNode inCondition = buildInCondition(pkValues, keyNameNodeForInClause);

                SqlNode newNode = new SqlDelete(SqlParserPos.ZERO, targetTableNode, inCondition, null, null);

                List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                RelDataType returnType = RelOptUtil.createDmlRowType(SqlKind.DELETE, logicalModify.getCluster()
                    .getTypeFactory());
                PhyTableOperation operation = buildPhyTableOperation(newNode,
                    logicalModify,
                    targetDb,
                    tableNames,
                    returnType,
                    currentParams,
                    null);

                newPhysicalPlans.add(operation);
            }
        }

        return newPhysicalPlans;
    }

    /**
     * Build delete for each table: DELETE FROM tb WHERE (pk1, pk2) IN ((?, ?))
     *
     * @param shardResults targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
     * @return physical plans
     */
    public List<RelNode> buildDeleteWithPk(LogicalModify logicalModify,
                                           Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults,
                                           ExecutionContext ec) {
        final SqlNode targetTable = RelUtils.buildTargetNode();

        final SqlDelete originDelete = (SqlDelete) logicalModify.getOriginalSqlNode();
        final SqlIdentifier alias = originDelete.getAlias();

        // Build from with parameterized table name and alias
        final SqlBasicCall originFrom = (SqlBasicCall) originDelete.getFrom();
        assert originFrom.getKind() == SqlKind.AS;

        final SqlNode from = new SqlBasicCall(SqlStdOperatorTable.AS,
            new SqlNode[] {targetTable, originFrom.getOperands()[1]},
            SqlParserPos.ZERO);

        // Get column part of in condition
        final SqlBasicCall originCondition = (SqlBasicCall) originDelete.getCondition();
        assert originCondition.getKind() == SqlKind.IN;

        final SqlNode columnPart = originCondition.getOperands()[0];

        // Build physical plans
        final List<RelNode> result = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            // Target physical db
            final String targetDb = dbEntry.getKey();

            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {

                resetParams();

                // Target physical table
                final String targetTb = tbEntry.getKey();
                buildTargetTableParams(targetTb);

                // IN condition
                final List<List<Object>> pkValues = tbEntry.getValue()
                    .stream()
                    .map(Pair::getValue)
                    .collect(Collectors.toList());
                final SqlNode inCondition = buildInCondition(pkValues, columnPart);

                final SqlNode delete = FastSqlConstructUtils.collectTableInfo(new SqlDelete(SqlParserPos.ZERO,
                    targetTable,
                    inCondition,
                    null,
                    alias,
                    from,
                    null,
                    new SqlNodeList(ImmutableList.of(alias), SqlParserPos.ZERO),
                    null,
                    null,
                    originDelete.getKeywords(),
                    originDelete.getHints()), ec);

                final List<List<String>> tableNames = ImmutableList.of(ImmutableList.of(targetTb));
                final RelDataType returnType = RelOptUtil.createDmlRowType(SqlKind.DELETE,
                    logicalModify.getCluster().getTypeFactory());
                final PhyTableOperation operation = buildPhyTableOperation(delete,
                    logicalModify,
                    targetDb,
                    tableNames,
                    returnType,
                    currentParams,
                    null);

                result.add(operation);
            }
        }

        return result;
    }

    public static void removeNonReplicateShardResultForInsert(
        List<PhyTableInsertSharder.PhyTableShardResult> shardResults,
        TableMeta tableMeta) {
        if (GeneralUtil.isEmpty(shardResults)) {
            return;
        }
        Map<String, Set<String>> replicateDbIndexAndPhycialTables = new HashMap<>();
        for (PartitionSpec partitionSpec : tableMeta.getNewPartitionInfo().getPartitionBy().getPartitions()) {
            if (!partitionSpec.getLocation().isVisiable() && ComplexTaskPlanUtils
                .canWrite(tableMeta, partitionSpec.getName()) && !ComplexTaskPlanUtils
                .isDeleteOnly(tableMeta, partitionSpec.getName())) {
                PartitionLocation location = partitionSpec.getLocation();
                replicateDbIndexAndPhycialTables
                    .computeIfAbsent(location.getGroupKey().toUpperCase(), o -> new HashSet<>())
                    .add(location.getPhyTableName().toUpperCase());
            }
        }
        shardResults.removeIf(c -> !replicateDbIndexAndPhycialTables.containsKey(c.getGroupName().toUpperCase())
            || !replicateDbIndexAndPhycialTables.get(c.getGroupName().toUpperCase())
            .contains(c.getPhyTableName().toUpperCase()));
    }

    private void buildTargetTableParams(String tableName) {
        int tableIndex = PlannerUtils.TABLE_NAME_PARAM_INDEX + 2;
        currentParams.put(tableIndex, PlannerUtils.buildParameterContextForTableName(tableName, tableIndex));
    }

    private SqlNodeList buildTargetColumn(SqlUpdate oldUpdate, String tableNameAlias,
                                          List<Integer> pickedColumnIndexes) {
        SqlNodeList oldTargetColumns = oldUpdate.getTargetColumnList();
        SqlNodeList newTargetColumns = new SqlNodeList(oldTargetColumns.getParserPosition());

        for (int i = 0; i < oldTargetColumns.size(); i++) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) oldTargetColumns.get(i);
            List<String> names = sqlIdentifier.names;
            String columnName = null;
            if (names.size() > 1) {
                if (tableNameAlias.equalsIgnoreCase(names.get(0))) {
                    columnName = names.get(1);
                }
            } else {
                columnName = names.get(0);
            }

            if (columnName != null) {
                SqlIdentifier newSqlIdentifier = new SqlIdentifier(columnName, sqlIdentifier.getParserPosition());
                newTargetColumns.add(newSqlIdentifier);
                pickedColumnIndexes.add(i);
            }
        }

        return newTargetColumns;
    }

    private SqlNodeList buildSourceExpression(List<Integer> pickedColumnIndexes, int sourceExpressionBeginIndex,
                                              List<Integer> sourceExpressionIndexes) {
        SqlNodeList sourceExpressionList = new SqlNodeList(SqlParserPos.ZERO);

        for (int i = 0; i < pickedColumnIndexes.size(); i++) {
            SqlNode exp = new SqlDynamicParam(nextDynamicIndex++, SqlParserPos.ZERO);
            sourceExpressionList.add(exp);

            int index = pickedColumnIndexes.get(i) + sourceExpressionBeginIndex;
            sourceExpressionIndexes.add(index);
        }

        return sourceExpressionList;
    }

    private void buildSourceExpressionParams(List<Object> row, List<Integer> sourceExpressionIndexes) {
        for (int index : sourceExpressionIndexes) {
            addParam(row.get(index));
        }
    }

    private SqlNode buildAndCondition(List<String> primaryKeyNames) {
        final AtomicInteger paramIndex = new AtomicInteger(nextDynamicIndex);
        final SqlNode node = RelUtils.buildAndCondition(primaryKeyNames, paramIndex);
        nextDynamicIndex = paramIndex.get();
        return node;
    }

    private void buildAndConditionParams(List<Object> pkValues) {
        for (Object value : pkValues) {
            addParam(value);
        }
    }

    private static SqlNode buildKeyNameNodeForInClause(List<String> keyNames) {
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
     * Build the condition for SqlDelete. Format: IN( ROW( SqlIdentifier,
     * SqlIdentifier ), ROW( ROW( SqlDynamicParam, SqlDynamicParam), ROW(
     * SqlDynamicParam, SqlDynamicParam ) )
     *
     * @param pkValues each row stands for a composed primary key value
     * @param keyNameNode (pk1, pk2)
     * @return the condition node
     */
    private SqlNode buildInCondition(List<List<Object>> pkValues, SqlNode keyNameNode) {
        SqlNode[] allValues = new SqlNode[pkValues.size()];

        for (int i = 0; i < pkValues.size(); i++) {
            List<Object> pkRow = pkValues.get(i);

            if (pkRow.size() > 1) {
                SqlNode[] rowValues = new SqlNode[pkRow.size()];

                for (int j = 0; j < pkRow.size(); j++) {
                    // Why not use SqlLiteral: constructor of SqlLiteral is
                    // protected
                    rowValues[j] = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);
                    addParam(pkRow.get(j));
                }

                allValues[i] = new SqlBasicCall(SqlStdOperatorTable.ROW, rowValues, SqlParserPos.ZERO);
            } else {
                allValues[i] = new SqlDynamicParam(nextDynamicIndex, SqlParserPos.ZERO);
                addParam(pkRow.get(0));
            }
        }

        SqlNode outerRow = new SqlBasicCall(SqlStdOperatorTable.ROW, allValues, SqlParserPos.ZERO);

        return new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[] {keyNameNode, outerRow}, SqlParserPos.ZERO);
    }

    private static PhyTableOperation buildPhyTableOperation(SqlNode sqlNode, RelNode relNode, String dbIndex,
                                                            List<List<String>> tableNames, RelDataType rowType,
                                                            Map<Integer, ParameterContext> params,
                                                            List<Map<Integer, ParameterContext>> batchParams) {
        String sql = RelUtils.toNativeSql(sqlNode);
        PhyTableOperation operation =
            new PhyTableOperation(relNode.getCluster(), relNode.getTraitSet(), rowType, null, relNode);
        operation.setDbIndex(dbIndex);
        operation.setTableNames(tableNames);
        operation.setSqlTemplate(sql);
        operation.setKind(sqlNode.getKind());
        operation.setDbType(DbType.MYSQL);
        operation.setParam(params);
        operation.setBatchParameters(batchParams);
        return operation;
    }

    private void resetParams() {
        currentParams = new HashMap<>();
        nextDynamicIndex = 0;
    }

    private void addParam(Object value) {
        int paramIndex = nextDynamicIndex++;
        ParameterContext newPC = new ParameterContext(ParameterMethod.setObject1,
            new Object[] {paramIndex + 2, value});
        currentParams.put(paramIndex + 2, newPC);
    }

}
