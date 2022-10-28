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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BatchUpdateBuilder extends PhysicalPlanBuilder {

    public BatchUpdateBuilder(String schemaName, ExecutionContext ec) {
        super(schemaName, ec);
    }

    /**
     * Build batch update for duplicate data in INSERT ON DUPLICATE KEY UPDATE.
     *
     * @param duplicateRows [[index in logical insert, selected values]]
     * @param duplicateColumnNames column names of selected values
     * @return physical batch update plan
     */
    public List<RelNode> buildUpdate(SqlInsert oldSqlInsert, TableMeta tableMeta,
                                     List<Pair<Integer, List<Object>>> duplicateRows,
                                     List<String> duplicateColumnNames, RelNode oldPhysicalPlan) {

        List<List<Object>> duplicateValues = duplicateRows.stream().map(Pair::getValue).collect(Collectors.toList());
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResults =
            buildCommonInUpdateAndDelete(tableMeta,
                duplicateValues,
                duplicateColumnNames);

        // new plans to be returned
        List<RelNode> newPhysicalPlans = new ArrayList<>();

        initParams(0);
        // fixed params in expression list for each batch
        Map<Integer, ParameterContext> expressionParams = new HashMap<>();
        // which index corresponds to column value in insert
        Map<Integer, String> paramIndexToColumn = new HashMap<>();
        // convert column names to column indexes to speed up
        Map<Integer, Integer> paramIndexToColumnIndex = new HashMap<>();
        // build new expression list for update
        buildTargetColumnList(tableMeta,
            oldSqlInsert,
            ec.getParams().getCurrentParameter(),
            expressionParams,
            paramIndexToColumn,
            paramIndexToColumnIndex);
        if (columnList.size() == 0) {
            return newPhysicalPlans;
        }

        int startParamIndexForInCondition = nextDynamicIndex;
        SqlNode inCondition = null;
        List<Integer> inConditionParamIndex = null;
        List<SqlNode> rows = ((SqlCall) oldSqlInsert.getSource()).getOperandList();

        // build physical plans
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> dbEntry : shardResults.entrySet()) {
            String targetDb = dbEntry.getKey();
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> tbEntry : dbEntry.getValue().entrySet()) {

                String targetTb = tbEntry.getKey();
                List<Pair<Integer, List<Object>>> indexAndPkValues = tbEntry.getValue();
                List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>(indexAndPkValues.size());

                for (Pair<Integer, List<Object>> indexAndPkValue : indexAndPkValues) {
                    int rowIndex = indexAndPkValue.getKey();
                    List<Object> pkValue = indexAndPkValue.getValue();

                    initParams(0);
                    buildTargetTableParams(targetTb);

                    // put SET clause
                    currentParams.putAll(expressionParams);

                    // For VALUES function in SET clause, refer to the values in
                    // the INSERT statement.
                    setParamsInValuesCall(rows,
                        rowIndex,
                        paramIndexToColumnIndex,
                        paramIndexToColumn,
                        ec.getParams());

                    // put WHERE clause
                    if (inCondition == null) {
                        nextDynamicIndex = startParamIndexForInCondition;
                        inCondition = buildInCondition(ImmutableList.of(pkValue), keyNameNodeForInClause);
                        inConditionParamIndex = PlannerUtils.getDynamicParamIndex(inCondition);
                    } else {
                        for (int i = 0; i < inConditionParamIndex.size(); i++) {
                            int paramIndex = inConditionParamIndex.get(i);
                            currentParams.put(paramIndex + 2, new ParameterContext(ParameterMethod.setObject1,
                                new Object[] {paramIndex + 2, pkValue.get(i)}));
                        }
                    }

                    batchParams.add(currentParams);
                }

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
                    null,
                    LockMode.UNDEF,
                    tableMeta);
                operation.setBatchParameters(batchParams);

                newPhysicalPlans.add(operation);
            }
        }

        return newPhysicalPlans;
    }

    /**
     * For VALUES function in SET clause, refer to the values in the INSERT
     * statement.
     *
     * @param rows rows in the logical INSERT
     * @param rowIndex current row index
     * @param paramIndexToColumnIndex {param index : column index}
     * @param paramIndexToColumn {param index : column name}
     * @param parameters parameters for the logical INSERT
     */
    private void setParamsInValuesCall(List<SqlNode> rows, int rowIndex, Map<Integer, Integer> paramIndexToColumnIndex,
                                       Map<Integer, String> paramIndexToColumn, Parameters parameters) {

        for (Map.Entry<Integer, Integer> entry : paramIndexToColumnIndex.entrySet()) {
            int paramIndex = entry.getKey();
            int columnIndex = entry.getValue();

            SqlNode rowExpression;
            Map<Integer, ParameterContext> oldParams;
            if (parameters.isBatch()) {
                rowExpression = ((SqlCall) rows.get(0)).getOperandList().get(columnIndex);
                oldParams = parameters.getBatchParameters().get(rowIndex);
            } else {
                rowExpression = ((SqlCall) rows.get(rowIndex)).getOperandList().get(columnIndex);
                oldParams = parameters.getCurrentParameter();
            }

            Object value;
            if (rowExpression instanceof SqlLiteral) {
                value = BuildPlanUtils.getValueForParameter((SqlLiteral) rowExpression);
            } else if (rowExpression instanceof SqlDynamicParam) {
                int oldParamIndex = ((SqlDynamicParam) rowExpression).getIndex();
                value = oldParams.get(oldParamIndex + 1).getValue();
            } else {
                // impossible here, it must have been calculated in
                // ReplaceCallWithLiteralVisitor
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    "Column '" + paramIndexToColumn.get(paramIndex) + "' can only be literal");
            }

            ParameterContext newPC = new ParameterContext(ParameterMethod.setObject1, new Object[] {
                paramIndex + 2,
                value});
            currentParams.put(paramIndex + 2, newPC);
        }
    }

    /**
     * Convert ON DUPLICATE KEY UPDATE clause in INSERT into SET clause in
     * UPDATE. Results are in columnList and expressionList.
     */
    private void buildTargetColumnList(TableMeta tableMeta, SqlInsert sqlInsert,
                                       Map<Integer, ParameterContext> oldParams,
                                       Map<Integer, ParameterContext> newParams,
                                       Map<Integer, String> paramIndexToColumn,
                                       Map<Integer, Integer> paramIndexToColumnIndex) {
        columnList = new SqlNodeList(SqlParserPos.ZERO);
        expressionList = new SqlNodeList(SqlParserPos.ZERO);

        SqlNodeList updateList = sqlInsert.getUpdateList();
        for (SqlNode node : updateList) {
            if (!(node instanceof SqlCall)) {
                continue;
            }
            SqlCall call = (SqlCall) node;
            SqlIdentifier column = (SqlIdentifier) call.getOperandList().get(0);
            if (tableMeta.getColumnIgnoreCase(column.getLastName()) != null) {
                columnList.add(column);

                SqlNode newExpression = convertExpression(call.getOperandList().get(1),
                    oldParams,
                    newParams,
                    paramIndexToColumn);
                expressionList.add(newExpression);
            }
        }

        // convert column names to column indexes to speed up
        if (!paramIndexToColumn.isEmpty()) {
            List<Map.Entry<Integer, String>> paramIndexAndColumnList =
                Lists.newArrayList(paramIndexToColumn.entrySet());
            List<String> columnNames = paramIndexAndColumnList.stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
            List<Integer> columnIndexes = BuildPlanUtils.getColumnIndexesInInsert(sqlInsert, columnNames);

            for (int i = 0; i < columnIndexes.size(); i++) {
                paramIndexToColumnIndex.put(paramIndexAndColumnList.get(i).getKey(), columnIndexes.get(i));
            }
        }
    }

    /**
     * Convert ON DUPLICATE KEY UPDATE clause in INSERT into SET clause in
     * UPDATE. This function converts recursively. For DynamicParam, put it in
     * newParams. For VALUES(col), put param index and column name in
     * paramIndexToColumn.
     *
     * @param sqlNode the node to be converted
     * @param paramIndexToColumn output parameter
     * @return converted node
     */
    private SqlNode convertExpression(SqlNode sqlNode, Map<Integer, ParameterContext> oldParams,
                                      Map<Integer, ParameterContext> newParams,
                                      Map<Integer, String> paramIndexToColumn) {
        if (sqlNode instanceof SqlDynamicParam) {
            // return a new one with new index
            int oldParamIndex = ((SqlDynamicParam) sqlNode).getIndex();
            ParameterContext newPc = PlannerUtils.changeParameterContextIndex(oldParams.get(oldParamIndex + 1),
                nextDynamicIndex + 2);
            newParams.put(nextDynamicIndex + 2, newPc);
            return new SqlDynamicParam(nextDynamicIndex++, ((SqlDynamicParam) sqlNode).getTypeName(),
                sqlNode.getParserPosition());

        } else if (sqlNode instanceof SqlNodeList) {
            // impossible actually
            SqlNodeList newSqlNodeList = new SqlNodeList((sqlNode.getParserPosition()));
            for (SqlNode subNode : (SqlNodeList) sqlNode) {
                newSqlNodeList.add(convertExpression(subNode, oldParams, newParams, paramIndexToColumn));
            }
            return newSqlNodeList;

        } else if (sqlNode instanceof SqlCall) {
            SqlCall call = (SqlCall) sqlNode;
            // VALUES(col), refer to the corresponding value
            if (call.getOperator() == TddlOperatorTable.VALUES) {
                String columnName = ((SqlIdentifier) call.getOperandList().get(0)).getLastName();
                paramIndexToColumn.put(nextDynamicIndex, columnName);
                return new SqlDynamicParam(nextDynamicIndex++, sqlNode.getParserPosition());
            }

            // for other functions, convert recursively
            List<SqlNode> oldOperands = call.getOperandList();
            SqlNode[] newOperands = new SqlNode[oldOperands.size()];
            for (int i = 0; i < oldOperands.size(); i++) {
                newOperands[i] = oldOperands.get(i) != null ? convertExpression(oldOperands.get(i),
                    oldParams,
                    newParams,
                    paramIndexToColumn) : null;
            }
            return call.getOperator().createCall(call.getFunctionQuantifier(), call.getParserPosition(), newOperands);

        } else {
            // shallow copy
            return sqlNode;
        }
    }
}
