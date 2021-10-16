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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlReplace;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.List;
import java.util.Map;

/**
 * @author minggong.zm 2018/1/26 Create a new SqlInsert and corresponding
 * Parameters, which contains only the rows for a specific physical table.
 */
public class BuildInsertValuesVisitor extends SqlShuttle {

    private boolean isBatch;
    // the global params
    private Parameters parameters;
    private boolean inUpdateList = false;
    private int curValueIndex = -1;

    // null stands for all values
    private List<Integer> valueIndices;
    // built params for current insert
    private Map<Integer, ParameterContext> outputParams;

    public BuildInsertValuesVisitor(List<Integer> valueIndices, Parameters parameters,
                                    Map<Integer, ParameterContext> outputParams) {
        super();
        this.valueIndices = valueIndices;
        this.parameters = parameters;
        this.outputParams = outputParams;
        this.isBatch = parameters != null && parameters.isBatch();
    }

    public SqlInsert visit(SqlInsert insert) {
        List<SqlNode> operandList = insert.getOperandList();
        SqlNode oldSource = operandList.get(2);
        SqlNode newSource = oldSource.accept(this);

        if (insert.getKind() == SqlKind.INSERT) {
            SqlNodeList oldUpdateList = (SqlNodeList) operandList.get(4);
            inUpdateList = true;
            SqlNodeList newUpdateList = (SqlNodeList) oldUpdateList.accept(this);

            return new SqlInsert(insert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                operandList.get(1),
                newSource,
                (SqlNodeList) operandList.get(3),
                newUpdateList,
                0,
                null);
        } else {
            return new SqlReplace(insert.getParserPosition(),
                (SqlNodeList) operandList.get(0),
                operandList.get(1),
                newSource,
                (SqlNodeList) operandList.get(3),
                0,
                null);
        }
    }

    public SqlNode visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            if (call.getKind() == SqlKind.VALUES) {
                return buildSqlValues((SqlBasicCall) call);
            } else if (call.getKind() == SqlKind.ROW) {
                return buildSqlRow((SqlBasicCall) call);
            }
        }

        List<SqlNode> operands = call.getOperandList();
        SqlNode[] newOperands = new SqlNode[operands.size()];
        for (int i = 0; i < operands.size(); i++) {
            newOperands[i] = operands.get(i) != null ? operands.get(i).accept(this) : null;
        }
        return call.getOperator().createCall(call.getFunctionQuantifier(), call.getParserPosition(), newOperands);
    }

    /**
     * Copy sharded rows by their indices.
     */
    private SqlBasicCall buildSqlValues(SqlBasicCall call) {
        SqlNode[] oldRows = call.getOperands();
        SqlNode[] valuesOperands;

        if (valueIndices != null) {
            valuesOperands = new SqlBasicCall[valueIndices.size()];
            for (int i = 0; i < valueIndices.size(); i++) {
                curValueIndex = valueIndices.get(i);
                int index = isBatch ? 0 : curValueIndex;
                valuesOperands[i] = oldRows[index].accept(this); // ROW
            }
        } else {
            int num = isBatch ? parameters.getBatchSize() : oldRows.length;
            valuesOperands = new SqlBasicCall[num];
            for (int i = 0; i < num; i++) {
                curValueIndex = i;
                int index = isBatch ? 0 : i;
                valuesOperands[i] = oldRows[index].accept(this); // ROW
            }
        }

        return new SqlBasicCall(call.getOperator(),
            valuesOperands,
            call.getParserPosition(),
            call.isExpanded(),
            call.getFunctionQuantifier());
    }

    private SqlNode buildSqlRow(SqlBasicCall call) {
        SqlNode[] operands = call.getOperands();
        SqlNode[] newOperands = new SqlNode[operands.length];
        for (int i = 0; i < operands.length; i++) {
            newOperands[i] = operands[i].accept(this);
        }

        return new SqlBasicCall(call.getOperator(),
            newOperands,
            call.getParserPosition(),
            call.isExpanded(),
            call.getFunctionQuantifier());
    }

    public SqlDynamicParam visit(SqlDynamicParam dynamicParam) {
        int oldIndex = dynamicParam.getIndex();
        int newIndex = outputParams.size();

        if (parameters != null) {
            ParameterContext oldPc;
            if (isBatch) {
                oldPc = parameters.getBatchParameters().get(curValueIndex).get(oldIndex + 1);
            } else {
                oldPc = parameters.getCurrentParameter().get(oldIndex + 1);
            }

            ParameterContext newPc = PlannerUtils.changeParameterContextIndex(oldPc, newIndex + 1);
            outputParams.put(newIndex + 1, newPc);
        }

        // DynamicParam.index + 2 = params.key
        return new SqlDynamicParam(newIndex - 1, SqlParserPos.ZERO, dynamicParam.getValue());
    }

    public SqlNode visit(SqlLiteral literal) {
        return SqlNode.clone(literal);
    }
}
