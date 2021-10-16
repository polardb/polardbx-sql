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

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lingce.ldm 2018-02-02 15:39
 */
public class SqlNodeCloneVisitor extends SqlShuttle {

    @Override
    public SqlNode visit(SqlLiteral literal) {
        return SqlNode.clone(literal);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        return SqlNode.clone(id);
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        return SqlNode.clone(type);
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        return SqlNode.clone(param);
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        return SqlNode.clone(intervalQualifier);
    }

    @Override
    public SqlNode visit(SqlCall call) {
        List<SqlNode> operandList = call.getOperandList();
        List<SqlNode> newOperandList = new ArrayList<>(operandList);
        for (int i = 0; i < operandList.size(); i++) {
            SqlNode op = operandList.get(i);
            if (op == null) {
                continue;
            }

            SqlNode newOp = operandList.get(i).accept(new SqlNodeCloneVisitor());
            newOperandList.set(i, newOp);
        }

        SqlKind kind = call.getKind();
        switch (kind) {
        case DELETE:
            Preconditions.checkArgument(newOperandList.size() == 3);
            return new SqlDelete(call.getParserPosition(),
                newOperandList.get(0),
                newOperandList.get(1),
                null,
                (SqlIdentifier) newOperandList.get(2));
        case UPDATE:
            Preconditions.checkArgument(newOperandList.size() == 5);
            return new SqlUpdate(call.getParserPosition(),
                newOperandList.get(0),
                (SqlNodeList) newOperandList.get(1),
                (SqlNodeList) newOperandList.get(2),
                newOperandList.get(3),
                null,
                (SqlIdentifier) newOperandList.get(4));
        case INSERT:
            Preconditions.checkArgument(newOperandList.size() == 4);
            return new SqlInsert(call.getParserPosition(),
                (SqlNodeList) newOperandList.get(0),
                newOperandList.get(1),
                newOperandList.get(2),
                (SqlNodeList) newOperandList.get(3));
        default:
            return call.getOperator().createCall(call.getParserPosition(), newOperandList);
        }
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        return SqlNode.clone(nodeList);
    }
}
