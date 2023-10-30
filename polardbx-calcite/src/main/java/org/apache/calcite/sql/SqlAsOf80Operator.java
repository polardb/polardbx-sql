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

package org.apache.calcite.sql;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class SqlAsOf80Operator extends SqlSpecialOperator{
    public SqlAsOf80Operator() {
        this(
            "AS OF GCN",
            SqlKind.AS_OF,
            20,
            true,
            ReturnTypes.ARG0,
            InferTypes.RETURN_TYPE,
            OperandTypes.ANY_ANY);
    }
    protected SqlAsOf80Operator(String name, SqlKind kind, int prec,
                                boolean leftAssoc, SqlReturnTypeInference returnTypeInference,
                                SqlOperandTypeInference operandTypeInference,
                                SqlOperandTypeChecker operandTypeChecker) {
        super(name, kind, prec, leftAssoc, returnTypeInference,
            operandTypeInference, operandTypeChecker);
    }
    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        assert call.operandCount() >= 2;
        final SqlWriter.Frame frame =
            writer.startList(
                SqlWriter.FrameTypeEnum.SIMPLE);
        call.operand(0).unparse(writer, leftPrec, getLeftPrec());
        final boolean needsSpace = true;
        writer.setNeedWhitespace(needsSpace);
        if (writer.getDialect().allowsAsOf()) {
            writer.sep("AS OF GCN");
            writer.setNeedWhitespace(needsSpace);
        }
        call.operand(1).unparse(writer, getRightPrec(), rightPrec);
        writer.endList(frame);
    }
}

