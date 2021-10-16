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

package com.alibaba.polardbx.optimizer.core.function;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.util.Util;

import java.util.List;

public class MySQLMatchAgainst extends SqlFunction {
    private static final SqlWriter.FrameType FRAME_TYPE =
        SqlWriter.FrameTypeEnum.create("MATCH");

    public MySQLMatchAgainst() {
        super("MATCH_AGAINST",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY,
            SqlFunctionCategory.STRING);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
                        int rightPrec) {
        final SqlWriter.Frame frame =
            writer.startList(FRAME_TYPE, "MATCH", "");
        writer.sep("(");
        List<SqlNode> operandList = call.getOperandList();
        Preconditions.checkArgument(operandList.size() > 2);
        Preconditions.checkArgument(Util.last(operandList) instanceof SqlLiteral);

        final int size = operandList.size();
        for (int i = 0; i < size - 2; i++) {
            operandList.get(i).unparse(writer, 0, 0);
            if (i != size - 3) {
                writer.sep(",");
            }
        }
        writer.sep(")");
        writer.sep("AGAINST");
        writer.sep("(");
        operandList.get(size - 2).unparse(writer, 0, 0);
        final String searchModifier = ((SqlLiteral) Util.last(operandList)).toValue();
        writer.sep(searchModifier);
        writer.sep(")");

        writer.endList(frame);
    }

    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }

    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }
}
