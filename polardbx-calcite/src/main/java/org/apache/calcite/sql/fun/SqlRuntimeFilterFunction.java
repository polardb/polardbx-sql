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

package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class SqlRuntimeFilterFunction extends SqlFunction {

    private int id;
    private double guessSelectivity;

    public SqlRuntimeFilterFunction(int id, double guessSelectivity) {
        super(
            "BLOOMFILTER",
            SqlKind.RUNTIME_FILTER,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.family(SqlTypeFamily.ANY),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        this.id = id;
        this.guessSelectivity = guessSelectivity;
    }

    public int getId() {
        return id;
    }

    public double getGuessSelectivity() {
        return guessSelectivity;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword("BLOOMFILTER");
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");


        // Prepare statement place holder for bloom filter data, bloom filter length, bloom filter function num
        writer.sep(",");
        writer.print("?");
        writer.sep(",");
        writer.print("?");
        writer.sep(",");
        writer.print("?");

        for (SqlNode operand : call.getOperandList()) {
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }
}