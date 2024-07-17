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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * @author wumu
 */
public class SqlBinaryFunction extends SqlFunction {
    public SqlBinaryFunction() {
        super(
            "BINARY",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_BINARY,
            null,
            OperandTypes.ANY_ANY_OR_ANY_ANY_ANY,
            SqlFunctionCategory.STRING);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        call.operand(0).unparse(writer, leftPrec, rightPrec);
        writer.endFunCall(frame);
    }
}
