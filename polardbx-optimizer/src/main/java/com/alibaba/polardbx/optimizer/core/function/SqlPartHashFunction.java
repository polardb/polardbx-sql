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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.type.InferTypes.FIRST_KNOWN;

public class SqlPartHashFunction extends SqlFunction {
    public SqlPartHashFunction(String funcName) {
        super(
            funcName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.MIX_OF_COLLATION_RETURN_VARCHAR,
            FIRST_KNOWN,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING);
    }
}
