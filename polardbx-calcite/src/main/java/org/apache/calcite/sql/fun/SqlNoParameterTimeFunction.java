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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

public class SqlNoParameterTimeFunction extends SqlFunction {
    public SqlNoParameterTimeFunction(String name, SqlKind kind,
                                      SqlReturnTypeInference returnTypeInference,
                                      SqlOperandTypeInference operandTypeInference,
                                      SqlOperandTypeChecker operandTypeChecker) {
        super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, SqlFunctionCategory.TIMEDATE);
    }

    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION_ID;
    }

    // All of the time functions are increasing. Not strictly increasing.
    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.INCREASING;
    }

    // Plans referencing context variables should never be cached
    public boolean isDynamicFunction() {
        return true;
    }

}
