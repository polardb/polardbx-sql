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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Definition of the SQL <code>HASHCHECK</code> aggregation function.
 *
 * <p><code>HASHCHECK</code> is an aggregator which returns the hash checksum of rows which
 * have gone into it. With one argument (or more), it returns the checksum of rows
 * for which that argument (or all) is not <code>null</code>.
 */
public class SqlHashCheckAggFunction extends SqlAggFunction {
    public SqlHashCheckAggFunction() {
        super("HASHCHECK", null, SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT_UNSIGNED, null,
            OperandTypes.ONE_OR_MORE,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false);
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.FUNCTION;
    }

    @SuppressWarnings("deprecation")
    public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
        return ImmutableList.of(
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), false));
    }

    @SuppressWarnings("deprecation")
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED);
    }

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
        return super.deriveType(validator, scope, call);
    }

    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz == SqlSplittableAggFunction.class) {
            return clazz.cast(SqlSplittableAggFunction.CountSplitter.INSTANCE);
        }
        return super.unwrap(clazz);
    }

}
