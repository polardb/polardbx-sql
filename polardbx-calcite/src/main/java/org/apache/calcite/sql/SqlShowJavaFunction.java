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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

public class SqlShowJavaFunction extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowJavaFunctionOperator();

    public SqlShowJavaFunction(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers,
                               List<SqlNode> operands) {
        super(pos, specialIdentifiers, operands);
    }

    public static SqlShowJavaFunction create(SqlParserPos pos) {
        List<SqlSpecialIdentifier> sqlSpecialIdentifiers = new LinkedList<>();

        return new SqlShowJavaFunction(pos, sqlSpecialIdentifiers, ImmutableList.<SqlNode>of());
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_JAVA_FUNCTION;
    }

    public static class SqlShowJavaFunctionOperator extends SqlSpecialOperator {

        public SqlShowJavaFunctionOperator() {
            super("SHOW_JAVA_FUNCTION", SqlKind.SHOW_JAVA_FUNCTION);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("FunctionName", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
