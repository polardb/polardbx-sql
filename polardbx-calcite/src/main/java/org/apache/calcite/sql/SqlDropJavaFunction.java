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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class SqlDropJavaFunction extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlDropJavaFunctionOperator();

    protected String funcName;
    private boolean ifExists;

    private String tableName;

    public SqlDropJavaFunction(SqlParserPos pos, String funcName, boolean ifExists) {
        super(OPERATOR, pos);
        this.funcName = funcName;
        this.ifExists = ifExists;
        this.tableName = "_NONE_";
    }

    @Override
    public void unparse(SqlWriter writer, int lefPrec, int rightPrec) {
        writer.keyword("DROP JAVA FUNCTION");

        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        writer.literal(funcName);
    }

    public String getFuncName() {
        return funcName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getTableName() {
        return tableName;
    }
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlDropJavaFunctionOperator extends SqlSpecialOperator {

        public SqlDropJavaFunctionOperator() {
            super("DROP_JAVA_FUNCTION", SqlKind.DROP_JAVA_FUNCTION);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("DROP_JAVA_FUNCTION_RESULT",
                    0,
                    columnType)));
        }
    }
}
