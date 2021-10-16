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

import java.util.List;

import lombok.Data;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

/**
 * @author chenmo.cm
 * @date 2018/7/6 下午4:34
 */
@Data
public class SqlDropDatabase extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlDropDatabaseOperator();

    final boolean                           ifExists;
    final SqlIdentifier                     dbName;

    public SqlDropDatabase(SqlParserPos pos, boolean ifExists, SqlIdentifier dbName){
        super(OPERATOR, pos);
        this.ifExists = ifExists;
        this.dbName = dbName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP DATABASE");

        if (ifExists) {
            writer.keyword("IF EXISTS");
        }

        dbName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public SqlIdentifier getDbName() {
        return dbName;
    }

    public static class SqlDropDatabaseOperator extends SqlSpecialOperator {

        public SqlDropDatabaseOperator(){
            super("DROP_DATABASE", SqlKind.DROP_DATABASE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("DROP_DATABASE_RESULT",
                0,
                columnType)));
        }
    }
}
