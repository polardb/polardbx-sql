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

/**
 * @author moyi
 */
public class SqlShowCreateDatabase extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowCreateDatabaseOperator();
    private SqlNode databaseName;
    private boolean ifNotExists = false;

    public SqlShowCreateDatabase(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers,
                                 List<SqlNode> operands,
                                 SqlNode databaseName, boolean ifNotExists) {
        super(pos, specialIdentifiers, operands, null, null, null, null,
            specialIdentifiers.size() + operands.size() - 1);
        this.databaseName = databaseName;
        this.ifNotExists = ifNotExists;
    }

    public static SqlShowCreateDatabase create(SqlParserPos pos, SqlNode databaseName, boolean ifNotExists) {
        return new SqlShowCreateDatabase(pos,
            ImmutableList.of(SqlSpecialIdentifier.CREATE, SqlSpecialIdentifier.DATABASE),
            ImmutableList.of(databaseName),
            databaseName, ifNotExists);
    }

    @Override
    public SqlNode getDbName() {
        return databaseName;
    }

    public boolean isIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_CREATE_DATABASE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW CREATE DATABASE");
        if (this.isIfNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }
        this.databaseName.unparse(writer, leftPrec, rightPrec);
    }

    public static class SqlShowCreateDatabaseOperator extends SqlSpecialOperator {

        public SqlShowCreateDatabaseOperator() {
            super("SHOW_CREATE_DATABASE", SqlKind.SHOW_CREATE_DATABASE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Database", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Create Database", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

}
