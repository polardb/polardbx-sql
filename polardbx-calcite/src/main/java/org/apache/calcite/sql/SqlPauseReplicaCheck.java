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
 * @author yudong
 * @since 2023/11/9 11:10
 **/
public class SqlPauseReplicaCheck extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlPauseReplicaCheckOperator();

    private SqlNode dbName;
    private SqlNode tableName;

    public SqlPauseReplicaCheck(SqlParserPos pos, SqlNode dbName) {
        super(pos);
        this.dbName = dbName;
    }

    public SqlPauseReplicaCheck(SqlParserPos pos, SqlNode dbName, SqlNode tableName) {
        super(pos);
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public SqlNode getDbName() {
        return dbName;
    }

    public void setDbName(SqlNode dbName) {
        this.dbName = dbName;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public void setTableName(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CHECK REPLICA TABLE");
        dbName.unparse(writer, 0, 0);
        if (tableName != null) {
            writer.print(".");
            tableName.unparse(writer, 0, 0);
        }
        writer.keyword("PAUSE");
    }

    public static class SqlPauseReplicaCheckOperator extends SqlSpecialOperator {

        public SqlPauseReplicaCheckOperator() {
            super("PAUSE_REPLICA_CHECK", SqlKind.PAUSE_REPLICA_CHECK);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            return typeFactory.createStructType(columns);
        }
    }
}
