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
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlShowMoveDatabase extends SqlShow {

    private SqlShowMoveDatabaseOperator operator;

    public SqlShowMoveDatabase(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers) {
        super(pos, specialIdentifiers);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowMoveDatabaseOperator();
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_MOVE_DATABASE;
    }

    public static class SqlShowMoveDatabaseOperator extends SqlSpecialOperator {

        public SqlShowMoveDatabaseOperator() {
            super("SHOW_MOVE_DATABASE", SqlKind.SHOW_MOVE_DATABASE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("SCHEMA", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns
                .add(new RelDataTypeFieldImpl("LAST_UPDATE_TIME", 1, typeFactory.createSqlType(SqlTypeName.DATETIME)));
            columns.add(
                new RelDataTypeFieldImpl("SOURCE_DB_GROUP_KEY", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(
                new RelDataTypeFieldImpl("TARGET_STORAGE_INST_ID", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("STATUS", 4, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("JOB_STATUS", 5, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("BATCH_ID", 6, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            return typeFactory.createStructType(columns);
        }
    }
}