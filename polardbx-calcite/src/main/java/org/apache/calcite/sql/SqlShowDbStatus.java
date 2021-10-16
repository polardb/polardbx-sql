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

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/13 上午9:58
 */
public class SqlShowDbStatus extends SqlShow {

    private SqlSpecialOperator operator;

    private boolean full;

    public SqlShowDbStatus(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                           SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean full){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.full = full;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowDbStatusOperator(this.full);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_DB_STATUS;
    }

    public static class SqlShowDbStatusOperator extends SqlSpecialOperator {

        private boolean full;

        public SqlShowDbStatusOperator(boolean full){
            super("SHOW_DB_STATUS", SqlKind.SHOW_DB_STATUS);
            this.full = full;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("CONNECTION_STRING", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PHYSICAL_DB", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (full) {
                columns.add(new RelDataTypeFieldImpl("PHYSICAL_TABLE",
                    4,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }
            columns.add(new RelDataTypeFieldImpl("SIZE_IN_MB", 5, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
            columns.add(new RelDataTypeFieldImpl("RATIO", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("THREAD_RUNNING", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
