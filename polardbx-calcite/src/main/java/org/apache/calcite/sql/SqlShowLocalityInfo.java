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
 * @author jinkun.taojinkun
 */
public class SqlShowLocalityInfo extends SqlShow {

    private SqlSpecialOperator operator;

    private boolean full;

    private String schema;

    public SqlShowLocalityInfo(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers,
                               List<SqlNode> operands, SqlNode like, SqlNode where, SqlNode orderBy,
                               SqlNode limit, String schema) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit,
            specialIdentifiers.size() + operands.size() - 1);
        this.schema = schema;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowTableInfoOperator(full);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_LOCALITY_INFO;
    }

    public static class SqlShowTableInfoOperator extends SqlSpecialOperator {
        private boolean full;

        public SqlShowTableInfoOperator(boolean full) {
            super("SHOW_LOCALITY_INIFO", SqlKind.SHOW_LOCALITY_INFO);
            this.full = full;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();

            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("GROUP_NAME", 1, typeFactory.createSqlType(SqlTypeName.CHAR)));
            columns.add(new RelDataTypeFieldImpl("TABLE_NAME", 2, typeFactory.createSqlType(SqlTypeName.CHAR)));
            columns.add(new RelDataTypeFieldImpl("SIZE_IN_MB", 3, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

            return typeFactory.createStructType(columns);
        }
    }
}
