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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class SqlShowTrace extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowTraceOperator();

    public SqlShowTrace(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                        SqlNodeList selectList,
                        SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        super(pos, specialIdentifiers, operands, selectList, like, where, orderBy, limit, Collections.emptyList(),
            Collections.emptyList(), Collections.emptyList());
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_TRACE;
    }

    @Override
    public boolean canConvertToSelect() {
        return true;
    }

    @Override
    public SqlSelect convertToSelect() {
        return doConvertToSelect();
    }

    public static class SqlShowTraceOperator extends SqlSpecialOperator {

        public SqlShowTraceOperator() {
            super("SHOW_TRACE", SqlKind.SHOW_TRACE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("NODE_IP", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("TIMESTAMP", 2, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
            columns.add(new RelDataTypeFieldImpl("TYPE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("GROUP_NAME", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("DBKEY_NAME", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("TIME_COST(ms)", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("CONNECTION_TIME_COST(ms)",
                7,
                typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("ROWS", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("STATEMENT", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PARAMS", 10, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
