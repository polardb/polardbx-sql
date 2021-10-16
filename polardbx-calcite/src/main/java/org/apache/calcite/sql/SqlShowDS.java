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
 * @author chenmo.cm
 */
public class SqlShowDS extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowDSOperator();

    public SqlShowDS(SqlParserPos pos,
                     List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                     SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
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
        return SqlKind.SHOW_DS;
    }

    public static class SqlShowDSOperator extends SqlSpecialOperator {

        public SqlShowDSOperator() {
            super("SHOW_DS", SqlKind.SHOW_DS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(
                new RelDataTypeFieldImpl("STORAGE_INST_ID", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("DB", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("GROUP", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PHY_DB", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("MOVABLE", 5, typeFactory.createSqlType(SqlTypeName.INTEGER)));

            return typeFactory.createStructType(columns);
        }
    }
}
