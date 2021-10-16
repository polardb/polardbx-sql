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
 * @author chenmo.cm
 */
public class SqlShowProcesslist extends SqlShow {

    private SqlSpecialOperator operator;

    private boolean full;
    private boolean physical;

    public SqlShowProcesslist(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                              SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean full,
                              boolean physical) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.full = full;
        this.physical = physical;
    }

    public static SqlShowProcesslist create(SqlParserPos pos, boolean full, boolean physical, SqlNode like,
                                            SqlNode where, SqlNode orderBy, SqlNode limit) {
        List<SqlSpecialIdentifier> sqlSpecialIdentifiers = new LinkedList<>();
        if (full) {
            sqlSpecialIdentifiers.add(SqlSpecialIdentifier.FULL);
        }
        if (physical) {
            sqlSpecialIdentifiers.add(SqlSpecialIdentifier.PHYSICAL_PROCESSLIST);
        } else {
            sqlSpecialIdentifiers.add(SqlSpecialIdentifier.PROCESSLIST);
        }
        return new SqlShowProcesslist(pos, sqlSpecialIdentifiers, ImmutableList.<SqlNode>of(), like, where, orderBy,
            limit, full, physical);
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    public boolean isPhysical() {
        return physical;
    }

    public void setPhysical(boolean physical) {
        this.physical = physical;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowProcesslistOperator(this.full, this.physical);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_PROCESSLIST;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowProcesslistOperator extends SqlSpecialOperator {

        private boolean full;
        private boolean physical;

        public SqlShowProcesslistOperator(boolean full, boolean physical) {
            super("SHOW_PROCESSLIST", SqlKind.SHOW_PROCESSLIST);
            this.full = full;
            this.physical = physical;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            if (physical) {
                columns.add(new RelDataTypeFieldImpl("Group", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("Atom", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("Id", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("User", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("db", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Command", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Time", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("State", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Info", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            } else {
                columns.add(new RelDataTypeFieldImpl("Id", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("User", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Host", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("db", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Command", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Time", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
                columns.add(new RelDataTypeFieldImpl("State", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Info", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                if (full) {
                    columns.add(
                        new RelDataTypeFieldImpl("SQL_Template_Id", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                }
            }

            return typeFactory.createStructType(columns);
        }
    }
}
