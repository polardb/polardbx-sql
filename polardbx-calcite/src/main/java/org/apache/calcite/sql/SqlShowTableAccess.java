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
public class SqlShowTableAccess extends SqlShow {

    private SqlSpecialOperator operator = new SqlShowTableAccessOperator();

    private boolean full;
    private boolean physical;

    public SqlShowTableAccess(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                              SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean full) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.full = full;
    }

    public static SqlShowTableAccess create(SqlParserPos pos, boolean full, boolean physical, SqlNode like,
                                            SqlNode where, SqlNode orderBy, SqlNode limit) {
        List<SqlSpecialIdentifier> sqlSpecialIdentifiers = new LinkedList<>();
        if (full) {
            sqlSpecialIdentifiers.add(SqlSpecialIdentifier.FULL);
        }
        return new SqlShowTableAccess(pos, sqlSpecialIdentifiers, ImmutableList.<SqlNode>of(), like, where, orderBy,
            limit, full);
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
            operator = new SqlShowTableAccessOperator();
        }

        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_TABLE_ACCESS;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowTableAccessOperator extends SqlSpecialOperator {

        public SqlShowTableAccessOperator() {
            super("SHOW_TABLE_ACCESS", SqlKind.SHOW_TABLE_ACCESS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("TABLE_SCHEMA", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("TABLE_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("TABLE_TYPE", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("ACCESS_COUNT", 3, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            return typeFactory.createStructType(columns);
        }
    }
}
