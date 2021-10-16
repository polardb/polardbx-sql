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

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetRoleStatement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Set role statement.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html">Set role</a>
 * @see MySqlSetRoleStatement
 * @since 5.4.9
 */
public class SqlSetRole extends SqlDal {
    private static final SqlSetRoleOperator OPERATOR = new SqlSetRoleOperator();

    private final MySqlSetRoleStatement.RoleSpec roleSpec;
    private final List<SqlUserName> users;

    public SqlSetRole(SqlParserPos pos,
                      MySqlSetRoleStatement.RoleSpec roleSpec, List<SqlUserName> users) {
        super(pos);
        this.roleSpec = roleSpec;
        this.users = users;

        this.operands = new ArrayList<>(1 + users.size());
        this.operands.addAll(users);
    }

    public MySqlSetRoleStatement.RoleSpec getRoleSpec() {
        return roleSpec;
    }

    public List<SqlUserName> getUsers() {
        return Collections.unmodifiableList(users);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SQL_SET_ROLE;
    }

    public static class SqlSetRoleOperator extends SqlSpecialOperator {
        public SqlSetRoleOperator() {
            super(SqlKind.SQL_SET_ROLE.name(), SqlKind.SQL_SET_ROLE);
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
