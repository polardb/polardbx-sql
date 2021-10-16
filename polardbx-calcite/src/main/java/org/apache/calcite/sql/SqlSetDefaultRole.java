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

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetDefaultRoleStatement;
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
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Set default role statement.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-default-role.html">Set default role</a>
 * @see MySqlSetDefaultRoleStatement
 * @since 5.4.9
 */
public class SqlSetDefaultRole extends SqlDal {
    private static final SqlOperator OPERATOR = new SqlSetDefaultRoleOperator();

    private final MySqlSetDefaultRoleStatement.DefaultRoleSpec defaultRoleSpec;
    private final List<SqlUserName> roles;
    private final List<SqlUserName> users;

    public SqlSetDefaultRole(SqlParserPos pos,
                             MySqlSetDefaultRoleStatement.DefaultRoleSpec defaultRoleSpec,
                             List<SqlUserName> roles, List<SqlUserName> users) {
        super(pos);
        this.defaultRoleSpec = defaultRoleSpec;
        this.roles = roles;
        this.users = users;

        this.operands = new ArrayList<>(roles.size() + users.size());
        this.operands.addAll(roles);
        this.operands.addAll(users);
    }

    public MySqlSetDefaultRoleStatement.DefaultRoleSpec getDefaultRoleSpec() {
        return defaultRoleSpec;
    }

    public List<SqlUserName> getRoles() {
        return Collections.unmodifiableList(roles);
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
        return SqlKind.SQL_SET_DEFAULT_ROLE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.print("SET DEFAULT ROLE ");

        switch (defaultRoleSpec) {
        case NONE:
            writer.print("NONE ");
            break;
        case ALL:
            writer.print("ALL ");
            break;
        case ROLES:
            writer.print(roles.stream().map(SqlUserName::toString).collect(Collectors.joining(", ")));
            writer.print(" ");
            break;
        default:
            throw new IllegalArgumentException("Unrecognized default role spec: " + defaultRoleSpec);
        }

        writer.print("TO ");
        writer.print(users.stream().map(SqlUserName::toString).collect(Collectors.joining(", ")));

        writer.endList(selectFrame);
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlSetDefaultRole(pos, defaultRoleSpec,
            Optional.ofNullable(roles)
                .map(ArrayList::new)
                .map(t -> (List<SqlUserName>) t)
                .orElse(Collections.emptyList()),
            Optional.ofNullable(users)
                .map(ArrayList::new)
                .map(t -> (List<SqlUserName>) t)
                .orElse(Collections.emptyList()));
    }

    public static class SqlSetDefaultRoleOperator extends SqlSpecialOperator {
        public SqlSetDefaultRoleOperator() {
            super(SqlKind.SQL_SET_DEFAULT_ROLE.name(), SqlKind.SQL_SET_DEFAULT_ROLE);
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
