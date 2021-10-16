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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Sql node for show grants.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/show-grants.html">Show grants</a>
 * @since 5.4.9
 */
public class SqlShowGrants extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowGrantsOperator();

    /**
     * <code>User's</code> privileges to show.
     * <p>
     * Empty for current user.
     */
    private final Optional<SqlUserName> user;

    /**
     * Roles to use.
     */
    private final List<SqlUserName> roles;

    public SqlShowGrants(SqlParserPos pos, Optional<SqlUserName> user, List<SqlUserName> roles) {
        super(pos, getSpecialIdentifiers(user.isPresent()), operandsOf(user, roles), null, null, null, null);
        this.user = user;
        this.roles = roles;
    }

    private static List<SqlSpecialIdentifier> getSpecialIdentifiers(boolean hasUser) {
        List<SqlSpecialIdentifier> identifiers = new ArrayList<>(2);
        identifiers.add(SqlSpecialIdentifier.GRANTS);
        if (hasUser) {
            identifiers.add(SqlSpecialIdentifier.FOR);
        }
        return identifiers;
    }

    private static List<SqlNode> operandsOf(Optional<SqlUserName> user, List<SqlUserName> roles) {
        List<SqlNode> nodes = new ArrayList<>(roles.size() + 1);
        user.ifPresent(nodes::add);
        nodes.addAll(roles);
        return nodes;
    }

    public boolean isCurrentUser() {
        return !user.isPresent();
    }

    public Optional<SqlUserName> getUser() {
        return user;
    }

    public List<SqlUserName> getRoles() {
        return Collections.unmodifiableList(roles);
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
        return SqlKind.SHOW_GRANTS;
    }

    public static class SqlShowGrantsOperator extends SqlSpecialOperator {

        public SqlShowGrantsOperator(){
            super("SHOW_GRANTS", SqlKind.SHOW_GRANTS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("User", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Host", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Grant statement", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
