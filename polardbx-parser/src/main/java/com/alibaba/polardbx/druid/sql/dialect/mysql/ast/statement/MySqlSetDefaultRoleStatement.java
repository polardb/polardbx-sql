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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A statement like this:
 * <p>
 * <code>
 * SET DEFAULT ROLE a, b TO c, d;
 * </code>
 * <p>
 * Set users c, d's default roles to a, b.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-default-role.html">Set default role</a>
 * @since 2.1.12.9_drds_3
 */
public class MySqlSetDefaultRoleStatement extends MySqlStatementImpl {
    public enum DefaultRoleSpec {
        /**
         * For statements like:
         * <code>
         * SET DEFAULT ROLE NONE TO a;
         * </code>
         */
        NONE,
        /**
         * For statements like:
         * <code>
         * SET DEFAULT ROLE ALL TO a;
         * </code>
         */
        ALL,
        /**
         * For statements like:
         * <code>
         * SET DEFAULT ROLE c TO a;
         * </code>
         */
        ROLES
    }

    private DefaultRoleSpec defaultRoleSpec = DefaultRoleSpec.NONE;
    private final List<MySqlUserName> roles = new ArrayList<MySqlUserName>();
    private final List<MySqlUserName> users = new ArrayList<MySqlUserName>();

    public void setDefaultRoleSpec(DefaultRoleSpec defaultRoleSpec) {
        this.defaultRoleSpec = defaultRoleSpec;
    }

    public void addRole(MySqlUserName role) {
        role.verifyNoIdentify();
        roles.add(role);
    }

    public void addUser(MySqlUserName user) {
        user.verifyNoIdentify();
        users.add(user);
    }

    public DefaultRoleSpec getDefaultRoleSpec() {
        return defaultRoleSpec;
    }

    public List<MySqlUserName> getRoles() {
        return Collections.unmodifiableList(roles);
    }

    public List<MySqlUserName> getUsers() {
        return Collections.unmodifiableList(users);
    }

    @Override
    public void accept0(MySqlASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, roles);
        }

        v.endVisit(this);
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
