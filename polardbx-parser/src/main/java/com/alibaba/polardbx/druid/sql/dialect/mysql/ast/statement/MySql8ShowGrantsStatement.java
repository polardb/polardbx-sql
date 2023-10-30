/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/create-role.html">Create Role</a>
 * @since 2.1.12.9_drds_3
 */
public class MySql8ShowGrantsStatement extends MySqlShowGrantsStatement implements MySqlShowStatement {
    public enum UserSpec {
        /**
         * Case: <code>SHOW GRANTS;</code>
         */
        NONE,

        /**
         * Case: <code>SHOW GRANTS FOR CURRENT_USER;</code>
         */
        CURRENT_USER,

        /**
         * Case: <code>SHOW GRANTS FOR CURRENT_USER();</code>
         */
        CURRENT_USER_FUNC,

        /**
         * Case: SHOW GRANTS FOR 'a';
         */
        USERNAME
    }

    private UserSpec userSpec = UserSpec.NONE;
    private MySqlUserName username;

    private final List<MySqlUserName> rolesToUse = new ArrayList<MySqlUserName>(2);

    public MySql8ShowGrantsStatement() {
        dbType = DbType.mysql;
    }

    public UserSpec getUserSpec() {
        return userSpec;
    }

    public void setUserSpec(UserSpec userSpec) {
        this.userSpec = userSpec;
    }

    public MySqlUserName getUsername() {
        return username;
    }

    public void setUsername(MySqlUserName username) {
        username.verifyNoIdentify();
        this.username = username;
    }

    public void addRoleToUse(MySqlUserName mysqlUsername) {
        mysqlUsername.verifyNoIdentify();
        rolesToUse.add(mysqlUsername);
    }

    public List<MySqlUserName> getRolesToUse() {
        return Collections.unmodifiableList(rolesToUse);
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, user);
        }
        visitor.endVisit(this);
    }
}
