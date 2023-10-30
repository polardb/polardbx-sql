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

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/create-role.html">Create Role</a>
 * @see MySqlCreateUserStatement
 * @since 2.1.12.9_drds_3
 */
public class MySqlCreateRoleStatement extends MySqlStatementImpl implements SQLCreateStatement {
    private final List<RoleSpec> roleSpecs = new ArrayList<RoleSpec>(2);
    private boolean ifNotExists = false;

    public List<RoleSpec> getRoleSpecs() {
        return roleSpecs;
    }

    public void addRoleSpec(RoleSpec roleSpec) {
        roleSpecs.add(roleSpec);
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, roleSpecs);
        }
        visitor.endVisit(this);
    }

    public static class RoleSpec extends MySqlObjectImpl {
        private MySqlUserName username;

        public MySqlUserName getUsername() {
            return username;
        }

        public void setUsername(MySqlUserName username) {
            username.verifyNoIdentify();
            this.username = username;
        }

        @Override
        public void accept0(MySqlASTVisitor visitor) {
            if (visitor.visit(this)) {
                acceptChild(visitor, username);
            }

            visitor.endVisit(this);
        }
    }
}
