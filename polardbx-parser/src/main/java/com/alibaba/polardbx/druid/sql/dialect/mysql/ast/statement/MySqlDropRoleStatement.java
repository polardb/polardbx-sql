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
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement.RoleSpec;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Drop role statement.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/drop-role.html">Drop Role</a>
 * @see RoleSpec
 * @since 2.1.12.9_drds_3
 */
public class MySqlDropRoleStatement extends MySqlStatementImpl implements SQLCreateStatement {
    private List<RoleSpec> roleSpecs = new ArrayList<RoleSpec>(2);

    public List<RoleSpec> getRoleSpecs() {
        return roleSpecs;
    }

    public void addRoleSpec(RoleSpec roleSpec) {
        roleSpecs.add(roleSpec);
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, roleSpecs);
        }
        visitor.endVisit(this);
    }
}
