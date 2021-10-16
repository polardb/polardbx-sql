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

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/grant.html#grant-roles">Grant Role</a>
 * @since 2.1.12.9_drds_3
 */
public class MySqlGrantRoleStatement extends MySqlStatementImpl {
    private final List<MySqlUserName> sourceAccounts = new ArrayList<MySqlUserName>(2);
    private final List<MySqlUserName> destAccounts = new ArrayList<MySqlUserName>(2);
    private boolean withAdminOption = false;

    public List<MySqlUserName> getSourceAccounts() {
        return Collections.unmodifiableList(sourceAccounts);
    }

    public List<MySqlUserName> getDestAccounts() {
        return Collections.unmodifiableList(destAccounts);
    }

    public void addSourceAccount(MySqlUserName sourceAccount) {
        sourceAccount.verifyNoIdentify();
        sourceAccounts.add(sourceAccount);
    }

    public void addDestAccount(MySqlUserName destAccount) {
        destAccount.verifyNoIdentify();
        destAccounts.add(destAccount);
    }

    public boolean isWithAdminOption() {
        return withAdminOption;
    }

    public void setWithAdminOption(boolean withAdminOption) {
        this.withAdminOption = withAdminOption;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, sourceAccounts);
            acceptChild(visitor, destAccounts);
        }

        visitor.endVisit(this);
    }
}
