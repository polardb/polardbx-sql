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

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.google.common.base.Objects;

import java.util.List;

/**
 * @author guxu
 */
public class DrdsCreateScheduleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private SQLName name;
    private SQLExpr cronExpr;
    private SQLExpr timeZone;

    private boolean ifNotExists;
    private boolean forLocalPartition;

    public DrdsCreateScheduleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.name != null) {
                this.name.accept(visitor);
            }
            if (this.cronExpr != null) {
                this.cronExpr.accept(visitor);
            }
            if (this.timeZone != null) {
                this.timeZone.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsCreateScheduleStatement clone() {
        DrdsCreateScheduleStatement x = new DrdsCreateScheduleStatement();
        if (this.name != null) {
            x.name = this.name.clone();
            x.name.setParent(x);
        }
        if (this.cronExpr != null) {
            x.cronExpr = this.cronExpr.clone();
            x.cronExpr.setParent(x);
        }
        if (this.timeZone != null) {
            x.timeZone = this.timeZone.clone();
            x.timeZone.setParent(x);
        }
        x.setIfNotExists(this.isIfNotExists());
        x.setForLocalPartition(this.isForLocalPartition());
        return x;
    }

    public SQLName getName() {
        return this.name;
    }

    public void setName(final SQLName name) {
        this.name = name;
    }

    public SQLExpr getCronExpr() {
        return this.cronExpr;
    }

    public void setCronExpr(final SQLExpr cronExpr) {
        this.cronExpr = cronExpr;
    }

    public SQLExpr getTimeZone() {
        return this.timeZone;
    }

    public void setTimeZone(final SQLExpr timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isIfNotExists() {
        return this.ifNotExists;
    }

    public void setIfNotExists(final boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isForLocalPartition() {
        return this.forLocalPartition;
    }

    public void setForLocalPartition(final boolean forLocalPartition) {
        this.forLocalPartition = forLocalPartition;
    }
}
