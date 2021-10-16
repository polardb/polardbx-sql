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

package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLCreateOutlineStatement extends SQLStatementImpl {
    private SQLName name;
    private SQLExpr where;

    private SQLStatement on;
    private SQLStatement to;

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.name = x;
    }

    public SQLStatement getOn() {
        return on;
    }

    public void setOn(SQLStatement x) {
        if (x != null) {
            x.setParent(this);
        }
        this.on = x;
    }

    public SQLStatement getTo() {
        return to;
    }

    public void setTo(SQLStatement x) {
        if (x != null) {
            x.setParent(this);
        }
        this.to = x;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, name);
            acceptChild(v, where);
            acceptChild(v, on);
            acceptChild(v, to);
        }
        v.endVisit(this);
    }

    public SQLExpr getWhere() {
        return where;
    }

    public void setWhere(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.where = x;
    }
}
