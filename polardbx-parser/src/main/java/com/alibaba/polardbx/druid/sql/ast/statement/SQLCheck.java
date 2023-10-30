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
package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLReplaceable;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLCheck extends SQLConstraintImpl implements SQLTableElement, SQLTableConstraint, SQLReplaceable {

    private SQLExpr expr;
    private boolean withEnforced;
    private boolean enforced;

    public SQLCheck() {

    }

    public SQLCheck(SQLExpr expr) {
        this.setExpr(expr);
    }

    public SQLExpr getExpr() {
        return expr;
    }

    public void setExpr(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.expr = x;
    }

    public boolean isWithEnforced() {
        return withEnforced;
    }

    public void setWithEnforced(boolean withEnforced) {
        this.withEnforced = withEnforced;
    }

    public boolean isEnforced() {
        return enforced;
    }

    public void setEnforced(boolean enforced) {
        this.enforced = enforced;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (getName() != null) {
                getName().accept(visitor);
            }

            if (expr != null) {
                expr.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public void cloneTo(SQLCheck x) {
        super.cloneTo(x);

        if (expr != null) {
            expr = expr.clone();
        }
    }

    public SQLCheck clone() {
        SQLCheck x = new SQLCheck();
        cloneTo(x);
        return x;
    }

    @Override
    public boolean replace(SQLExpr expr, SQLExpr target) {
        if (this.expr == expr) {
            setExpr(target);
            return true;
        }

        if (getName() == expr) {
            setName((SQLName) target);
            return true;
        }

        if (getComment() == expr) {
            setComment(target);
            return true;
        }
        return false;
    }
}
