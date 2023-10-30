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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author guxu
 */
public class DrdsCreateStoragePoolStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private SQLName name;
    private SQLExpr dnList;

    public SQLExpr getUndeletableDn() {
        return undeletableDn;
    }

    public void setUndeletableDn(SQLExpr undeletableDn) {
        this.undeletableDn = undeletableDn;
    }

    private SQLExpr undeletableDn;

    public DrdsCreateStoragePoolStatement() {

    }

    @Override
    public DrdsCreateStoragePoolStatement clone() {
        DrdsCreateStoragePoolStatement x = new DrdsCreateStoragePoolStatement();
        if (this.name != null) {
            x.name = this.name.clone();
            x.name.setParent(x);
        }
        if (this.dnList != null) {
            x.dnList = this.dnList.clone();
            x.dnList.setParent(x);
        }
        if (this.undeletableDn != null) {
            x.undeletableDn = this.undeletableDn.clone();
            x.undeletableDn.setParent(x);
        }
        return x;
    }

    public SQLName getName() {
        return this.name;
    }

    public void setName(final SQLName name) {
        this.name = name;
    }

    public SQLExpr getDnList() {
        return this.dnList;
    }

    public void setDnList(final SQLExpr dnList) {
        this.dnList = dnList;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}
