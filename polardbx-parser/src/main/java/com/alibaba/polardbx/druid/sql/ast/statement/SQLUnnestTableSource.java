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
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLUnnestTableSource extends SQLTableSourceImpl {
    private final List<SQLExpr> items = new ArrayList<SQLExpr>();
    protected List<SQLName> columns = new ArrayList<SQLName>();
    private boolean ordinality = false;

    public SQLUnnestTableSource() {

    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, items);
            acceptChild(v, columns);
        }
        v.endVisit(this);
    }

    public List<SQLName> getColumns() {
        return columns;
    }

    public void addColumn(SQLName column) {
        column.setParent(this);
        this.columns.add(column);
    }

    public boolean isOrdinality() {
        return ordinality;
    }

    public void setOrdinality(boolean ordinality) {
        this.ordinality = ordinality;
    }

    public List<SQLExpr> getItems() {
        return items;
    }

    public void addItem(SQLExpr item) {
        item.setParent(this);
        this.items.add(item);
    }

    public SQLUnnestTableSource clone() {
        SQLUnnestTableSource x = new SQLUnnestTableSource();

        for (SQLExpr item : items) {
            SQLExpr item2 = item.clone();
            item2.setParent(x);
            x.items.add(item2);
        }

        for (SQLName column : columns) {
            SQLName c2 = column.clone();
            c2.setParent(x);
            x.columns.add(c2);
        }

        x.alias = alias;

        return x;
    }
}
