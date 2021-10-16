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
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLPartitionRef extends SQLObjectImpl {
    private List<Item> items = new ArrayList<Item>();

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, items);
        }
        v.endVisit(this);
    }

    public List<Item> getItems() {
        return items;
    }

    public void addItem(Item item) {
        item.setParent(this);
        items.add(item);
    }

    public void addItem(SQLIdentifierExpr name, SQLExpr value) {
        Item item = new Item();
        item.setColumnName(name);
        item.setValue(value);
        item.setParent(this);
        this.items.add(item);
    }

    public static class Item extends SQLObjectImpl {

        private SQLIdentifierExpr columnName;
        private SQLExpr value;
        private SQLBinaryOperator operator;

        public Item() {

        }

        public Item(SQLIdentifierExpr columnName) {
            setColumnName(columnName);
        }

        @Override
        protected void accept0(SQLASTVisitor v) {

        }

        public SQLIdentifierExpr getColumnName() {
            return columnName;
        }

        public void setColumnName(SQLIdentifierExpr x) {
            if (x != null) {
                x.setParent(this);
            }
            this.columnName = x;
        }

        public SQLExpr getValue() {
            return value;
        }

        public void setValue(SQLExpr x) {
            if (x != null) {
                x.setParent(this);
            }
            this.value = x;
        }

        public SQLBinaryOperator getOperator() {
            return operator;
        }

        public void setOperator(SQLBinaryOperator operator) {
            this.operator = operator;
        }
    }
}
