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
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lijun.cailj 2018/8/13
 */
public class MysqlCreateFullTextCharFilterStatement extends MySqlStatementImpl {

    private SQLName name; // for all, not null
    private SQLTextLiteralExpr typeName;  // for charfilter/tokenizer/tokenfilter

    protected final List<SQLAssignItem> options = new ArrayList<SQLAssignItem>(); // charfilter/tokenizer/tokenfilter

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
            acceptChild(visitor, typeName);
            acceptChild(visitor, options);
        }
        visitor.endVisit(this);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            name.setParent(this);
        }
        this.name = name;
    }

    public SQLTextLiteralExpr getTypeName() {
        return typeName;
    }

    public void setTypeName(SQLTextLiteralExpr typeName) {
        if (name != null) {
            name.setParent(this);
        }
        this.typeName = typeName;
    }

    public List<SQLAssignItem> getOptions() {
        return options;
    }

    public void addOption(String name, SQLExpr value) {
        SQLAssignItem assignItem = new SQLAssignItem(new SQLIdentifierExpr(name), value);
        assignItem.setParent(this);
        options.add(assignItem);
    }

    public SQLExpr getOption(String name) {
        if (name == null) {
            return null;
        }

        long hash64 = FnvHash.hashCode64(name);

        for (SQLAssignItem item : options) {
            final SQLExpr target = item.getTarget();
            if (target instanceof SQLIdentifierExpr) {
                if (((SQLIdentifierExpr) target).hashCode64() == hash64) {
                    return item.getValue();
                }
            }
        }

        return null;
    }

    public Object getOptionValue(String name) {
        SQLExpr option = getOption(name);
        if (option instanceof SQLValuableExpr) {
            return ((SQLValuableExpr) option).getValue();
        }

        return null;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
