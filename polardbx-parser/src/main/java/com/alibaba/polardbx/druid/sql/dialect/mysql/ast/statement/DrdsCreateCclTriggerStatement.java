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

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author busu
 * date: 2021/4/15 7:44 下午
 */
public class DrdsCreateCclTriggerStatement extends MySqlStatementImpl implements SQLCreateStatement {
    private boolean ifNotExists;
    private SQLName name;
    private SQLName schema;
    private List<SQLBinaryOpExpr> whens;
    private List<SQLAssignItem> limitAssignItems;
    private List<SQLAssignItem> withAssignItems;

    public DrdsCreateCclTriggerStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public List<SQLAssignItem> getLimitAssignItems() {
        return limitAssignItems;
    }

    public void setLimitAssignItems(List<SQLAssignItem> limitAssignItems) {
        this.limitAssignItems = limitAssignItems;
    }

    public List<SQLAssignItem> getWithAssignItems() {
        return withAssignItems;
    }

    public void setWithAssignItems(List<SQLAssignItem> withAssignItems) {
        this.withAssignItems = withAssignItems;
    }

    public List<SQLBinaryOpExpr> getWhens() {
        return whens;
    }

    public void setWhens(List<SQLBinaryOpExpr> whens) {
        this.whens = whens;
    }

    public SQLName getSchema() {
        return schema;
    }

    public void setSchema(SQLName schema) {
        this.schema = schema;
    }
}
