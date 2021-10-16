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
 * @author busu
 */
public class DrdsCreateCclRuleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private SQLExprTableSource table;

    private SQLIdentifierExpr FOR;

    private MySqlUserName user;

    private SQLListExpr keywords;

    private SQLListExpr template;

    private SQLCharExpr query;

    private List<SQLAssignItem> with;

    private SQLName name;

    private boolean ifNotExists;

    public DrdsCreateCclRuleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.name != null) {
                this.name.accept(visitor);
            }
            if (this.table != null) {
                this.table.accept(visitor);
            }
            if (this.FOR != null) {
                this.FOR.accept(visitor);
            }
            if (this.user != null) {
                this.user.accept(visitor);
            }
            if (this.keywords != null) {
                this.keywords.accept(visitor);
            }
            if (this.template != null) {
                this.template.accept(visitor);
            }
            if (this.query != null) {
                this.query.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsCreateCclRuleStatement clone() {
        DrdsCreateCclRuleStatement x = new DrdsCreateCclRuleStatement();
        if (this.name != null) {
            x.name = this.name.clone();
            x.name.setParent(x);
        }
        if (this.table != null) {
            x.table = this.table.clone();
            x.table.setParent(x);
        }
        if (this.FOR != null) {
            x.FOR = this.FOR.clone();
            x.FOR.setParent(x);
        }
        if (this.user != null) {
            x.user = this.user.clone();
            x.user.setParent(x);
        }
        if (this.keywords != null) {
            x.keywords = this.keywords.clone();
        }
        if (this.template != null) {
            x.template = this.template.clone();
            x.template.setParent(x);
        }
        if (this.query != null) {
            x.query = this.query.clone();
            x.query.setParent(x);
        }

        return x;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DrdsCreateCclRuleStatement that = (DrdsCreateCclRuleStatement) o;
        return Objects.equal(this.table, that.table) && Objects.equal(this.FOR, that.FOR) && Objects
            .equal(this.user, that.user) && Objects.equal(this.keywords, that.keywords) && Objects
            .equal(this.template, that.template) && Objects.equal(this.name, that.name) && Objects
            .equal(this.query, that.query);
    }

    @Override
    public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (FOR != null ? FOR.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (keywords != null ? keywords.hashCode() : 0);
        result = 31 * result + (template != null ? template.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (query != null ? query.hashCode() : 0);
        return result;
    }

    public SQLExprTableSource getTable() {
        return table;
    }

    public void setTable(SQLExprTableSource table) {
        this.table = table;
    }

    public SQLIdentifierExpr getFOR() {
        return FOR;
    }

    public void setFOR(SQLIdentifierExpr FOR) {
        this.FOR = FOR;
    }

    public MySqlUserName getUser() {
        return user;
    }

    public void setUser(MySqlUserName user) {
        this.user = user;
    }

    public SQLListExpr getKeywords() {
        return keywords;
    }

    public void setKeywords(SQLListExpr keywords) {
        this.keywords = keywords;
    }

    public SQLListExpr getTemplate() {
        return template;
    }

    public void setTemplate(SQLListExpr template) {
        this.template = template;
    }

    public List<SQLAssignItem> getWith() {
        return with;
    }

    public void setWith(List<SQLAssignItem> with) {
        this.with = with;
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public SQLCharExpr getQuery() {
        return query;
    }

    public void setQuery(SQLCharExpr query) {
        this.query = query;
    }

}
