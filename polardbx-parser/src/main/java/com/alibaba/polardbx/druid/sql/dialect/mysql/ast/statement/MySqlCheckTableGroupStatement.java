package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class MySqlCheckTableGroupStatement extends MySqlStatementImpl {
    private final List<SQLName> tableGroups = new ArrayList<SQLName>();
    private SQLOrderBy orderBy;
    private SQLExpr where;
    private SQLLimit limit;
    public MySqlCheckTableGroupStatement() {

    }

    public void addTableGroup(SQLName tableGroup) {
        if (tableGroup == null) {
            return;
        }
        tableGroups.add(tableGroup);
    }

    public List<SQLName> getTableGroups() {
        return tableGroups;
    }

    public SQLLimit getLimit() {
        return limit;
    }

    public void setLimit(SQLLimit x) {
        if (x != null) {
            x.setParent(this);
        }
        this.limit = x;
    }

    public SQLOrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(SQLOrderBy x) {
        if (x != null) {
            x.setParent(this);
        }
        this.orderBy = x;
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

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableGroups);
            acceptChild(visitor, where);
            acceptChild(visitor, orderBy);
            acceptChild(visitor, limit);
        }
        visitor.endVisit(this);
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.CHECK_TABLEGROUP;
    }
}
