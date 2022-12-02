package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * Created by jinkun.taojinkun.
 *
 * @author jinkun.taojinkun
 */
public class DrdsShowLocality extends MySqlStatementImpl implements MySqlShowStatement {
    private SQLOrderBy orderBy;
    private SQLExpr where;
    private SQLLimit limit;
    private SQLName name;
    private boolean full = false;

    public SQLLimit getLimit() {
        return limit;
    }

    public void setLimit(SQLLimit limit) {
        this.limit = limit;
    }

    public SQLOrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(SQLOrderBy orderBy) {
        this.orderBy = orderBy;
    }

    public SQLExpr getWhere() {
        return where;
    }

    public void setWhere(SQLExpr where) {
        this.where = where;
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, where);
            acceptChild(visitor, orderBy);
            acceptChild(visitor, limit);
        }
        visitor.endVisit(this);
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }
}
