package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class MySqlShowBinlogDumpStatusStatement extends MySqlStatementImpl implements MySqlShowStatement {
    private SQLExpr with;

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, with);
        }
        visitor.endVisit(this);
    }

    public SQLExpr getWith() {
        return with;
    }

    public void setWith(SQLExpr with) {
        this.with = with;
    }
}
