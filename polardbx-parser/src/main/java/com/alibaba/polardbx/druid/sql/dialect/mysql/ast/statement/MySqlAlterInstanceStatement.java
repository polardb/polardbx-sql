package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.statement.MySqlAlterInstanceItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnsupportedStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

public class MySqlAlterInstanceStatement extends MySqlStatementImpl implements SQLUnsupportedStatement {

    private MySqlAlterInstanceItem item;

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (!(visitor instanceof MySqlOutputVisitor)) {
            throw new UnsupportedOperationException();
        }

        MySqlOutputVisitor mySqlOutputVisitor = (MySqlOutputVisitor) visitor;
        if (mySqlOutputVisitor.visit(this)) {
            acceptChild(visitor, item);
        }
        mySqlOutputVisitor.endVisit(this);
    }

    public void setItem(MySqlAlterInstanceItem item) {
        this.item = item;
    }

    public MySqlAlterInstanceItem getItem() {
        return this.item;
    }

}
