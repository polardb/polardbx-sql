package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class DrdsCheckColumnarSnapshot extends MySqlStatementImpl implements SQLShowStatement {

    private SQLName tableName = null;

    @Override
    public void accept0(MySqlASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public SQLName getTableName() {
        return tableName;
    }

    public void setTableName(SQLName tableName) {
        tableName.setParent(this);
        this.tableName = tableName;
    }
}
