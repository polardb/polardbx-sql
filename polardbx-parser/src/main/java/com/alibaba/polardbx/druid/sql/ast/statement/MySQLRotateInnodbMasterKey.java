package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

public class MySQLRotateInnodbMasterKey extends MySqlObjectImpl implements MySqlAlterInstanceItem {

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (!(visitor instanceof MySqlOutputVisitor)) {
            throw new UnsupportedOperationException();
        }

        MySqlOutputVisitor mySqlOutputVisitor = (MySqlOutputVisitor) visitor;
        mySqlOutputVisitor.visit(this);
        mySqlOutputVisitor.endVisit(this);
    }

}
