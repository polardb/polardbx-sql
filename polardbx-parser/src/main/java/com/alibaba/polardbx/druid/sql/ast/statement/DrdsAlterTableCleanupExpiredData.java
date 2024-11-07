package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class DrdsAlterTableCleanupExpiredData extends SQLObjectImpl implements SQLAlterTableItem {

    public DrdsAlterTableCleanupExpiredData() {
        super();
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}