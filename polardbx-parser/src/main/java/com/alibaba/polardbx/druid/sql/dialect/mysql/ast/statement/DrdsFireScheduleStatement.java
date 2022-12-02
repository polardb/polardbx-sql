package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class DrdsFireScheduleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private Long scheduleId;

    public DrdsFireScheduleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsFireScheduleStatement clone() {
        DrdsFireScheduleStatement x = new DrdsFireScheduleStatement();
        if (this.scheduleId != null) {
            x.setScheduleId(this.scheduleId);
        }
        return x;
    }

    public Long getScheduleId() {
        return this.scheduleId;
    }

    public void setScheduleId(final Long scheduleId) {
        this.scheduleId = scheduleId;
    }
}