package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author guxu
 */
public class DrdsDropScheduleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private boolean ifExist;

    private Long scheduleId;

    public DrdsDropScheduleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsDropScheduleStatement clone() {
        DrdsDropScheduleStatement x = new DrdsDropScheduleStatement();
        x.ifExist = this.ifExist;
        if (this.scheduleId != null) {
            x.setScheduleId(this.scheduleId);
        }
        return x;
    }

    public boolean isIfExist() {
        return this.ifExist;
    }

    public void setIfExist(final boolean ifExist) {
        this.ifExist = ifExist;
    }

    public Long getScheduleId() {
        return this.scheduleId;
    }

    public void setScheduleId(final Long scheduleId) {
        this.scheduleId = scheduleId;
    }
}
