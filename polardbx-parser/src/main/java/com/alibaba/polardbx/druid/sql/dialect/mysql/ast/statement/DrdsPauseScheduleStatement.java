package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author guxu
 */
public class DrdsPauseScheduleStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private boolean ifExist;

    private Long scheduleId;

    private boolean forLocalPartition;

    public DrdsPauseScheduleStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsPauseScheduleStatement clone() {
        DrdsPauseScheduleStatement x = new DrdsPauseScheduleStatement();
        x.ifExist = this.ifExist;
        x.forLocalPartition = this.forLocalPartition;
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

    public boolean isForLocalPartition() {
        return forLocalPartition;
    }

    public void setForLocalPartition(boolean forLocalPartition) {
        this.forLocalPartition = forLocalPartition;
    }

    public Long getScheduleId() {
        return this.scheduleId;
    }

    public void setScheduleId(final Long scheduleId) {
        this.scheduleId = scheduleId;
    }
}
