package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author guxu
 */
public class DrdsShowScheduleResultStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private Long scheduleId;

    public DrdsShowScheduleResultStatement() {

    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
        }
        visitor.endVisit(this);
    }

    @Override
    public DrdsShowScheduleResultStatement clone() {
        DrdsShowScheduleResultStatement x = new DrdsShowScheduleResultStatement();
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
