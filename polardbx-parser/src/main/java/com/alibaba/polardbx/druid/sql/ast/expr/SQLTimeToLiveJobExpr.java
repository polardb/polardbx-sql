package com.alibaba.polardbx.druid.sql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExprImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author chenghui.lch
 */
public class SQLTimeToLiveJobExpr extends SQLExprImpl {

    protected SQLExpr cron;
    protected SQLExpr timezone;

    public SQLTimeToLiveJobExpr() {
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        SQLTimeToLiveJobExpr otherTtlExpr = (SQLTimeToLiveJobExpr) obj;
        SQLExpr otherCron = otherTtlExpr.getCron();
        SQLExpr otherTimezone = otherTtlExpr.getTimezone();

        if (cron != null) {
            if (!cron.equals(otherCron)) {
                return false;
            }
        } else {
            if (otherCron != null) {
                return false;
            }
        }

        if (timezone != null) {
            if (!timezone.equals(otherTimezone)) {
                return false;
            }
        } else {
            if (otherTimezone != null) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        SQLASTVisitor visitor = new MySqlOutputVisitor(sb);
        this.accept(visitor);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = cron != null ? cron.hashCode() : 0;
        result = 31 * result + (timezone != null ? timezone.hashCode() : 0);
        return result;
    }

    @Override
    public SQLExpr clone() {
        SQLTimeToLiveJobExpr newTtlExpr = new SQLTimeToLiveJobExpr();
        if (cron != null) {
            newTtlExpr.setCron(cron.clone());
        }

        if (timezone != null) {
            newTtlExpr.setTimezone(timezone.clone());
        }
        return newTtlExpr;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (cron != null) {
                acceptChild(visitor, cron);
            }

            if (timezone != null) {
                acceptChild(visitor, timezone);
            }

        }
        visitor.endVisit(this);
    }

    public SQLExpr getTimezone() {
        return timezone;
    }

    public void setTimezone(SQLExpr timezone) {
        this.timezone = timezone;
    }

    public SQLExpr getCron() {
        return cron;
    }

    public void setCron(SQLExpr cron) {
        this.cron = cron;
    }
}
