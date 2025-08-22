package com.alibaba.polardbx.druid.sql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExprImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author chenghui.lch
 */
public class SQLTimeToLiveExpr extends SQLExprImpl {

    /**
     * <pre>
     * usage1- expire by time:
     *     `ttlColName` EXPIRE AFTER $interval $timeUnit [TIMEZONE '$timezone']
     * usage2- expire by partition count:
     *     `ttlColName` EXPIRE OVER $partCount PARTITIONS [TIMEZONE '$timezone']
     * </pre>
     */

    /**
     * For ttlColName
     */
    protected SQLExpr column;

    /**
     * For $interval $timeUnit of EXPIRE AFTER
     */
    protected SQLExpr expireAfter;
    protected SQLExpr unit;

    /**
     * For $partCount of EXPIRE OVER
     */
    protected SQLExpr expireOver;

    /**
     * For TIMEZONE '$timezone'
     */
    protected SQLExpr timezone;

    public SQLTimeToLiveExpr() {
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

        SQLTimeToLiveExpr otherTtlExpr = (SQLTimeToLiveExpr) obj;
        SQLExpr otherColumn = otherTtlExpr.getColumn();
        SQLExpr otherExpireAfter = otherTtlExpr.getExpireAfter();
        SQLExpr otherExpireOver = otherTtlExpr.getExpireOver();
        SQLExpr otherUnit = otherTtlExpr.getUnit();
        SQLExpr otherTimezone = otherTtlExpr.getTimezone();

        if (column != null) {
            if (!column.equals(otherColumn)) {
                return false;
            }
        } else {
            if (otherColumn != null) {
                return false;
            }
        }

        if (expireAfter != null) {
            if (!expireAfter.equals(otherExpireAfter)) {
                return false;
            }
        } else {
            if (otherExpireAfter != null) {
                return false;
            }
        }

        if (unit != null) {
            if (!unit.equals(otherUnit)) {
                return false;
            }
        } else {
            if (otherUnit != null) {
                return false;
            }
        }

        if (expireOver != null) {
            if (!expireOver.equals(otherExpireOver)) {
                return false;
            }
        } else {
            if (otherExpireOver != null) {
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
    public int hashCode() {
        int result = column != null ? column.hashCode() : 0;
        result = 31 * result + (expireAfter != null ? expireAfter.hashCode() : 0);
        result = 31 * result + (expireOver != null ? expireOver.hashCode() : 0);
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        result = 31 * result + (timezone != null ? timezone.hashCode() : 0);
        return result;
    }

    @Override
    public SQLExpr clone() {
        SQLTimeToLiveExpr newTtlExpr = new SQLTimeToLiveExpr();
        if (column != null) {
            newTtlExpr.setColumn(column.clone());
        }
        if (expireAfter != null) {
            newTtlExpr.setExpireAfter(expireAfter.clone());
        }

        if (expireOver != null) {
            newTtlExpr.setExpireOver(expireOver.clone());
        }

        if (unit != null) {
            newTtlExpr.setUnit(unit.clone());
        }

        if (timezone != null) {
            newTtlExpr.setTimezone(timezone.clone());
        }
        return newTtlExpr;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.column != null) {
                acceptChild(visitor, this.column);
            }

            if (this.expireAfter != null) {
                acceptChild(visitor, this.expireAfter);
            }

            if (this.unit != null) {
                acceptChild(visitor, this.unit);
            }

            if (this.expireOver != null) {
                acceptChild(visitor, this.expireOver);
            }

            if (this.timezone != null) {
                acceptChild(visitor, this.timezone);
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder("");
        SQLASTVisitor visitor = new MySqlOutputVisitor(sb);
        this.accept(visitor);
        return sb.toString();
    }

    public SQLExpr getColumn() {
        return column;
    }

    public void setColumn(SQLExpr column) {
        this.column = column;
    }

    public SQLExpr getExpireAfter() {
        return expireAfter;
    }

    public void setExpireAfter(SQLExpr expireAfter) {
        this.expireAfter = expireAfter;
    }

    public SQLExpr getUnit() {
        return unit;
    }

    public void setUnit(SQLExpr unit) {
        this.unit = unit;
    }

    public SQLExpr getTimezone() {
        return timezone;
    }

    public void setTimezone(SQLExpr timezone) {
        this.timezone = timezone;
    }

    public SQLExpr getExpireOver() {
        return expireOver;
    }

    public void setExpireOver(SQLExpr expireOver) {
        this.expireOver = expireOver;
    }
}
