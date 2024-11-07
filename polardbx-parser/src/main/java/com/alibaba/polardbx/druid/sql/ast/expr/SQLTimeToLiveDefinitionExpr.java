package com.alibaba.polardbx.druid.sql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExprImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author chenghui.lch
 */
public class SQLTimeToLiveDefinitionExpr extends SQLExprImpl {

    protected SQLExpr ttlEnableExpr;
    protected SQLExpr ttlExpr;
    protected SQLExpr ttlJobExpr;
    protected SQLExpr ttlFilterExpr;
    protected SQLExpr archiveTypeExpr;
    protected SQLExpr archiveTableSchemaExpr;
    protected SQLExpr archiveTableNameExpr;
    protected SQLExpr archiveTablePreAllocateExpr;
    protected SQLExpr archiveTablePostAllocateExpr;

    public SQLTimeToLiveDefinitionExpr() {
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

        SQLTimeToLiveDefinitionExpr otherTtlDefineExpr = (SQLTimeToLiveDefinitionExpr) obj;
        SQLExpr otherTtlEnableExpr = otherTtlDefineExpr.getTtlEnableExpr();
        SQLExpr otherTtlExpr = otherTtlDefineExpr.getTtlExpr();
        SQLExpr otherTtlJobExpr = otherTtlDefineExpr.getTtlJobExpr();
        SQLExpr otherTtlFilterExpr = otherTtlDefineExpr.getTtlFilterExpr();
        SQLExpr otherArchiveTypeExpr = otherTtlDefineExpr.getArchiveTypeExpr();
        SQLExpr otherArchiveSchemaExpr = otherTtlDefineExpr.getArchiveTableSchemaExpr();
        SQLExpr otherArchiveNameExpr = otherTtlDefineExpr.getArchiveTableNameExpr();
        SQLExpr otherArchivePreAllocateExpr = otherTtlDefineExpr.getArchiveTablePreAllocateExpr();
        SQLExpr otherArchivePostAllocateExpr = otherTtlDefineExpr.getArchiveTablePostAllocateExpr();

        if (ttlEnableExpr != null) {
            if (otherTtlEnableExpr == null) {
                return false;
            }
            if (!ttlEnableExpr.equals(otherTtlEnableExpr)) {
                return false;
            }
        } else {
            if (otherTtlEnableExpr != null) {
                return false;
            }
        }

        if (ttlExpr != null) {
            if (otherTtlExpr == null) {
                return false;
            }
            if (!ttlExpr.equals(otherTtlExpr)) {
                return false;
            }
        } else {
            if (otherTtlExpr != null) {
                return false;
            }
        }

        if (ttlJobExpr != null) {
            if (otherTtlJobExpr == null) {
                return false;
            }
            if (!ttlJobExpr.equals(otherTtlJobExpr)) {
                return false;
            }
        } else {
            if (otherTtlJobExpr != null) {
                return false;
            }
        }

        if (ttlFilterExpr != null) {
            if (otherTtlFilterExpr == null) {
                return false;
            }
            if (!ttlExpr.equals(otherTtlFilterExpr)) {
                return false;
            }
        } else {
            if (otherTtlFilterExpr != null) {
                return false;
            }
        }

        if (archiveTypeExpr != null) {
            if (otherArchiveTypeExpr == null) {
                return false;
            }
            if (!archiveTypeExpr.equals(otherArchiveTypeExpr)) {
                return false;
            }
        } else {
            if (otherArchiveTypeExpr != null) {
                return false;
            }
        }

        if (archiveTableSchemaExpr != null) {
            if (otherArchiveSchemaExpr == null) {
                return false;
            }
            if (!archiveTableSchemaExpr.equals(otherArchiveSchemaExpr)) {
                return false;
            }
        } else {
            if (otherArchiveSchemaExpr != null) {
                return false;
            }
        }

        if (archiveTableNameExpr != null) {
            if (otherArchiveNameExpr == null) {
                return false;
            }
            if (!archiveTableNameExpr.equals(otherArchiveNameExpr)) {
                return false;
            }
        } else {
            if (otherArchiveNameExpr != null) {
                return false;
            }
        }

        if (archiveTablePreAllocateExpr != null) {
            if (otherArchivePreAllocateExpr == null) {
                return false;
            }
            if (!archiveTablePreAllocateExpr.equals(otherArchivePreAllocateExpr)) {
                return false;
            }
        } else {
            if (otherArchivePreAllocateExpr != null) {
                return false;
            }
        }

        if (archiveTablePostAllocateExpr != null) {
            if (otherArchivePostAllocateExpr == null) {
                return false;
            }
            if (!archiveTablePostAllocateExpr.equals(otherArchivePostAllocateExpr)) {
                return false;
            }
        } else {
            if (otherArchivePostAllocateExpr != null) {
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

        int result = ttlEnableExpr != null ? ttlEnableExpr.hashCode() : 0;
        result = 31 * result + (ttlExpr != null ? ttlExpr.hashCode() : 0);
        result = 31 * result + (ttlJobExpr != null ? ttlJobExpr.hashCode() : 0);
        result = 31 * result + (ttlFilterExpr != null ? ttlFilterExpr.hashCode() : 0);
        result = 31 * result + (archiveTypeExpr != null ? archiveTypeExpr.hashCode() : 0);
        result = 31 * result + (archiveTableSchemaExpr != null ? archiveTableSchemaExpr.hashCode() : 0);
        result = 31 * result + (archiveTableNameExpr != null ? archiveTableNameExpr.hashCode() : 0);
        result = 31 * result + (archiveTablePreAllocateExpr != null ? archiveTablePreAllocateExpr.hashCode() : 0);
        result = 31 * result + (archiveTablePostAllocateExpr != null ? archiveTablePostAllocateExpr.hashCode() : 0);

        return result;
    }

    @Override
    public SQLExpr clone() {

        SQLTimeToLiveDefinitionExpr sqlTimeToLiveDefinitionExpr = new SQLTimeToLiveDefinitionExpr();

        if (ttlEnableExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlEnableExpr(ttlEnableExpr.clone());
        }

        if (ttlExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlExpr(ttlExpr.clone());
        }

        if (ttlJobExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlJobExpr(ttlJobExpr.clone());
        }
        if (archiveTypeExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTypeExpr(archiveTypeExpr.clone());
        }

        if (archiveTableSchemaExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTableSchemaExpr(archiveTableSchemaExpr.clone());
        }

        if (archiveTableNameExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTableNameExpr(archiveTableNameExpr.clone());
        }

        if (archiveTablePreAllocateExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTablePreAllocateExpr(archiveTablePreAllocateExpr.clone());
        }

        if (archiveTablePostAllocateExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTablePostAllocateExpr(archiveTablePostAllocateExpr.clone());
        }

        return sqlTimeToLiveDefinitionExpr;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (ttlEnableExpr != null) {
                acceptChild(visitor, ttlEnableExpr);
            }

            if (ttlExpr != null) {
                acceptChild(visitor, ttlExpr);
            }

            if (ttlJobExpr != null) {
                acceptChild(visitor, ttlJobExpr);
            }

            if (ttlFilterExpr != null) {
                acceptChild(visitor, ttlFilterExpr);
            }

            if (archiveTypeExpr != null) {
                acceptChild(visitor, archiveTypeExpr);
            }

            if (archiveTableSchemaExpr != null) {
                acceptChild(visitor, archiveTableSchemaExpr);
            }

            if (archiveTableNameExpr != null) {
                acceptChild(visitor, archiveTableNameExpr);
            }

            if (archiveTablePreAllocateExpr != null) {
                acceptChild(visitor, archiveTablePreAllocateExpr);
            }

            if (archiveTablePostAllocateExpr != null) {
                acceptChild(visitor, archiveTablePostAllocateExpr);
            }
        }
        visitor.endVisit(this);
    }

    public SQLExpr getTtlEnableExpr() {
        return ttlEnableExpr;
    }

    public void setTtlEnableExpr(SQLExpr ttlEnableExpr) {
        this.ttlEnableExpr = ttlEnableExpr;
    }

    public SQLExpr getTtlExpr() {
        return ttlExpr;
    }

    public void setTtlExpr(SQLExpr ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    public SQLExpr getTtlJobExpr() {
        return ttlJobExpr;
    }

    public void setTtlJobExpr(SQLExpr ttlJobExpr) {
        this.ttlJobExpr = ttlJobExpr;
    }

    public SQLExpr getTtlFilterExpr() {
        return ttlFilterExpr;
    }

    public void setTtlFilterExpr(SQLExpr ttlFilterExpr) {
        this.ttlFilterExpr = ttlFilterExpr;
    }

    public SQLExpr getArchiveTypeExpr() {
        return archiveTypeExpr;
    }

    public void setArchiveTypeExpr(SQLExpr archiveTypeExpr) {
        this.archiveTypeExpr = archiveTypeExpr;
    }

    public SQLExpr getArchiveTableSchemaExpr() {
        return archiveTableSchemaExpr;
    }

    public void setArchiveTableSchemaExpr(SQLExpr archiveTableSchemaExpr) {
        this.archiveTableSchemaExpr = archiveTableSchemaExpr;
    }

    public SQLExpr getArchiveTableNameExpr() {
        return archiveTableNameExpr;
    }

    public void setArchiveTableNameExpr(SQLExpr archiveTableNameExpr) {
        this.archiveTableNameExpr = archiveTableNameExpr;
    }

    public SQLExpr getArchiveTablePreAllocateExpr() {
        return archiveTablePreAllocateExpr;
    }

    public void setArchiveTablePreAllocateExpr(SQLExpr archiveTablePreAllocateExpr) {
        this.archiveTablePreAllocateExpr = archiveTablePreAllocateExpr;
    }

    public SQLExpr getArchiveTablePostAllocateExpr() {
        return archiveTablePostAllocateExpr;
    }

    public void setArchiveTablePostAllocateExpr(SQLExpr archiveTablePostAllocateExpr) {
        this.archiveTablePostAllocateExpr = archiveTablePostAllocateExpr;
    }
}
