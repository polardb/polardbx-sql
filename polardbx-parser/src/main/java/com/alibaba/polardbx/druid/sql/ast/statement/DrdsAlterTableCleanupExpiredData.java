package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class DrdsAlterTableCleanupExpiredData extends SQLObjectImpl implements SQLAlterTableItem {

    protected SQLExpr ttlCleanup;

    protected SQLExpr ttlCleanupBound;
    protected SQLExpr archiveTablePreAllocateCount;
    protected SQLExpr archiveTablePostAllocateCount;
    protected SQLExpr ttlPartInterval;

    protected SQLExpr ttlCleanupPolicy;

    public DrdsAlterTableCleanupExpiredData() {
        super();
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public SQLExpr getTtlCleanup() {
        return ttlCleanup;
    }

    public void setTtlCleanup(SQLExpr ttlCleanup) {
        this.ttlCleanup = ttlCleanup;
    }

    public SQLExpr getTtlCleanupBound() {
        return ttlCleanupBound;
    }

    public void setTtlCleanupBound(SQLExpr ttlCleanupBound) {
        this.ttlCleanupBound = ttlCleanupBound;
    }

    public SQLExpr getArchiveTablePreAllocateCount() {
        return archiveTablePreAllocateCount;
    }

    public void setArchiveTablePreAllocateCount(SQLExpr archiveTablePreAllocateCount) {
        this.archiveTablePreAllocateCount = archiveTablePreAllocateCount;
    }

    public SQLExpr getArchiveTablePostAllocateCount() {
        return archiveTablePostAllocateCount;
    }

    public void setArchiveTablePostAllocateCount(SQLExpr archiveTablePostAllocateCount) {
        this.archiveTablePostAllocateCount = archiveTablePostAllocateCount;
    }

    public SQLExpr getTtlPartInterval() {
        return ttlPartInterval;
    }

    public void setTtlPartInterval(SQLExpr ttlPartInterval) {
        this.ttlPartInterval = ttlPartInterval;
    }

    public SQLExpr getTtlCleanupPolicy() {
        return ttlCleanupPolicy;
    }

    public void setTtlCleanupPolicy(SQLExpr ttlCleanupPolicy) {
        this.ttlCleanupPolicy = ttlCleanupPolicy;
    }
}