package com.alibaba.polardbx.gms.topology;

/**
 * @author chenghui.lch
 */
public class DatabaseDdlContext {
    protected volatile boolean interrupted = false;
    protected Long connId;
    protected String traceId;
    protected boolean dropDb = false;

    public DatabaseDdlContext() {
    }

    public boolean isInterrupted() {
        return interrupted;
    }

    public void setInterrupted(boolean interrupted) {
        this.interrupted = interrupted;
    }

    public Long getConnId() {
        return connId;
    }

    public void setConnId(Long connId) {
        this.connId = connId;
    }

    public boolean isDropDb() {
        return dropDb;
    }

    public void setDropDb(boolean dropDb) {
        this.dropDb = dropDb;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
}
