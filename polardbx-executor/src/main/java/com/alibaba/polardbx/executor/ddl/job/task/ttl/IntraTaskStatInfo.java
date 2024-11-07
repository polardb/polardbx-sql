package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chenghui.lch
 */
@Data
public class IntraTaskStatInfo {

    protected Long jobId;
    protected Long taskId;

    protected AtomicLong totalInsertRows = new AtomicLong(0);
    protected AtomicLong totalDeleteRows = new AtomicLong(0);
    protected AtomicLong totalInsertSqlCount = new AtomicLong(0);
    protected AtomicLong totalDeleteSqlCount = new AtomicLong(0);
    protected AtomicLong totalInsertTimeCostNano = new AtomicLong(0);
    protected AtomicLong totalDeleteTimeCostNano = new AtomicLong(0);
    protected AtomicLong totalSelectTimeCostNano = new AtomicLong(0);
    protected AtomicLong totalExecTimeCostNano = new AtomicLong(0);
    protected AtomicLong totalWaitPermitsTimeCostNano = new AtomicLong(0);

    public IntraTaskStatInfo() {
    }
}
