package com.alibaba.polardbx.executor.ddl.job.task.ttl;

/**
 * @author chenghui.lch
 */
interface TtlJobTask {
    TtlJobContext getJobContext();

    void setJobContext(TtlJobContext jobContext);
}
