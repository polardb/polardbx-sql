package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

/**
 * @author chenghui.lch
 */
public class DropPartsForTtlTblTaskLogInfo extends TtlAlterPartsTaskLogInfo {
    public DropPartsForTtlTblTaskLogInfo() {
        this.taskName = DropPartsForTtlTblTaskLogInfo.class.getSimpleName();
    }
}
