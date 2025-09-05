package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

/**
 * @author chenghui.lch
 */
public class AddPartsForTtlTblTaskLogInfo extends TtlAlterPartsTaskLogInfo {
    public AddPartsForTtlTblTaskLogInfo() {
        this.taskName = AddPartsForTtlTblTaskLogInfo.class.getSimpleName();
    }
}
