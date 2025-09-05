package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

/**
 * @author chenghui.lch
 */
public class AddPartsForCciTaskLogInfo extends TtlAlterPartsTaskLogInfo {
    public AddPartsForCciTaskLogInfo() {
        this.taskName = AddPartsForCciTaskLogInfo.class.getSimpleName();
    }
}
