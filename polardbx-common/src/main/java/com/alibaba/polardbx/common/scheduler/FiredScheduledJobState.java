package com.alibaba.polardbx.common.scheduler;

public enum FiredScheduledJobState {

    QUEUED,
    RUNNING,
    SUCCESS,
    FAILED,
    SKIPPED,
    ;

}