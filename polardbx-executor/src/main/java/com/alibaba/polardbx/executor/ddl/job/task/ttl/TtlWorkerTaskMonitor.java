package com.alibaba.polardbx.executor.ddl.job.task.ttl;

public interface TtlWorkerTaskMonitor {

    /**
     * Do some monitoring for intra tasks
     */
    void doMonitoring();

    /**
     * Check need to stop and exit intra task running.
     */
    boolean checkNeedStop();

    /**
     * Handle the results of intra-tasks
     */
    void handleResults(boolean isFinished,
                       boolean interrupted,
                       boolean withinMaintainableTimeFrame);

}
