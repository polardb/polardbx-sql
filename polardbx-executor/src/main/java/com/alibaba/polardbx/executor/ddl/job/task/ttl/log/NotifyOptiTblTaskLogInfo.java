package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;

public class NotifyOptiTblTaskLogInfo extends BaseTtlTaskLogInfo {
    public NotifyOptiTblTaskLogInfo() {
        this.taskName = NotifyOptiTblTaskLogInfo.class.getSimpleName();
    }

    public Long taskBeginTs;
    public Long taskEndTs;

    public long dataFreePercent;
    public long dataFreePercentLimit;

    public boolean isFinished = false;
    public boolean isInterrupted = false;
    public boolean isMaintenancePeriodOver = false;

    public boolean enableAutoCtrlOptiTblJob = true;
    public boolean needWaitAndMonitorOptiTblJob = false;
    public long waitOptiTblRunningBeginTs = 0;
    public long waitOptiTblTaskRunningTimeCost = 0;
    public long waitOptiTblTaskRunningLoopRound = 0;

    public boolean findOptiTblTaskExecuting = false;
    public boolean optiTblDdlComeFromTtlJob = false;
    public long optiTblDdlStmtJobId = 0;
    public String optiTblDdlStmt = "";
    public String newSubmittedOptiTblSql = "";
    public String optiTblDdlJobStateBeforeCtrl = "";

    public boolean noFoundOptiTblJobRec = false;
    public String continueOptiTblSql = "none";
    public boolean failedToRecoverDdlJob = false;
    public int waitFromPauseToRunningRound = 0;
    public long waitFromPauseToRunningTimeCost = 0;

    public String pauseOptiTblSql = "none";
    public long waitFromRunningToPauseTimeCost = 0;
    public int optiJobProgress = 0;

    public void logTaskExecResult(DdlTask ddlTask,
                                  TtlJobContext jobContext) {
        logTaskExecResult(ddlTask, jobContext, this);
    }

    private void logTaskExecResult(DdlTask parentDdlTak,
                                   TtlJobContext jobContext,
                                   NotifyOptiTblTaskLogInfo logInfo) {

        try {

            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = logInfo.jobStatInfo;
            TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = logInfo.globalStatInfo;

            jobStatInfo.setOptimizeTableProgress(logInfo.optiJobProgress);

            String logMsg = "";
            String msgPrefix = TtlLoggerUtil.buildTaskLogRowMsgPrefix(jobId, taskId, "NotifyOptiTbl");
            logMsg += String.format("ttlTblSchema: %s, ttlTblName: %s\n", logInfo.ttlTblSchema, logInfo.ttlTblName);

            logMsg += msgPrefix;
            logMsg += String.format("isMaintenancePeriodOver: %s\n", logInfo.isMaintenancePeriodOver);

            logMsg += msgPrefix;
            logMsg += String.format("dataFreePercent:%s , dataFreePercentLimit: %s\n", logInfo.dataFreePercent,
                logInfo.dataFreePercentLimit);

            logMsg += msgPrefix;
            logMsg += String.format("enableAutoCtrlOptiTblJob: %s\n", logInfo.enableAutoCtrlOptiTblJob);

            logMsg += msgPrefix;
            logMsg += String.format("needWaitAndMonitorOptiTblJob: %s\n", logInfo.needWaitAndMonitorOptiTblJob);

            logMsg += msgPrefix;
            logMsg +=
                String.format("interrupted: %s, loopRound: %s, waitOptiTblTimeCost(ns): %s\n", logInfo.isInterrupted,
                    logInfo.waitOptiTblTaskRunningLoopRound, logInfo.waitOptiTblTaskRunningTimeCost);

            if (!logInfo.findOptiTblTaskExecuting) {
                logMsg += msgPrefix;
                logMsg += String.format("newSubmittedOtpiTblStmt: %s\n", logInfo.newSubmittedOptiTblSql);
            } else {
                logMsg += msgPrefix;
                logMsg += String.format("optiTblStmtJobId: %s\n", logInfo.optiTblDdlStmtJobId);
                logMsg += msgPrefix;
                logMsg += String.format("optiTblStmt: %s\n", logInfo.optiTblDdlStmt);
                logMsg += msgPrefix;
                logMsg += String.format("optiTblFromTtlJob: %s\n", logInfo.optiTblDdlComeFromTtlJob);
                logMsg += msgPrefix;
                logMsg += String.format("optiTblStmtJobStateBeforeCtrl: %s\n", logInfo.optiTblDdlJobStateBeforeCtrl);
            }

            if (logInfo.needWaitAndMonitorOptiTblJob) {

                logMsg += msgPrefix;
                logMsg += String.format("noFoundOptiTblJobRec: %s\n", logInfo.noFoundOptiTblJobRec);
                logMsg += msgPrefix;
                logMsg += String.format("continueOptiTblSql: %s\n", logInfo.continueOptiTblSql);
                logMsg += msgPrefix;
                logMsg += String.format("waitFromPauseToRunningRound: %s\n", logInfo.waitFromPauseToRunningRound);
                logMsg += msgPrefix;
                logMsg +=
                    String.format("waitFromPauseToRunningTimeCost(ns): %s\n", logInfo.waitFromPauseToRunningTimeCost);
                logMsg += msgPrefix;
                logMsg += String.format("failedToRecoverDdlJob: %s\n", logInfo.failedToRecoverDdlJob);

                logMsg += msgPrefix;
                logMsg += String.format("pauseOptiTblSql: %s\n", logInfo.pauseOptiTblSql);
                logMsg += msgPrefix;
                logMsg +=
                    String.format("waitFromRunningToPauseTimeCost(ns): %s\n", logInfo.waitFromRunningToPauseTimeCost);

            }

            logMsg += msgPrefix;
            logMsg += String.format("notifyOptiTblJobFinished: %s\n", logInfo.isFinished);

            if (logInfo.isFinished) {
                logMsg += msgPrefix;
                long taskTimeCostNano = logInfo.taskEndTs - logInfo.taskBeginTs;
                logMsg += String.format("taskTimeCost(ns): %s\n", taskTimeCostNano);
                if (logInfo.needWaitAndMonitorOptiTblJob) {
                    jobStatInfo.getOptimizeSqlCount().incrementAndGet();
                    jobStatInfo.getOptimizeSqlTimeCost().addAndGet(taskTimeCostNano / 1000000); // unit: ms
                }
                jobStatInfo.setCurrJobEndTs(System.currentTimeMillis());
                globalStatInfo.getTotalTtlSuccessJobCount().incrementAndGet();
            } else {
                logMsg += msgPrefix;
                logMsg += String.format("taskTimeCost(ns): %s\n", System.nanoTime() - logInfo.taskBeginTs);
            }

            TtlLoggerUtil.logTaskMsg(parentDdlTak, jobContext, logMsg);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
        }
    }

}
