package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.IntraTaskStatInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;

/**
 * @author chenghui.lch
 */
public class FetchCleanupLowerBoundTaskLogInfo extends BaseTtlTaskLogInfo {

    public IntraTaskStatInfo intraTaskStatInfo;
    public String currentDatetime = null;
    public String expiredUpperBound = null;
    public String expiredLowerBound = null;
    public String ttlColMinValue = null;
    public String minBoundToBeCleanUp = null;

    public FetchCleanupLowerBoundTaskLogInfo() {
        this.taskName = FetchCleanupLowerBoundTaskLogInfo.class.getSimpleName();
    }

    @Override
    public void logTaskExecResult(DdlTask parentDdlTask, TtlJobContext jobContext) {
        String logMsgHeader = TtlLoggerUtil.buildIntraTaskLogMsgHeader(parentDdlTask, jobContext);
        logTaskExecResult(parentDdlTask, jobContext, logMsgHeader, this);
    }

    protected void logTaskExecResult(
        DdlTask parentDdlTask,
        TtlJobContext jobContext,
        String logMsgHeader,
        FetchCleanupLowerBoundTaskLogInfo logInfo) {

        IntraTaskStatInfo statInfo = logInfo.intraTaskStatInfo;

        String dbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String tbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = TtlScheduledJobStatManager.getInstance()
            .getTtlJobStatInfo(dbName, tbName);
        TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = TtlScheduledJobStatManager.getInstance()
            .getGlobalTtlJobStatInfo();

        long taskTimeCostMills = statInfo.getTotalExecTimeCostNano().get() / 1000000;
        long selectTimeCostMills = statInfo.getTotalSelectTimeCostNano().get() / 1000000;

        jobStatInfo.setTtlTblDataFreePercent(jobContext.getDataFreePercentOfTtlTblPrim());
        jobStatInfo.getSelectSqlCount().incrementAndGet();
        jobStatInfo.getSelectSqlTimeCost().addAndGet(selectTimeCostMills);
        jobStatInfo.getCleanupTimeCost().addAndGet(taskTimeCostMills);
        jobStatInfo.calcSelectSqlAvgRt();

        globalStatInfo.getTotalSelectSqlCount().incrementAndGet();
        globalStatInfo.getTotalSelectSqlTimeCost().addAndGet(selectTimeCostMills);
        globalStatInfo.getTotalCleanupTimeCost().addAndGet(taskTimeCostMills);

        /**
         * <pre>
         *     min: ttl col min value
         *     lb: lower bound to be cleanup
         *     up: upper bound to be cleanup
         *     cb: the bound to cleanup in this job
         *     bz: batch size
         *     timeCost unit: ms
         * </pre>
         */
        String simpleLogMsg = "";
        simpleLogMsg = String.format("[%s] [ts=%s,lb=%s,ub=%s,cb=%s,min=%s,bs=%s] [tc=%d,stc=%d]",
            logMsgHeader, currentDatetime, expiredLowerBound, expiredUpperBound, minBoundToBeCleanUp,
            ttlColMinValue, jobContext.getDmlBatchSize(),
            taskTimeCostMills, selectTimeCostMills);

        jobStatInfo.setCurrTtlColMinVal(String.valueOf(ttlColMinValue));
        jobStatInfo.setCurrCleanupBound(String.valueOf(minBoundToBeCleanUp));
        jobStatInfo.setCurrCleanupUpperBound(String.valueOf(expiredUpperBound));
        jobStatInfo.setCurrDataTimeVal(currentDatetime);

        TtlLoggerUtil.TTL_TASK_LOGGER.info(simpleLogMsg);

        String logMsg = "";
        String msgPrefix =
            TtlLoggerUtil.buildTaskLogRowMsgPrefix(parentDdlTask.getJobId(), parentDdlTask.getTaskId(),
                "PrepareCleanupBound");
        logMsg += msgPrefix;

        logMsg += String.format("currentDateTime: %s\n", currentDatetime);

        logMsg += msgPrefix;
        logMsg +=
            String.format("expiredLowerBnd: %s, expiredUpperBnd: %s\n", minBoundToBeCleanUp, expiredUpperBound);

        logMsg += msgPrefix;
        logMsg += String.format("ttlColMinVal: %s, minBndToBeCleanUp: %s\n", ttlColMinValue, minBoundToBeCleanUp);

        logMsg += msgPrefix;
        logMsg += String.format("ttlPrimDataFree: %s, ttlPrimDataLength: %s, ttlPrimRowLengthAvg: %s\n",
            jobContext.getDataFreeOfTtlTblPrim(), jobContext.getTtlTblPrimDataLength(),
            jobContext.getRowLengthAvgOfTtlTbl());

        logMsg += msgPrefix;
        logMsg += String.format("needPerformOptiTbl: %s, dfPercent: %s, dfPercentAvg: %s\n",
            jobContext.getNeedPerformOptiTable(), jobContext.getDataFreePercentOfTtlTblPrim(),
            jobContext.getDataFreePercentAvgOfTtlTbl());

        logMsg += msgPrefix;
        logMsg += String.format("dmlBatchSize: %s\n", jobContext.getDmlBatchSize());

        logMsg += msgPrefix;
        logMsg += String.format("selectTimeCost(ms): %s\n", selectTimeCostMills);

        logMsg += msgPrefix;
        logMsg += String.format("taskTimeCot(ms): %s\n", taskTimeCostMills);

        TtlLoggerUtil.logTaskMsg(parentDdlTask, jobContext, logMsg);

    }
}
