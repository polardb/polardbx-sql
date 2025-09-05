package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

public class TtlLoggerUtil {
    // TTL 任务日志
    public final static Logger TTL_TASK_LOGGER = SQLRecorderLogger.ttlTaskLogger;

    public static String buildTaskLogRowMsgPrefix(Long jobId, Long taskId, String name) {
        String msgPrefix = String.format("TaskInfoLog-%s-%s-%s: ", jobId, taskId, name);
        return msgPrefix;
    }

    public static void logTaskMsg(DdlTask parentDdlTask,
                                  TtlJobContext jobContext,
                                  String taskExecRsMsg) {
        String logHeader = buildIntraTaskLogMsgHeader(parentDdlTask, jobContext);
        StringBuilder sb = new StringBuilder();
        sb.append(logHeader);
        sb.append(", FullTaskInfoLog=[");
        sb.append(taskExecRsMsg);
        sb.append("]");
        TTL_TASK_LOGGER.info(sb.toString());
    }

    public static String buildIntraTaskLogMsgHeader(DdlTask parentDdlTask,
                                                    TtlJobContext jobContext) {
        Long jobId = parentDdlTask.getJobId();
        Long taskId = parentDdlTask.getTaskId();
        String taskName = parentDdlTask.getName();
        String targetTableSchema = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String targetTableName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        String fullTblName = String.format("[%s.%s]", targetTableSchema, targetTableName);
        String fullIntraTaskName = String.format("%s[%s,%s]-%s", taskName, jobId, taskId, fullTblName);
        String logMsgHeader =
            String.format("task=%s", fullIntraTaskName);
        return logMsgHeader;
    }

    public static String buildIntraTaskLogMsgHeaderWithPhyPart(DdlTask parentDdlTask,
                                                               PartitionMetaUtil.PartitionMetaRecord phyPartData) {
        Long jobId = parentDdlTask.getJobId();
        Long taskId = parentDdlTask.getTaskId();
        String taskName = parentDdlTask.getName();
//        String fullTblName = String.format("[%s.%s]", phyPartData.getTableSchema(), phyPartData.getTableName());
        String phyPartName = String.format("[%s:%s]", phyPartData.getPartNum(), phyPartData.getPartName());
        String fullIntraTaskName = String.format("%s[%s,%s]-%s", taskName, jobId, taskId, phyPartName);
        String fulPhyTblName =
            String.format("[%s.%s]", phyPartData.getPhyDb(), phyPartData.getPhyTb());
        String logMsgHeader =
            String.format("task=%s, phyTb=%s", fullIntraTaskName, fulPhyTblName);
        return logMsgHeader;
    }

    public static void logTaskExecResult(
        DdlTask parentDdlTask,
        TtlJobContext jobContext,
        TtlAlterPartsTaskLogInfo logInfo) {

        try {

            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = logInfo.getJobStatInfo();
            TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = logInfo.getGlobalStatInfo();

            Long jobId = logInfo.getJobId();
            Long taskId = logInfo.getTaskId();

            String ttlTblSchema = logInfo.getTtlTblSchema();
            String ttlTblName = logInfo.getTtlTblName();

            String arcTmpTblSchema = logInfo.getArcTmpTblSchema();
            String arcTmpTblName = logInfo.getArcTmpTblName();
            String arcCciFulLTblName = logInfo.getArcCciFullTblName();

            Long ttlInterval = logInfo.getTtlInterval();
            String ttlTimeUnit = logInfo.getTtlTimeUnit();

            Long arcPartInterval = logInfo.getArcPartInterval();
            String arcPartTimeUnit = logInfo.getArcPartTimeUnit();

            Long finalPreAllocateCount = logInfo.getFinalPreAllocateCount();

            String currentDateTime = logInfo.getCurrentDateTime();
            String formatedCurrentDateTime = logInfo.getFormatedCurrentDateTime();

            Boolean needPerformSubJobTaskDdl = logInfo.getNeedPerformSubJobTaskDdl();
            String subJobTaskDdlStmt = logInfo.getSubJobTaskDdlStmt();
            Boolean foundOptiTblDdlRunning = logInfo.getFoundOptiTblDdlRunning();
            Boolean ignoreSubJobTaskDdlOnCurrRound = logInfo.getIgnoreSubJobTaskDdlOnCurrRound();

            String logMsg = "";
            String msgPrefix = TtlLoggerUtil.buildTaskLogRowMsgPrefix(jobId, taskId, "AddNewPartsForArcTable");
            logMsg += String.format("ttlTblSchema: %s, ttlTblName: %s\n", ttlTblSchema, ttlTblName);
            logMsg += String.format("arcTmpTblSchema: %s, arcTmpTblName: %s\n", arcTmpTblSchema, arcTmpTblName);
            logMsg += String.format("cciFullTable: %s\n", arcCciFulLTblName);

            logMsg += msgPrefix;
            logMsg += String.format("ttlInterval: %s, ttlTimeUnit: %s\n", ttlInterval, ttlTimeUnit);

            logMsg += msgPrefix;
            logMsg += String.format("arcPartInterval: %s, arcPartTimeUnit: %s\n", arcPartInterval, arcPartTimeUnit);

            logMsg += msgPrefix;
            logMsg += String.format("finalPreAllocateCount: %s\n", finalPreAllocateCount);

            logMsg += msgPrefix;
            logMsg += String.format("currentDateTime: %s\n", currentDateTime);
            logMsg += msgPrefix;
            logMsg += String.format("formatedCurrentDateTime: %s\n", formatedCurrentDateTime);

//            logMsg += msgPrefix;
//            logMsg += String.format("currMaxBoundVal: %s\n", logInfo.currMaxBoundVal);

            logMsg += msgPrefix;
            logMsg += String.format("needPerformSubJobTaskDdl: %s\n", needPerformSubJobTaskDdl);
            logMsg += msgPrefix;
            logMsg += String.format("subJobTaskDdlStmt: %s\n", subJobTaskDdlStmt);

            logMsg += msgPrefix;
            logMsg += String.format("foundOptiTblDdlRunning: %s\n", foundOptiTblDdlRunning);
            logMsg += msgPrefix;
            logMsg += String.format("ignoreSubJobTaskDdlOnCurrRound: %s\n", ignoreSubJobTaskDdlOnCurrRound);

//            if (needPerformSubJobTaskDdl && !ignoreSubJobTaskDdlOnCurrRound) {
//                jobStatInfo.getAddPartSqlCount().addAndGet(1);
//                globalStatInfo.getTotalAddPartSqlCount().addAndGet(1);
//            }

            TtlLoggerUtil.logTaskMsg(parentDdlTask, jobContext, logMsg);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
        }
    }

}