package com.alibaba.polardbx.executor.ddl.job.task.ttl.log;

import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobContext;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlJobUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Data;

/**
 * @author chenghui.lch
 */
@Data
public class TtlAlterPartsTaskLogInfo extends BaseTtlTaskLogInfo {

    public TtlAlterPartsTaskLogInfo() {
        this.taskName = TtlAlterPartsTaskLogInfo.class.getSimpleName();
    }

    public void logTaskExecResult(
        DdlTask parentDdlTask,
        TtlJobContext jobContext) {
        logTaskExecResult(parentDdlTask, jobContext, this);
    }

    public static void initAlterPartsTaskLogInfo(TtlAlterPartsTaskLogInfo logInfo,
                                                 TtlJobContext jobContext,
                                                 ExecutionContext executionContext) {

        TtlDefinitionInfo ttlDefinitionInfo = jobContext.getTtlInfo();
        TtlInfoRecord ttlInfoRec = ttlDefinitionInfo.getTtlInfoRecord();
        String ttlTblSchema = ttlInfoRec.getTableSchema();
        String ttlTblName = ttlInfoRec.getTableName();
        TableMeta arcCciTableMeta = TtlJobUtil.getArchiveCciTableMeta(ttlTblSchema, ttlTblName, executionContext);

        String currentDataTime = jobContext.getCurrentDateTime();
        String formatedCurrentDatetime = jobContext.getCurrentDateTimeFormatedByArcPartUnit();

        logInfo.ttlTblSchema = ttlTblSchema;
        logInfo.ttlTblName = ttlTblName;
        logInfo.jobStatInfo = TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(ttlTblSchema, ttlTblName);
        logInfo.globalStatInfo = TtlScheduledJobStatManager.getInstance().getGlobalTtlJobStatInfo();
        logInfo.arcTmpTblSchema = ttlDefinitionInfo.getTmpTableSchema();
        logInfo.arcTmpTblName = ttlDefinitionInfo.getTmpTableName();
        logInfo.currentDateTime = currentDataTime;
        logInfo.formatedCurrentDateTime = formatedCurrentDatetime;

        logInfo.arcCciFullTblName = null;
        if (arcCciTableMeta != null) {
            logInfo.arcCciFullTblName = arcCciTableMeta.getTableName();
        }

        Integer ttlInterval = ttlDefinitionInfo.getTtlInfoRecord().getTtlInterval();
        TtlTimeUnit ttlTimeUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getTtlUnit());
        Integer arcPartInterval = ttlDefinitionInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getArcPartUnit());

        logInfo.ttlTimeUnit = ttlTimeUnit.getUnitName();
        logInfo.ttlInterval = Long.valueOf(ttlInterval);
        logInfo.arcPartTimeUnit = arcPartUnit.getUnitName();
        logInfo.arcPartInterval = Long.valueOf(arcPartInterval);

    }

    private void logTaskExecResult(
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
            String msgPrefix = TtlLoggerUtil.buildTaskLogRowMsgPrefix(jobId, taskId, taskName);
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

            if (needPerformSubJobTaskDdl && !ignoreSubJobTaskDdlOnCurrRound) {

                if (taskName.equalsIgnoreCase(AddPartsForCciTaskLogInfo.class.getSimpleName())) {
                    jobStatInfo.getAddPartSqlCount().addAndGet(1);
                    globalStatInfo.getTotalAddPartSqlCount().addAndGet(1);
                } else if (taskName.equalsIgnoreCase(AddPartsForCciTaskLogInfo.class.getSimpleName())) {
                    jobStatInfo.getAddPartSqlCount().addAndGet(1);
                    globalStatInfo.getTotalAddPartSqlCount().addAndGet(1);
                } else if (taskName.equalsIgnoreCase(DropPartsForTtlTblTaskLogInfo.class.getSimpleName())) {
                    jobStatInfo.getDropPartSqlCount().addAndGet(1);
//                   globalStatInfo.getTotalAddPartSqlCount().addAndGet(1);
                } else {

                }
            }

            TtlLoggerUtil.logTaskMsg(parentDdlTask, jobContext, logMsg);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
        }
    }
}
