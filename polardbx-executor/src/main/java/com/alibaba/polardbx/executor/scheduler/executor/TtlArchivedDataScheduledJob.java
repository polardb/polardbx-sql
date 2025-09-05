package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobManager;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowJobsHandler;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.TTL_JOB;

/**
 * TTL Table scheduled job
 *
 * @author chenghui.lch
 */
public class TtlArchivedDataScheduledJob extends SchedulerExecutor {
    private static final Logger logger = LoggerFactory.getLogger(TtlArchivedDataScheduledJob.class);
    private final ExecutableScheduledJob executableScheduledJob;

    public TtlArchivedDataScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    public long getScheduleId() {
        return this.executableScheduledJob.getScheduleId();
    }

    @Override
    public boolean execute() {
        final String tableSchema = executableScheduledJob.getTableSchema();
        final String timeZoneStr = executableScheduledJob.getTimeZone();
        final String tableName = executableScheduledJob.getTableName();
        final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);
        final long scheduleId = executableScheduledJob.getScheduleId();
        final long fireTime = executableScheduledJob.getFireTime();
        final long startTime = ZonedDateTime.now().toEpochSecond();

        try {

            logTtlScheduledJob(String.format("TtlScheduledJob[%s.%s] start.", tableSchema, tableName), null, false,
                false);

            /**
             * Stop all ttl-job scheduled
             */
            if (TtlConfigUtil.isStopAllTtlTableJobScheduling()) {
                /**
                 * If NOT allowed scheduling, just return directly with QUEUED-STATE
                 */
                logTtlScheduledJob(
                    String.format("All ttl jobs is not allowed scheduling, so ignore and queue to exec."), null, false,
                    false);
                return false;
            }

            /**
             * ask ttl job manager if allowed running curr job
             */
            boolean allowedRunningNow = applyForRunning();
            if (!allowedRunningNow) {
                /**
                 * If NOT allowed, just return directly with QUEUED-STATE
                 */
                logTtlScheduledJob(
                    String.format("Failed to apply running, so ignore and queue to exec."), null, false, false);
                return false;
            }

            // Fetch all ddl-job
            List<DdlEngineRecord> allDdlRecList =
                getCurrentDdlJobRecList(tableSchema, tableName,
                    new DdlState[] {DdlState.RUNNING, DdlState.QUEUED, DdlState.PAUSED});
            List<DdlEngineRecord> allRunningDdlRecList = new ArrayList<>();
            List<DdlEngineRecord> allPausedDdlRecList = new ArrayList<>();
            for (int i = 0; i < allDdlRecList.size(); i++) {
                DdlEngineRecord ddlRec = allDdlRecList.get(i);
                DdlState stateVal = DdlState.valueOf(ddlRec.state);
                if (stateVal == DdlState.RUNNING || stateVal == DdlState.QUEUED) {
                    allRunningDdlRecList.add(ddlRec);
                } else {
                    allPausedDdlRecList.add(ddlRec);
                }
            }
            boolean foundRunningDdl = !allRunningDdlRecList.isEmpty();
            boolean foundPausedDdl = !allPausedDdlRecList.isEmpty();
            String remark = "";
            if (!foundRunningDdl) {
                if (foundPausedDdl) {
                    /**
                     * try to recover ddl job by  "continue ddl xxx", and then
                     * just wait them to finished
                     */

                    /**
                     * Just wait them to finished
                     */
                    String ddlJobIdStr = "";
                    Long firstJobId = 0L;
                    for (int i = 0; i < allPausedDdlRecList.size(); i++) {
                        DdlEngineRecord ddlRec = allPausedDdlRecList.get(i);
                        if (i > 0) {
                            ddlJobIdStr += ",";
                        } else {
                            firstJobId = ddlRec.jobId;
                        }
                        ddlJobIdStr += String.format("[%s:%s]", ddlRec.jobId, ddlRec.ddlStmt);
                    }

                    // If there are paused DDLs, continue them first
                    List<Long> restartedJobIdList = new ArrayList<>();
                    for (DdlEngineRecord ddlRec : allPausedDdlRecList) {
                        Long jobId = ddlRec.getJobId();
                        if (!checkIfScheduledTtlJobInMaintenanceWindow()) {
                            continue;
                        }
                        if (jobId != null) {
                            restartedJobIdList.add(jobId);
                            String msg = String.format("Restart ttl job ddl. table:[%s], jobId:[%d]", tableName, jobId);
                            logTtlScheduledJob(msg, null, false, true);
                            String continueDdlSql = TtlTaskSqlBuilder.buildAsyncContinueDdlSql(jobId);
                            executeBackgroundSql(continueDdlSql, tableSchema, timeZone);
                        }
                    }
                    remark = genRemark(true, false, firstJobId, ddlJobIdStr);
                } else {
                    /**
                     * try to submit a new  ddl stmt, and then
                     * just wait them to finished
                     */
                    String alterTableCleanupExpiredDataSql =
                        TtlTaskSqlBuilder.buildAsyncTtTableCleanupExpiredDataSql(tableSchema, tableName);
                    String msg = String.format("Exec ttl job ddl. table:[%s], sql:[%s]", tableName,
                        alterTableCleanupExpiredDataSql);
                    logTtlScheduledJob(msg, null, false, true);
                    executeBackgroundSql(alterTableCleanupExpiredDataSql, tableSchema, timeZone);
                    remark = genRemark(false, false, 0L, alterTableCleanupExpiredDataSql);
                    markTtlJobFromAutoSchedule(tableSchema, tableName);
                }
            } else {
                /**
                 * Just wait them to finished
                 */
                String ddlJobIdStr = "";
                Long firstJobId = 0L;
                for (int i = 0; i < allRunningDdlRecList.size(); i++) {
                    DdlEngineRecord ddlRec = allRunningDdlRecList.get(i);
                    if (i > 0) {
                        ddlJobIdStr += ",";
                    } else {
                        firstJobId = ddlRec.jobId;
                    }
                    ddlJobIdStr += String.format("[%s:%s]", ddlRec.jobId, ddlRec.ddlStmt);
                }
                remark = genRemark(false, true, firstJobId, ddlJobIdStr);
            }

            /**
             * Update remark for scheduled job
             */

            boolean needInterruptJob = false;

            int waitRound = 0;
            while (true) {
                List<DdlEngineRecord> latestRunningDdlRecList =
                    getCurrentDdlJobRecList(tableSchema, tableName, new DdlState[] {DdlState.RUNNING, DdlState.QUEUED});

                if (!inMaintenanceWindow()) {
                    needInterruptJob = true;
                    break;
                }

                if (latestRunningDdlRecList.isEmpty()) {
                    break;
                }

                /**
                 * Wait result
                 */
                try {
                    waitRound++;
                    if (waitRound % 12 == 0) {
                        logTtlScheduledJob("Waiting for ddl stmt finish running...", null, false, false);
                    }
                    Thread.sleep(5000);
                } catch (Throwable ex) {
                    // ignore
                }
            }

            if (needInterruptJob) {
                /**
                 * Gen some remark
                 */
                return interruptionExit(scheduleId, fireTime, "Out of maintenance window");
            }

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    PROCESS_END,
                    new String[] {
                        TTL_JOB + "," + fireTime,
                        remark + ", consuming " + (finishTime - startTime) + " seconds"
                    },
                    NORMAL
                );

            return succeedExit(scheduleId, fireTime, remark);

        } catch (Throwable t) {
            // The schedule job may be interrupted asynchronously. e.g. Pause DDL by other thread,
            // throwing an exception which should be caught
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    UNEXPECTED,
                    new String[] {
                        TTL_JOB + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            String msg = String.format(
                "process scheduled ttl job[%s] error, fireTime is [%s], error is %s", scheduleId, fireTime,
                t.getMessage());
            logTtlScheduledJob(msg, t, true, false);
            errorExit(scheduleId, fireTime, t.getMessage());
            return false;
        }
    }

    private void markTtlJobFromAutoSchedule(String ttlTblSchema, String ttlTblName) {
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo =
            TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(ttlTblSchema, ttlTblName);
        if (jobStatInfo != null) {
            jobStatInfo.setCurrJobFromScheduler(true);
        }
    }

    private String fetchDdlJobIdFromRemark(String remarkJson) {
        return "";
    }

    @Override
    public Pair<Boolean, String> needInterrupted() {

        boolean withInMaintainWindow = checkIfScheduledTtlJobInMaintenanceWindow();
        if (withInMaintainWindow) {
            if (executableScheduledJob.getState().equalsIgnoreCase("RUNNING")) {
                String tableSchema = executableScheduledJob.getTableSchema();
                String tableName = executableScheduledJob.getTableName();
                Long scheduleId = executableScheduledJob.getScheduleId();
                Long fireTime = executableScheduledJob.getFireTime();

                Long fireTimeInMap = ScheduledJobsManager.get(scheduleId);
                DdlState[] allStates = new DdlState[DdlState.ALL_STATES.size()];
                DdlState.ALL_STATES.toArray(allStates);

                if (fireTimeInMap == null) {
                    List<DdlEngineRecord> allDdlRecList =
                        getCurrentDdlJobRecList(tableSchema, tableName,
                            new DdlState[] {DdlState.RUNNING, DdlState.QUEUED});
                    if (allDdlRecList.isEmpty()) {
                        return Pair.of(true, "No found in-memory running-state jobs and any running/queued ddl stmt");
                    }
                }
            }
        }

        return Pair.of(!withInMaintainWindow, "Out of maintenance window");
    }

    public boolean interrupt() {
        return interruptionExit(executableScheduledJob.getScheduleId(), executableScheduledJob.getFireTime(),
            "Interrupted by auto interrupter");
    }

    private List<Long> getCurrentDdlSqlList(String tableSchema, String tableName, DdlState[] ddlStates) {
        String alterTableCleanupExpireDataKeyWord = TtlTaskSqlBuilder.ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE_KEYWORD;
        List<Long> sqlList = new ArrayList<>();
        for (DdlEngineRecord record : DdlEngineShowJobsHandler.inspectDdlJobs(Pair.of(null, tableSchema),
            new DdlEngineSchedulerManager())) {
            String objName = record.objectName;
            if (StringUtils.isEmpty(objName)) {
                continue;
            }
            if (!objName.equalsIgnoreCase(tableName)) {
                continue;
            }
            for (DdlState ddlState : ddlStates) {
                if (ddlState.name().equalsIgnoreCase(record.state)) {
                    if (record.ddlStmt.toLowerCase().contains(alterTableCleanupExpireDataKeyWord)) {
                        sqlList.add(record.jobId);
                    }
                    break;
                }
            }
        }
        return sqlList;
    }

    private List<DdlEngineRecord> getCurrentDdlJobRecList(String tableSchema, String tableName, DdlState[] ddlStates) {
        String alterTableCleanupExpireDataKeyWord = TtlTaskSqlBuilder.ALTER_TABLE_CLEANUP_EXPIRED_DATA_TEMPLATE_KEYWORD;
        List<DdlEngineRecord> sqlList = new ArrayList<>();
        for (DdlEngineRecord record : DdlEngineShowJobsHandler.inspectDdlJobs(Pair.of(null, tableSchema),
            new DdlEngineSchedulerManager())) {
            String objName = record.objectName;
            if (StringUtils.isEmpty(objName)) {
                continue;
            }
            if (!objName.equalsIgnoreCase(tableName)) {
                continue;
            }
            for (DdlState ddlState : ddlStates) {
                if (ddlState.name().equalsIgnoreCase(record.state)) {
                    if (record.ddlStmt.toLowerCase().contains(alterTableCleanupExpireDataKeyWord)) {
                        sqlList.add(record);
                    }
                    break;
                }
            }
        }
        return sqlList;
    }

    private String genRemark(boolean findPausedDdl, boolean findRunningDdl, Long jobId, String ddlStmt) {
        String remark = "";
        if (findPausedDdl) {
            remark = String.format("find pause ddl, ddl_job_id is %s", jobId);
        } else {
            if (findRunningDdl) {
                remark = String.format("find running ddl, ddlStmt is %s", ddlStmt);
            } else {
                remark = String.format("no find pause ddl, new ddl stmt is %s", ddlStmt);
            }

        }
        return remark;
    }

    protected boolean applyForRunning() {
        return TtlScheduledJobManager.getInstance().applyForRunning(this);
    }

    /**
     * Ttl finish running and exit with Success state
     */
    private boolean succeedExit(long scheduleId, long fireTime, String remark) {
        long finishTime = ZonedDateTime.now().toEpochSecond();
        boolean updateRs =
            ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
        TtlScheduledJobManager.getInstance().reloadTtlScheduledJobInfos();
        logTtlScheduledJob("Job finished successfully", null, false, false);
        return updateRs;
    }

    /**
     * The safeExit method is used to handle those running-job find error and failed to exec, so exit
     */
    private void errorExit(long scheduleId, long fireTime, String error) {
        ScheduledJobsManager.casState(scheduleId, fireTime, RUNNING, FAILED, null, error);
        logTtlScheduledJob(String.format("ttl scheduled job change state from running to failed"), null, true, false);
        TtlScheduledJobManager.getInstance().reloadTtlScheduledJobInfos();
    }

    /**
     * The safeExit method is used to handle those running-job is killed by cn restarting,
     * it can auto paused the ddl-job by using "pause ddl" after cn finish restarting.
     */
    @Override
    public boolean safeExit() {
        return safeExitInner("interrupted by safe exit checker");
    }

    protected boolean safeExitInner(String exitMsg) {
        final String tableSchema = executableScheduledJob.getTableSchema();
        final String tableName = executableScheduledJob.getTableName();
        final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(executableScheduledJob.getTimeZone());
        boolean exitSuccess = true;
        for (Long jobId : getCurrentDdlSqlList(tableSchema, tableName,
            new DdlState[] {DdlState.RUNNING, DdlState.QUEUED})) {
            if (jobId != null) {
                String pausedDdl = TtlTaskSqlBuilder.buildPauseDdlSql(jobId);
                logger.info(pausedDdl);
                TtlLoggerUtil.TTL_TASK_LOGGER.info(pausedDdl);
                try {
                    // pause ddl
                    executeBackgroundSql(pausedDdl, tableSchema, timeZone);
                } catch (Throwable t) {
                    logger.error(t);
                    TtlLoggerUtil.TTL_TASK_LOGGER.error(t);
                    exitSuccess = false;
                }
            }
        }
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        ScheduledJobsManager.updateState(scheduleId, fireTime, FiredScheduledJobState.INTERRUPTED,
            exitMsg, "");
        logTtlScheduledJob(
            String.format("Ttl scheduled job safe exit and change state from running to interrupted, reason is %s",
                exitMsg), null, false, true);
        TtlScheduledJobManager.getInstance().reloadTtlScheduledJobInfos();
        return exitSuccess;
    }

    /**
     * The interruptionExit is
     * used to handle running-job is still running out of maintain window,
     * and its can be actively interrupt ttl-job by calling safeExit()
     */
    private boolean interruptionExit(long scheduleId, long fireTime, String remark) {
        if (safeExitInner("interrupted by self-interruption check")) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    INTERRUPTED,
                    new String[] {
                        TTL_JOB + "," + scheduleId + "," + fireTime,
                        remark
                    },
                    NORMAL);
            return true;
        }
        return false;
    }

    public ExecutableScheduledJob getExecutableScheduledJob() {
        return executableScheduledJob;
    }

    protected void logTtlScheduledJob(String msg,
                                      Throwable ex,
                                      boolean isErr,
                                      boolean isWarn) {

        try {
            long scheduleId = executableScheduledJob.getScheduleId();
            long fireTime = executableScheduledJob.getFireTime();
            String fireTimeStr = epochSecondsToDataTimeStr(fireTime);
            String dbName = executableScheduledJob.getTableSchema();
            String tbName = executableScheduledJob.getTableName();

            String logPrefix = String.format("[%s.%s][%s,%s,%s]", dbName, tbName, scheduleId, fireTime, fireTimeStr);

            if (!isErr) {
                String logMsg = String.format("%s [msg: %s]", logPrefix, msg);
                if (isWarn) {
                    TtlLoggerUtil.TTL_TASK_LOGGER.warn(logMsg);
                    logger.warn(logMsg);
                } else {
                    TtlLoggerUtil.TTL_TASK_LOGGER.info(logMsg);
                    logger.info(logMsg);
                }

            } else {
                String logMsg = String.format("%s [msg: %s] [error: %s]", logPrefix, msg);
                if (ex != null) {
                    TtlLoggerUtil.TTL_TASK_LOGGER.error(logMsg, ex);
                    logger.error(msg, ex);
                } else {
                    TtlLoggerUtil.TTL_TASK_LOGGER.error(logMsg);
                    logger.error(msg);
                }
            }
        } catch (Throwable e) {
            logger.error(e);
        }

    }

    protected String epochSecondsToDataTimeStr(long epochSeconds) {
        Instant instant = Instant.ofEpochSecond(epochSeconds);
        ZoneId zoneId = TimeZoneUtils.zoneIdOf(executableScheduledJob.getTimeZone());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(zoneId);
        String formattedDate = formatter.format(instant);
        return formattedDate;
    }

    protected boolean checkIfScheduledTtlJobInMaintenanceWindow() {
        return InstConfUtil.isInTtlJobMaintenanceTimeWindow();
    }

}