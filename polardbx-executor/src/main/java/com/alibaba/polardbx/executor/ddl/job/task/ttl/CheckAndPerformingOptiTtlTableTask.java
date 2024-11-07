package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chenghui.lch
 */
@Getter
@TaskName(name = "CheckAndPerformingOptiTtlTableTask")
public class CheckAndPerformingOptiTtlTableTask extends AbstractTtlJobTask {

    @JSONCreator
    public CheckAndPerformingOptiTtlTableTask(String schemaName,
                                              String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        try (Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
            executeInner(metaConn, executionContext);
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }
    }

    protected void executeInner(Connection metaDbConnection, ExecutionContext executionContext) {
        /**
         * Check if arcTmp table contains ready-state partitions
         */
        TtlJobUtil.updateJobStage(this.jobContext, "OptimizingTtlTable");
        NotifyOptiTblTaskLogInfo notifyOptiTblTaskLogInfo = new NotifyOptiTblTaskLogInfo();
        TtlDefinitionInfo ttlDefinitionInfo = this.jobContext.getTtlInfo();
        String ttlTblName = ttlDefinitionInfo.getTtlInfoRecord().getTableName();
        String ttlTblSchema = ttlDefinitionInfo.getTtlInfoRecord().getTableSchema();

        notifyOptiTblTaskLogInfo.jobId = this.jobId;
        notifyOptiTblTaskLogInfo.taskId = this.taskId;
        notifyOptiTblTaskLogInfo.taskBeginTs = System.nanoTime();
        notifyOptiTblTaskLogInfo.waitOptiTblRunningBeginTs = System.nanoTime();
        notifyOptiTblTaskLogInfo.ttlTblSchema = ttlTblSchema;
        notifyOptiTblTaskLogInfo.ttlTblName = ttlTblName;

        notifyOptiTblTaskLogInfo.dataFreePercent = this.jobContext.getDataFreePercentAvgOfTtlTbl().longValue();
        notifyOptiTblTaskLogInfo.dataFreePercentLimit = TtlConfigUtil.getMaxDataFreePercentOfTtlTable();

        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = TtlScheduledJobStatManager.getInstance()
            .getTtlJobStatInfo(ttlTblSchema, ttlTblName);
        TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = TtlScheduledJobStatManager.getInstance()
            .getGlobalTtlJobStatInfo();
        notifyOptiTblTaskLogInfo.jobStatInfo = jobStatInfo;
        notifyOptiTblTaskLogInfo.globalStatInfo = globalStatInfo;

        boolean isInterrupted = executionContext.getDdlContext().isInterrupted();
        notifyOptiTblTaskLogInfo.isInterrupted = isInterrupted;
        if (isInterrupted) {

            notifyOptiTblTaskLogInfo.waitOptiTblTaskRunningTimeCost =
                System.nanoTime() - notifyOptiTblTaskLogInfo.waitOptiTblRunningBeginTs;
            logTaskExecResult(notifyOptiTblTaskLogInfo);

            /**
             * Throw an exception , so next continue ddl of curr job can be retried
             */
            throw new TtlJobRuntimeException(String.format(
                "CheckAndPerformingOptiTtlTableTask[%s.%s] is interrupted", ttlTblSchema, ttlTblName));
        }

        Long[] optiTblDdlStmtJobId = new Long[1];
        String[] optiTblDdlStmt = new String[1];
        Boolean[] optiTblDdlStmtFromTtlJob = new Boolean[1];
        boolean optimizePartitionTaskExecuting =
            CheckAndPerformingOptiTtlTableTask.checkIfOptiTblDdlExecuting(metaDbConnection,
                optiTblDdlStmtJobId, optiTblDdlStmt, optiTblDdlStmtFromTtlJob, ttlTblSchema, ttlTblName);
        notifyOptiTblTaskLogInfo.findOptiTblTaskExecuting = optimizePartitionTaskExecuting;
        if (optimizePartitionTaskExecuting) {
            notifyOptiTblTaskLogInfo.optiTblDdlStmtJobId = optiTblDdlStmtJobId[0];
            notifyOptiTblTaskLogInfo.optiTblDdlStmt = optiTblDdlStmt[0];
        }

        /**
         * Trigger optimize partition ddl
         *  <pre>
         *
         *      1. check if opti tbl task is running,
         *          if it is running,
         *              ignore
         *          else
         *              try to trigger a new optiTbl task by logical ddl by async=true;
         *              check if the ddl has been submitted successfully
         *  </pre>
         */

        /**
         * Check if one opti tbl ddl of curr ttl table is running
         */
        boolean needWaitAndMonitorOptiTblJob = false;
        if (!optimizePartitionTaskExecuting) {

            /**
             * No found any opti tbl ddl of curr ttl table is running
             */
            boolean needPerformOptiTbl = jobContext.getNeedPerformOptiTable();
            if (needPerformOptiTbl) {
                /**
                 * Submit one opti part cmd here and return
                 */
                submitOptiTableDdl(executionContext, notifyOptiTblTaskLogInfo);
                needWaitAndMonitorOptiTblJob = true;
            }
        } else {
            this.jobContext.setNewOptiTableDdlJobId(optiTblDdlStmtJobId[0]);
            /**
             * Found one opti tbl ddl of curr ttl table is running
             */
            boolean optiTblDdlFromTtlJob = optiTblDdlStmtFromTtlJob[0];
            notifyOptiTblTaskLogInfo.optiTblDdlComeFromTtlJob = optiTblDdlFromTtlJob;
            if (optiTblDdlFromTtlJob) {
                /**
                 * If the running opti tbl ddl is from ttl-job
                 */
                needWaitAndMonitorOptiTblJob = true;
            } else {
                /**
                 * If the running opti tbl ddl is NOT from ttl-job,
                 * then it will be exec by user manly, so ignore to wait and monitor
                 */
                needWaitAndMonitorOptiTblJob = false;
            }
        }

        notifyOptiTblTaskLogInfo.needWaitAndMonitorOptiTblJob = needWaitAndMonitorOptiTblJob;
        if (needWaitAndMonitorOptiTblJob) {
            /**
             * wait it complete or pause it if it is out of maintain time interval
             */
            waitAndMonitorDdlJob(metaDbConnection, executionContext,
                this.jobContext.getNewOptiTableDdlJobId(), notifyOptiTblTaskLogInfo);
        }

        notifyOptiTblTaskLogInfo.taskEndTs = System.nanoTime();
        notifyOptiTblTaskLogInfo.isFinished = true;
        logTaskExecResult(notifyOptiTblTaskLogInfo);
    }

    private void waitAndMonitorDdlJob(Connection metaDbConnection,
                                      ExecutionContext executionContext,
                                      Long jobId,
                                      NotifyOptiTblTaskLogInfo logInfo) {

        /**
         * The whole progress of wait and monitor opti-tbl ddl job
         * <pre>
         *     The ddl job of opti tbl has the following state need handling:
         *          (1) the job is running or pending:
         *                  ttl-job should wait it to be completed
         *              or  ttl-job pause it if ttl-job is interrupted or out of maintain interval;
         *
         *          (2) the job is paused:
         *                  ttl-job try to recover it if ttl-job is in maintain interval;
         *
         *          (3) the job is completed and no found in ddl_engine table:
         *                  ttl-job ignore and return
         *          (4) other state:
         *                  ttl-job ignore and return
         *
         * </pre>
         *
         */

        /**
         * Fetch the opti-tbl job by jobid
         */
        String ttlSchemaName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        DdlEngineRecord currentJobRec = null;
        while (true) {

            logInfo.waitOptiTblTaskRunningLoopRound++;

            /**
             * Label if allowed controlling opti tbl job
             */
            boolean enableAutoCtrlOptiTblJob = TtlConfigUtil.isEnableAutoControlOptiTblByTtlJob();

            try {
                currentJobRec = fetchLatestDdlJobRecByJobId(metaDbConnection, jobId);
                DdlState currentState = null;
                int jobProgress = 0;
                if (currentJobRec != null) {
                    currentState = DdlState.valueOf(currentJobRec.state);
                    jobProgress = currentJobRec.progress;
                    logInfo.optiJobProgress = jobProgress;
                }
                if (currentState == null) {
                    /**
                     * No found the opti tbl job by the jobid,
                     * maybe the job is completed and no found in ddl_engine table,
                     * so ignore
                     */
                    logInfo.optiTblDdlJobStateBeforeCtrl = "";
                    break;
                } else {
                    logInfo.optiTblDdlJobStateBeforeCtrl = currentState.name();

                    if (currentState == DdlState.RUNNING || currentState == DdlState.QUEUED) {
                        /**
                         * Wait it to completed, so ignore
                         */
                    } else if (currentState == DdlState.PAUSED) {
                        /**
                         * try to recover
                         */
                        recoverOptiTblJob(metaDbConnection, ttlSchemaName, jobId, executionContext,
                            enableAutoCtrlOptiTblJob, logInfo);
                    } else {
                        /**
                         * Ignore and return
                         */
                        break;
                    }
                }

                boolean currTtlJobOutOfMaintainInterval = !checkIfTtlJobInMaintainTimeWindow(metaDbConnection, jobId);
                boolean isInterrupted = executionContext.getDdlContext().isInterrupted();
                logInfo.isInterrupted = isInterrupted;
                logInfo.isMaintenancePeriodOver = currTtlJobOutOfMaintainInterval;

                if (isInterrupted || currTtlJobOutOfMaintainInterval) {
                    pauseOptiTblJob(metaDbConnection, ttlSchemaName, jobId, executionContext, enableAutoCtrlOptiTblJob,
                        logInfo);
                    break;
                }

                /**
                 * Wait 5000 ms each round
                 */
                Thread.sleep(TtlConfigUtil.intraTaskDelegateEachRoundWaitTime);

            } catch (Throwable ex) {
                if (ex instanceof TtlJobRuntimeException) {
                    throw (TtlJobRuntimeException) ex;
                } else {
                    throw new TtlJobRuntimeException(ex);
                }
            }

            /**
             * continue and wait the oss archiving ddl finishing, sleep
             */
            try {
                logInfo.waitOptiTblTaskRunningTimeCost = System.nanoTime() - logInfo.waitOptiTblRunningBeginTs;
                logTaskExecResult(logInfo);
                long sleepTimeMills = TtlConfigUtil.intraTaskDelegateEachRoundWaitTime;
                Thread.sleep(sleepTimeMills);
            } catch (Throwable ex) {
                // ignore
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }
        }

        try {
            /**
             * Fetch the latest progress of optimize table
             */
            currentJobRec = fetchLatestDdlJobRecByJobId(metaDbConnection, jobId);
            if (currentJobRec != null) {
                logInfo.optiJobProgress = currentJobRec.progress;
            } else {
                logInfo.optiJobProgress = 100;
            }
        } catch (Throwable ex) {
            // ignore
        }
    }

    protected boolean checkIfTtlJobInMaintainTimeWindow(Connection metaDbConn, Long jobId) {
        return TtlJobUtil.checkIfInTtlMaintainWindow();
    }

    protected boolean recoverOptiTblJob(Connection metaDbConn,
                                        String jobSchema,
                                        Long jobId,
                                        ExecutionContext executionContext,
                                        boolean enableAutoCtrlOptiTblJob,
                                        NotifyOptiTblTaskLogInfo logInfo) {

        if (!enableAutoCtrlOptiTblJob) {
            return false;
        }

        DdlState latestJobState = fetchLatestDdlJobStateByJobId(metaDbConn, jobId);
        if (latestJobState == null) {
            /**
             * Maybe curr ddl job has been completed.
             */
            logInfo.noFoundOptiTblJobRec = true;
            return true;
        }

        if (latestJobState == DdlState.PAUSED) {

            /**
             * Send a cmd of continue ddl #job_id to recover the job
             */
            int optiTblParallelism = TtlConfigUtil.getOptimizePartitionParallelism();
            String continueDdlSql = TtlTaskSqlBuilder.buildAsyncContinueDdlSql(jobId, optiTblParallelism);
            submitDdlJobControlCmd(executionContext, jobSchema, continueDdlSql);

            /**
             * Make sure the ddl job has been become the ddl-state of running
             */
            logInfo.continueOptiTblSql = continueDdlSql;
            waitRecoverDdlDoneByJobId(metaDbConn, jobId, DdlState.RUNNING, executionContext, logInfo);
            return true;
        } else {
            /**
             * For other ddl-states,just ignore
             */
            return false;
        }
    }

    protected boolean pauseOptiTblJob(Connection metaDbConn,
                                      String jobSchema,
                                      Long jobId,
                                      ExecutionContext executionContext,
                                      boolean enableAutoCtrlOptiTblJob,
                                      NotifyOptiTblTaskLogInfo logInfo) {

        if (!enableAutoCtrlOptiTblJob) {
            return false;
        }

        DdlState latestJobState = fetchLatestDdlJobStateByJobId(metaDbConn, jobId);
        if (latestJobState == null) {
            /**
             * Maybe curr ddl job has been complated.
             */
            logInfo.noFoundOptiTblJobRec = true;
            return true;
        }

        if (latestJobState == DdlState.RUNNING || latestJobState == DdlState.QUEUED) {

            /**
             * Send a cmd of pause ddl #job_id to pause the job
             */
            String pauseDdlSql = TtlTaskSqlBuilder.buildPauseDdlSql(jobId);
            long waitPauseDdlBeginTsNano = System.nanoTime();
            submitDdlJobControlCmd(executionContext, jobSchema, pauseDdlSql);
            logInfo.pauseOptiTblSql = pauseDdlSql;
            logInfo.waitFromRunningToPauseTimeCost = System.nanoTime() - waitPauseDdlBeginTsNano;
            return true;

        } else {
            /**
             * For other ddl-states,just ignore
             */
        }

        return false;
    }

    protected DdlState fetchLatestDdlJobStateByJobId(Connection metaDbConnection, Long jobId) {

        DdlState ddlState = null;
        DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
        ddlEngineAccessor.setConnection(metaDbConnection);
        DdlEngineRecord ddlJobRec = ddlEngineAccessor.query(jobId);
        if (ddlJobRec == null) {
            /**
             * No found the opti tbl job by the jobid,
             * maybe the job is completed and no found in ddl_engine table,
             * so ignore
             */
        } else {
            ddlState = DdlState.valueOf(ddlJobRec.state);
        }
        return ddlState;
    }

    protected DdlEngineRecord fetchLatestDdlJobRecByJobId(Connection metaDbConnection, Long jobId) {
        DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
        ddlEngineAccessor.setConnection(metaDbConnection);
        DdlEngineRecord ddlJobRec = ddlEngineAccessor.query(jobId);
        return ddlJobRec;
    }

    protected boolean waitRecoverDdlDoneByJobId(Connection metaDbConnection,
                                                Long jobId,
                                                DdlState taretDdlState,
                                                ExecutionContext ec,
                                                NotifyOptiTblTaskLogInfo logInfo) {

        int waitFinishRecoveringRound = 0;
        long waitRecoverBeginTsNano = System.nanoTime();
        long waitRecoverEndTsNano = waitRecoverBeginTsNano;
        boolean failedToRecoverDdlJob = false;
        while (true) {
            boolean isInterrupted = ec.getDdlContext().isInterrupted();
            DdlState latestDdlState = fetchLatestDdlJobStateByJobId(metaDbConnection, jobId);
            if (latestDdlState != null) {
                if (latestDdlState == taretDdlState) {
                    break;
                }

                if (isInterrupted) {
                    break;
                }

                try {
                    Thread.sleep(TtlConfigUtil.getIntraTaskDelegateEachRoundWaitTime());
                    waitFinishRecoveringRound++;
                } catch (Throwable ex) {
                    // ignore
                } finally {
                    waitRecoverEndTsNano = System.nanoTime();
                }

                long totalWaitTimeNano = waitRecoverEndTsNano - waitRecoverBeginTsNano;
                long totalWaitTimeMs = (waitRecoverEndTsNano - waitRecoverBeginTsNano) / 1000000;
                if (totalWaitTimeMs > TtlConfigUtil.getMaxWaitTimeForDdlJobFromPauseToRunning()) {
                    failedToRecoverDdlJob = true;
                    logInfo.failedToRecoverDdlJob = failedToRecoverDdlJob;
                    break;
                }

                logInfo.waitFromPauseToRunningRound = waitFinishRecoveringRound;
                logInfo.waitFromPauseToRunningTimeCost = totalWaitTimeNano;
                logTaskExecResult(logInfo);
            } else {
                logInfo.noFoundOptiTblJobRec = true;
                break;
            }
        }

        logInfo.isInterrupted = true;
        logInfo.waitFromPauseToRunningRound = waitFinishRecoveringRound;
        logInfo.waitFromPauseToRunningTimeCost = System.nanoTime() - waitRecoverBeginTsNano;
        logTaskExecResult(logInfo);

        if (failedToRecoverDdlJob) {
            /**
             * Throw an exception , so next continue ddl of curr job can be retried
             */
            throw new TtlJobRuntimeException(
                String.format("Failed to recover optimize table ddlJob[%s] for %s.%s", jobId, logInfo.ttlTblSchema,
                    logInfo.ttlTblName));
        }

        return true;
    }

    public static boolean checkIfOptiTblDdlExecuting(Connection metaDbConnection,
                                                     Long[] ddlStmtJobIdOutput,
                                                     String[] ddlStmtOutput,
                                                     Boolean[] ddlStmtFromTtlJob,
                                                     String ttlTableSchema,
                                                     String ttlTableName) {
        /**
         * Trigger archive ddl
         *  <pre>
         *      1. check if optimize partition task is running,
         *          if it is running,
         *              ignore
         *          else
         *              try to trigger a new archiving task by logical ddl by async=true;
         *              check if the ddl has been submitted successfully
         *  </pre>
         */
        boolean opptimizePartitionTaskRunning = false;
        String tblName = ttlTableName;
        String tblSchema = ttlTableSchema;
        try {
            DdlEngineAccessor ddlEngineAccessor = new DdlEngineAccessor();
            ddlEngineAccessor.setConnection(metaDbConnection);
            List<DdlEngineRecord> jobList = ddlEngineAccessor.query(tblSchema, tblName);
            List<DdlEngineRecord> runningOptiPartJobList = new ArrayList<>();
            for (int i = 0; i < jobList.size(); i++) {
                DdlEngineRecord ddlEngineRecord = jobList.get(i);
                if (ddlEngineRecord.state.equalsIgnoreCase("COMPLETED")) {
                    continue;
                }
                if (ddlEngineRecord.ddlStmt.toUpperCase().contains("OPTIMIZE PARTITION")) {
                    runningOptiPartJobList.add(ddlEngineRecord);
                }
                if (ddlEngineRecord.ddlStmt.toUpperCase().contains("OPTIMIZE TABLE")) {
                    runningOptiPartJobList.add(ddlEngineRecord);
                }
            }
            if (!runningOptiPartJobList.isEmpty()) {
                Long jobId = runningOptiPartJobList.get(0).getJobId();
                String ddlStmt = runningOptiPartJobList.get(0).ddlStmt;
                if (ddlStmtJobIdOutput != null && ddlStmtJobIdOutput.length > 0) {
                    ddlStmtJobIdOutput[0] = jobId;
                }
                if (ddlStmtOutput != null && ddlStmtOutput.length > 0) {
                    ddlStmtOutput[0] = ddlStmt;
                    if (ddlStmtFromTtlJob != null) {
                        if (ddlStmt.toLowerCase().contains(TtlTaskSqlBuilder.DDL_FLAG_OF_TTL_JOB)) {
                            ddlStmtFromTtlJob[0] = true;
                        } else {
                            ddlStmtFromTtlJob[0] = false;
                        }
                    }
                }
                opptimizePartitionTaskRunning = true;
            }
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }

        return opptimizePartitionTaskRunning;
    }

    protected void submitOptiTableDdl(ExecutionContext executionContext,
                                      NotifyOptiTblTaskLogInfo logInfo) {

        TtlDefinitionInfo ttlDefinitionInfo = this.jobContext.getTtlInfo();
        String ttlTblName = ttlDefinitionInfo.getTtlInfoRecord().getTableName();
        String ttlTblSchema = ttlDefinitionInfo.getTtlInfoRecord().getTableSchema();

        String ttlTimezoneStr = this.jobContext.getTtlInfo().getTtlInfoRecord().getTtlTimezone();
        String charsetEncoding = TtlConfigUtil.defaultCharsetEncodingOnTransConn;
        String sqlModeSetting = TtlConfigUtil.defaultSqlModeOnTransConn;
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        sessionVariables.put("time_zone", ttlTimezoneStr);
        sessionVariables.put("names", charsetEncoding);
        sessionVariables.put("sql_mode", sqlModeSetting);

        String queryHintOfOptiTbl = TtlConfigUtil.getQueryHintForOptimizeTable();
        String optiTblSql = TtlTaskSqlBuilder.buildAsyncOptimizeTableSql(ttlTblSchema, ttlTblName, queryHintOfOptiTbl);
        AtomicLong newSubmitOptiTblJobId = new AtomicLong(0L);
        final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
        TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            ttlTblSchema,
            sessionVariables,
            (transConn) -> {
                List<Map<String, Object>> rsInfo = TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                    ttlTblSchema,
                    transConn,
                    executionContext,
                    optiTblSql);
                Long jobId = (Long) rsInfo.get(0).get(TtlTaskSqlBuilder.COL_NAME_FOR_DDL_JOB_ID);
                newSubmitOptiTblJobId.set(jobId);
                return jobId;
            }
        );
        this.jobContext.setNewOptiTableDdlJobId(newSubmitOptiTblJobId.get());
        logInfo.optiTblDdlStmtJobId = newSubmitOptiTblJobId.get();
        logInfo.newSubmittedOptiTblSql = optiTblSql;
    }

    /**
     * Submit a cmd of controlling ddl job
     */
    protected void submitDdlJobControlCmd(ExecutionContext executionContext,
                                          String jobSchema,
                                          String cmdSql) {

        String ttlTimezoneStr = this.jobContext.getTtlInfo().getTtlInfoRecord().getTtlTimezone();
        String charsetEncoding = TtlConfigUtil.defaultCharsetEncodingOnTransConn;
        String sqlModeSetting = TtlConfigUtil.defaultSqlModeOnTransConn;
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        sessionVariables.put("time_zone", ttlTimezoneStr);
        sessionVariables.put("names", charsetEncoding);
        sessionVariables.put("sql_mode", sqlModeSetting);
        final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
        TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            jobSchema,
            sessionVariables,
            (transConn) -> {
                int arows = TtlJobUtil.execLogicalDmlOnInnerConnection(serverConfigManager,
                    jobSchema,
                    transConn,
                    executionContext,
                    cmdSql);
                return arows;
            }
        );

    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        super.beforeRollbackTransaction(executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringRollbackTransaction(metaDbConnection, executionContext);
    }

    protected void fetchTtlJobContextFromPreviousTask() {
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(),
            PrepareCleanupIntervalTask.class,
            getSchemaName(),
            getLogicalTableName()
        );
        this.jobContext = jobContext;
    }

    protected static class NotifyOptiTblTaskLogInfo {
        public NotifyOptiTblTaskLogInfo() {
        }

        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo;
        TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo;

        protected Long jobId;
        protected Long taskId;
        protected Long taskBeginTs;
        protected Long taskEndTs;

        protected String ttlTblSchema;
        protected String ttlTblName;
        protected long dataFreePercent;
        protected long dataFreePercentLimit;

        protected boolean isFinished = false;
        protected boolean isInterrupted = false;
        protected boolean isMaintenancePeriodOver = false;

        protected boolean enableAutoCtrlOptiTblJob = true;
        protected boolean needWaitAndMonitorOptiTblJob = false;
        protected long waitOptiTblRunningBeginTs = 0;
        protected long waitOptiTblTaskRunningTimeCost = 0;
        protected long waitOptiTblTaskRunningLoopRound = 0;

        protected boolean findOptiTblTaskExecuting = false;
        protected boolean optiTblDdlComeFromTtlJob = false;
        protected long optiTblDdlStmtJobId = 0;
        protected String optiTblDdlStmt = "";
        protected String newSubmittedOptiTblSql = "";
        protected String optiTblDdlJobStateBeforeCtrl = "";

        protected boolean noFoundOptiTblJobRec = false;
        protected String continueOptiTblSql = "none";
        protected boolean failedToRecoverDdlJob = false;
        protected int waitFromPauseToRunningRound = 0;
        protected long waitFromPauseToRunningTimeCost = 0;

        protected String pauseOptiTblSql = "none";
        protected long waitFromRunningToPauseTimeCost = 0;
        protected int optiJobProgress = 0;

    }

    protected void logTaskExecResult(NotifyOptiTblTaskLogInfo logInfo) {

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

            TtlLoggerUtil.logTaskMsg(this, this.jobContext, logMsg);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
        }
    }
}
