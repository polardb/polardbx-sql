package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Getter;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Future;

/**
 * @author chenghui.lch
 */
@Getter
@TaskName(name = "PrepareCleanupIntervalTask")
public class PrepareCleanupIntervalTask extends AbstractTtlJobTask {

    /**
     * <pre>
     * The job of PrepareCleanupIntervalTask are the following:
     *  1. decide the the time interval of the expired data to be cleared in this job;
     *  2. check if need do archiving, then make sure that
     *      the the target time interval of the expired data to be cleared
     *      are included by range partitions of ttl_tmp table
     *     or add new range partition for ttl_tmp table if find
     *     the target time interval of the expired data are NOT included.
     * </pre>
     */

    @JSONCreator
    public PrepareCleanupIntervalTask(String schemaName,
                                      String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenPause();
    }

    public void executeImpl(ExecutionContext executionContext) {
        resetTtlJobStat();
        TtlJobUtil.updateJobStage(this.jobContext, "SelectingForCleanupBound");
        prepareExpiredCLeanUpBound(executionContext);
    }

    protected void resetTtlJobStat() {
        String ttlDb = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String ttlTb = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo =
            TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(ttlDb, ttlTb);
        if (jobStatInfo != null) {
            jobStatInfo.resetFinishedJobStatInfo();
            jobStatInfo.setCurrJobBeginTs(System.currentTimeMillis());
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        executeImpl(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);
    }

    protected static class FetchMinCleanUpBoundTaskSubmitter implements TtlWorkerTaskSubmitter {
        protected DdlTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext ttlJobContext;

        public FetchMinCleanUpBoundTaskSubmitter(DdlTask parentDdlTask, ExecutionContext ec,
                                                 TtlJobContext ttlJobContext) {
            this.parentDdlTask = parentDdlTask;
            this.ec = ec;
            this.ttlJobContext = ttlJobContext;
        }

        @Override
        public List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks() {
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos = new ArrayList<>();
            FetchExpiredDataUpperBoundTask task = new FetchExpiredDataUpperBoundTask(parentDdlTask, ec, ttlJobContext);
            List<TtlIntraTaskRunner> runners = new ArrayList<>();
            runners.add(task);
            futureInfos = TtlIntraTaskExecutor.getInstance().submitSelectTaskRunners(runners);
            return futureInfos;
        }
    }

    protected static class FetchExpiredDataUpperBoundTask extends TtlIntraTaskRunner {

        protected DdlTask parentDdlTask;
        protected TtlJobContext jobContext;
        protected ExecutionContext ec;
        protected Object transConn;
        protected String currentDatetime = null;
        protected String formatedCurrentDatetime = null;
        protected String expiredUpperBound = null;
        protected String expiredLowerBound = null;
        protected String ttlColMinValue = null;
        protected Boolean ttlColMinValueIsNull = false;
        protected Boolean ttlColMinValueIsZero = false;
        protected Boolean ttlTblIsEmpty = false;
        protected String minBoundToBeCleanUp = null;
        protected boolean stopTask = false;
        protected IntraTaskStatInfo statInfo = new IntraTaskStatInfo();

        public FetchExpiredDataUpperBoundTask(DdlTask parentDdlTask,
                                              ExecutionContext ec,
                                              TtlJobContext jobContext) {
            this.parentDdlTask = parentDdlTask;
            this.jobContext = jobContext;
            this.ec = ec;
        }

        @Override
        public void runTask() {
            final Map savedMdcContext = MDC.getCopyOfContextMap();
            try {
                String schemaName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema().toLowerCase();
                MDC.put(MDC.MDC_KEY_APP, schemaName);
                runInner();
            } finally {
                MDC.setContextMap(savedMdcContext);
            }
        }

        protected void runInner() {

            long taskStartTsNano = System.nanoTime();
            String logMsgHeader = TtlLoggerUtil.buildIntraTaskLogMsgHeader(parentDdlTask, jobContext);

            final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
            final String ttlTblSchemaName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
            boolean needPerformArchivingByOssTbl =
                this.jobContext.getTtlInfo().needPerformExpiredDataArchivingByOssTbl();

            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
            String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
            String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
            String groupParallelismForConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDqlConn());
            Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            sessionVariables.put("time_zone", ttlTimezoneStr);
            sessionVariables.put("names", charsetEncoding);
            sessionVariables.put("sql_mode", sqlModeSetting);
            sessionVariables.put("group_parallelism", groupParallelismForConnStr);

            TtlJobUtil.wrapWithDistributedTrx(
                serverConfigManager,
                ttlTblSchemaName,
                sessionVariables,
                (transConn) -> {

                    /**
                     * Save the reference of transConn on IntraTask to use to do force closing
                     */
                    this.transConn = transConn;

                    /**
                     * Select the lower bound value of ttl_col of ttl_tbl
                     * <pre>
                     * SELECT DATE_FORMAT(ttl_col,formatter) AS expired_lower_bound
                     *  FROM (SELECT %s as ttl_col
                     *         FROM [db_name.]ttl_tbl [FORCE INDEX(xxx_ttl_col_idx)]
                     *         ORDER BY %s ASC
                     *         LIMIT 1
                     *       ) as ttl_tbl
                     * </pre>
                     *
                     */
                    final String selectExpiredLowerBoundSql = buildSelectExpiredLowerBoundSql();
                    long selectLowerBoundStartTsNano = System.nanoTime();
                    List<Map<String, Object>> lowerBoundResult =
                        TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                            ttlTblSchemaName,
                            transConn,
                            ec,
                            selectExpiredLowerBoundSql);
                    long selectLowerBoundEndTsNano = System.nanoTime();
                    this.statInfo.getTotalSelectTimeCostNano()
                        .addAndGet(selectLowerBoundEndTsNano - selectLowerBoundStartTsNano);

                    if (lowerBoundResult == null || lowerBoundResult.isEmpty()) {
                        this.ttlTblIsEmpty = true;
                        /**
                         *  ttl_tbl has no data
                         */
                        this.expiredLowerBound = null;
                        this.ttlColMinValueIsNull = false;
                        this.ttlColMinValueIsZero = false;
                    } else {
                        this.ttlTblIsEmpty = false;
                        String minValIsNull = fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE_IS_NULL);
                        String minValIsZero = fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE_IS_ZERO);
                        String expiredLowerBoundStr = fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_LOWER_BOUND);
                        String minValueStr = fetchStringFromQueryValue(lowerBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_MIN_VALUE);

                        this.ttlColMinValueIsNull = minValIsNull.equalsIgnoreCase("1");
                        this.ttlColMinValueIsZero = ttlColMinValueIsNull ? false : minValIsZero.equalsIgnoreCase("1");
                        this.expiredLowerBound = expiredLowerBoundStr;
                        this.ttlColMinValue = minValueStr;

                    }

                    /**
                     * Select the round-downed upper bound of expired value of ttl_col
                     * <pre>
                     * set TIME_ZONE='xxx';
                     * SELECT DATE_FORMAT( DATE_SUB(NOW(), INTERVAL %s %s), formatter ) expired_upper_bound;
                     * formatter is like %Y-%m-%d 00:00:00.000000
                     * </pre>
                     *
                     */
                    final String selectExpiredUpperBoundSql = buildSelectExpiredUpperBoundSql();
                    List<Map<String, Object>> upperBoundResult =
                        TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                            ttlTblSchemaName,
                            transConn,
                            ec,
                            selectExpiredUpperBoundSql);

                    if (upperBoundResult == null || upperBoundResult.isEmpty()) {
                        /**
                         *  ttl_tbl has no data
                         */
                        this.expiredUpperBound = null;
                    } else {
                        String currentDatetime = fetchStringFromQueryValue(upperBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_CURRENT_DATETIME);
                        String formatedCurrentDatetime = fetchStringFromQueryValue(upperBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_FORMATED_CURRENT_DATETIME);
                        String expiredUpperBoundStr =
                            fetchStringFromQueryValue(upperBoundResult,
                                TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_UPPER_BOUND);
                        this.expiredUpperBound = expiredUpperBoundStr;
                        this.currentDatetime = currentDatetime;
                        this.formatedCurrentDatetime = formatedCurrentDatetime;
                    }

                    return 0;
                }
            );

            /**
             * <pre>
             *     Compute and the Min Bound to Be Cleanup
             * </pre>
             */
            minBoundToBeCleanUp = decideExpiredLowerBound();
            this.jobContext.setCurrentDateTime(currentDatetime);
            this.jobContext.setFormatedCurrentDateTime(formatedCurrentDatetime);
            this.jobContext.setCleanUpUpperBound(expiredUpperBound);
            this.jobContext.setCleanUpLowerBound(minBoundToBeCleanUp);
            this.jobContext.setTtlColMinValue(ttlColMinValue);
            this.jobContext.setTtlColMinValueIsNull(ttlColMinValueIsNull);
            this.jobContext.setTtlColMinValueIsZero(ttlColMinValueIsZero);
            this.jobContext.setTtlTblIsEmpty(ttlTblIsEmpty);

            long arcTmpTblDataLength = 0;
            if (needPerformArchivingByOssTbl) {
                // Fetch the arc-tmp tbl data-length
                arcTmpTblDataLength = TtlJobUtil.fetchArcTmlTableDataLength(ec, this.jobContext, null);
            }
            this.jobContext.setArcTmpDataLengthBeforeRunning(arcTmpTblDataLength);

            TtlTblFullDataFreeInfo[] ttlTblFullDfInfoOutput = new TtlTblFullDataFreeInfo[1];
            try {
                long maxTtlTblDfPercent = TtlConfigUtil.getMaxDataFreePercentOfTtlTable();
                TtlJobUtil.fetchTtlTableDataFreeStat(this.ec, this.jobContext, ttlTblFullDfInfoOutput);
                if (ttlTblFullDfInfoOutput[0] != null) {
                    TtlTblFullDataFreeInfo ttlTblFullDfInfo = ttlTblFullDfInfoOutput[0];
                    BigDecimal primDfPercentDec = ttlTblFullDfInfo.getPrimDataFreeInfo().getDataFreePercent();
                    long dfPercentAvg = ttlTblFullDfInfo.getDataFreePercentAvg();
                    long rowLenAvg = ttlTblFullDfInfo.getRowDataLengthAvg();
                    this.jobContext.setTtlTblPrimDataLength(
                        ttlTblFullDfInfo.getPrimDataFreeInfo().getDataLength().get());
                    this.jobContext.setDataFreeOfTtlTblPrim(
                        ttlTblFullDfInfo.getPrimDataFreeInfo().getDataFree().get());
                    this.jobContext.setDataFreePercentOfTtlTblPrim(primDfPercentDec);
                    this.jobContext.setDataFreePercentAvgOfTtlTbl(dfPercentAvg);
                    this.jobContext.setRowLengthAvgOfTtlTbl(rowLenAvg);

                    if (TtlConfigUtil.isEnableAutoControlOptiTblByTtlJob()) {
                        BigDecimal maxTtlTblDfPercentDec = new BigDecimal(maxTtlTblDfPercent);
                        if (TtlJobUtil.checkNeedPerformOptiTblForTtlTbl(primDfPercentDec, maxTtlTblDfPercentDec)) {
                            this.jobContext.setNeedPerformOptiTable(true);
                        }
                    }
                }
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }

            long taskEndTsNano = System.nanoTime();
            this.statInfo.getTotalExecTimeCostNano().addAndGet(taskEndTsNano - taskStartTsNano);

            logTaskExecResult(logMsgHeader, jobContext, statInfo);

        }

        private String fetchStringFromQueryValue(List<Map<String, Object>> boundResult, String colName) {
            String expiredLowerBoundStr = null;
            Object expiredLowerBoundObj = boundResult.get(0).get(colName);
            if (expiredLowerBoundObj instanceof io.airlift.slice.Slice) {
                //expiredLowerBoundStr = ((Slice) expiredLowerBoundObj).toString();
                byte[] valBytes = ((io.airlift.slice.Slice) expiredLowerBoundObj).getBytes();
                try {
                    expiredLowerBoundStr = new String(valBytes, "utf8");
                } catch (UnsupportedEncodingException e) {
                    throw new TtlJobRuntimeException(e);
                }
            } else if (expiredLowerBoundObj instanceof String) {
                expiredLowerBoundStr = (String) expiredLowerBoundObj;
            } else if (expiredLowerBoundObj instanceof OriginalTimestamp) {
                expiredLowerBoundStr =
                    ((OriginalTimestamp) expiredLowerBoundObj).getMysqlDateTime().toDatetimeString(0);
            } else if (expiredLowerBoundObj instanceof Long) {
                expiredLowerBoundStr = String.valueOf(expiredLowerBoundObj);
            }

            return expiredLowerBoundStr;
        }

        protected void logTaskExecResult(
            String logMsgHeader,
            TtlJobContext jobContext,
            IntraTaskStatInfo statInfo) {

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
                this.jobContext.getDataFreeOfTtlTblPrim(), this.jobContext.getTtlTblPrimDataLength(),
                this.jobContext.getRowLengthAvgOfTtlTbl());

            logMsg += msgPrefix;
            logMsg += String.format("needPerformOptiTbl: %s, dfPercent: %s, dfPercentAvg: %s\n",
                this.jobContext.getNeedPerformOptiTable(), this.jobContext.getDataFreePercentOfTtlTblPrim(),
                this.jobContext.getDataFreePercentAvgOfTtlTbl());

            logMsg += msgPrefix;
            logMsg += String.format("dmlBatchSize: %s\n", jobContext.getDmlBatchSize());

            logMsg += msgPrefix;
            logMsg += String.format("selectTimeCost(ms): %s\n", selectTimeCostMills);

            logMsg += msgPrefix;
            logMsg += String.format("taskTimeCot(ms): %s\n", taskTimeCostMills);

            TtlLoggerUtil.logTaskMsg(parentDdlTask, this.jobContext, logMsg);

        }

        protected String decideExpiredLowerBound() {

            String lowerBoundStr = this.expiredLowerBound;
            String upperBoundStr = this.expiredUpperBound;
            String minBoundToBeCleanupStr = upperBoundStr;

            try {
                DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
                TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
                boolean ttlTblIsEmptyVal = this.ttlTblIsEmpty;
                boolean ttlColMinValIsNullVal = this.ttlColMinValueIsNull;
                boolean ttlColMinValueIsZeroVal = this.ttlColMinValueIsZero;
                TtlTimeUnit ttlUnitVal = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getTtlUnit());

                if (StringUtils.isEmpty(lowerBoundStr) || ttlColMinValIsNullVal) {
                    if (ttlTblIsEmptyVal) {
                        /**
                         * ttl_tbl has no data
                         */
                        return minBoundToBeCleanupStr;
                    }
                }

                if (ttlColMinValIsNullVal || ttlColMinValueIsZeroVal) {
                    lowerBoundStr = TtlTaskSqlBuilder.TTL_COL_MIN_VAL_DEFAULT_LOWER_BOUND;
                }

                /**
                 * Convert upperBoundStr of ISO_LOCAL_DATE_TIME-format to DateTime
                 */
                LocalDateTime upperBoundDt = LocalDateTime.parse(upperBoundStr, formatter);

                /**
                 * Convert lowerBoundStr of ISO_LOCAL_DATE_TIME-format to DateTime
                 */
                LocalDateTime lowerBoundDt = LocalDateTime.parse(lowerBoundStr, formatter);

                int oneUnitStep = TtlConfigUtil.getTtlCleanupBoundIntervalCount();
                LocalDateTime minBoundToBeCleanup =
                    TtlJobUtil.plusDeltaIntervals(lowerBoundDt, ttlUnitVal, oneUnitStep);

                int cmpRs = minBoundToBeCleanup.compareTo(upperBoundDt);
                if (cmpRs >= 0) {
                    /**
                     * The minBoundToBeCleanup is NOT less than upperBound,
                     * use upperBound as the minBound that is to be cleanup
                     */
                } else {
                    /**
                     * The minBoundToBeCleanup is less than upperBound,
                     */
                    minBoundToBeCleanupStr = minBoundToBeCleanup.format(formatter);
                }

            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
                throw ex;
            }
            return minBoundToBeCleanupStr;
        }

        protected String buildSelectExpiredLowerBoundSql() {
            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            boolean useMergeConcurrent = TtlConfigUtil.isUseMergeConcurrentForSelectLowerBound();
            int mergeUnionSize = TtlConfigUtil.getMergeUnionSizeForSelectLowerBound();
            String queryHint = TtlConfigUtil.getQueryHintForSelectLowerBound();
            String forceIndexExpr = this.jobContext.getTtlColForceIndexExpr();
            String selectSql =
                TtlTaskSqlBuilder.buildSelectExpiredLowerBoundValueSqlTemplate(ttlInfo, ec, queryHint, forceIndexExpr);
            if (!useMergeConcurrent) {
                selectSql =
                    TtlTaskSqlBuilder.buildSelectExpiredLowerBoundValueBySqlTemplateWithoutConcurrent(ttlInfo, ec,
                        mergeUnionSize, forceIndexExpr);
            }
            return selectSql;
        }

        protected String buildSelectExpiredUpperBoundSql() {
            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            String selectSql = TtlTaskSqlBuilder.buildSelectExpiredUpperBoundValueSqlTemplate(ttlInfo, ec);
            return selectSql;
        }

        @Override
        public String getDnId() {
            return "";
        }

        @Override
        public void notifyStopTask() {
            this.stopTask = true;
        }

        @Override
        public void forceStopTask() {
            this.stopTask = true;
            if (transConn != null) {
                IServerConfigManager serverMgr = TtlJobUtil.getServerConfigManager();
                try {
                    serverMgr.closeTransConnection(transConn);
                } catch (Throwable ex) {
                    // ignore ex
                    TtlLoggerUtil.TTL_TASK_LOGGER.info(ex);
                }
            }
        }
    }

    protected void prepareExpiredCLeanUpBound(ExecutionContext ec) {

        /**
         * Find and build force index expr for ttl col
         */
        initForceIndexExprForTtlCol(ec);

        /**
         * Fetch min bound of expired data to be cleanup
         */
        FetchMinCleanUpBoundTaskSubmitter fetchBoundSubmitter =
            new FetchMinCleanUpBoundTaskSubmitter(this, ec, this.jobContext);
        TtlIntraTaskManager fetchBoundDelegate =
            new TtlIntraTaskManager(this, ec, this.jobContext, fetchBoundSubmitter);
        fetchBoundDelegate.submitAndRunIntraTasks();
    }

    protected void initForceIndexExprForTtlCol(ExecutionContext ec) {
        String forceIndexExpr = TtlJobUtil.buildForceLocalIndexExprForTtlCol(this.jobContext.getTtlInfo(), ec);
        this.jobContext.setTtlColForceIndexExpr(forceIndexExpr);
    }

}