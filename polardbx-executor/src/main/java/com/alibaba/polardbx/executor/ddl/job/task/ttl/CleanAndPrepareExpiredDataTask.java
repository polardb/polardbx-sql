package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobInterruptedException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.CleanupExpiredDataLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartCondExprRouter;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Data;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chenghui.lch
 */
@Getter
@TaskName(name = "CleanAndPrepareExpiredDataTask")
public class CleanAndPrepareExpiredDataTask extends AbstractTtlJobTask {
    private static final Logger log = LoggerFactory.getLogger(CleanAndPrepareExpiredDataTask.class);

    /**
     * <pre>
     * The job of ClearAndPrepareExpiredDataTask are the following:
     * for need archiving:
     *      delete from ttl_tbl only
     * for need NOT archiving:
     *      insert ttl_tmp select * from ttl_tbl
     *      delete from ttl_tbl;
     * </pre>
     */

    /**
     * The set of the phy part that  has been finish expired data cleaning
     * <pre>
     *      for auto-db part table:
     *          each item is the global partNum of a partition tbl
     *      for drds-db part table:
     *          each item is a global partNum of sharding tbl that is computed by posi of phydb nd posi phytb
     * </pre>
     */
    protected volatile Set<Integer> cleanedPhyPartSet = new HashSet<>();

    public CleanAndPrepareExpiredDataTask(String schemaName,
                                          String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenPause();
    }

    protected void initCleanedPartMarkSet(ExecutionContext ec) {

        /**
         * Try to check if cleanedPartBitSetStr has been init
         */
        if (this.cleanedPhyPartSet == null || this.cleanedPhyPartSet.isEmpty()) {
            storeCleanedPhyPartSetToTaskRecord(this);
        }
    }

    public void executeImpl(ExecutionContext executionContext) {

        initCleanedPartMarkSet(executionContext);
        TtlJobUtil.updateJobStage(this.jobContext, "DeletingExpiredData");
        CleanupExpiredDataLogInfo logInfo = new CleanupExpiredDataLogInfo();
        logInfo.taskBeginTs = System.currentTimeMillis();
        logInfo.taskBeginTsNano = System.nanoTime();
        logInfo.needPerformArchiving = this.jobContext.getTtlInfo().needPerformExpiredDataArchiving();
        logInfo.arcTmpTblSchema = this.jobContext.getTtlInfo().getTmpTableSchema();
        logInfo.arcTmpTblName = this.jobContext.getTtlInfo().getTmpTableName();
        logInfo.jobId = getJobId();
        logInfo.taskId = getTaskId();
        logInfo.ttlColMinValStr = this.jobContext.getTtlColMinValue();
        logInfo.firstLevelPartNameOfTtlColMinVal = this.jobContext.getPartNameForTtlColMinVal();
        logInfo.cleanupLowerBound = this.jobContext.getCleanUpLowerBound();
        logInfo.firstLevelPartNameOfCleanupLowerBound = this.jobContext.getPartNameForCleanupLowerBound();
        logInfo.previousPartBoundOfTtlColMinVal = this.jobContext.getPreviousPartBoundOfTtlColMinVal();

        TtlScheduledJobStatManager.TtlJobStatInfo statInfo =
            TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(this.schemaName, this.logicalTableName);
        if (statInfo != null) {
            logInfo.jobStatInfo = statInfo;
        }

        DataCleanupTaskSubmitter dataCleanupTaskSubmitter =
            new DataCleanupTaskSubmitter(this, executionContext, jobContext, logInfo);
        DataArchivingWorkerTaskMonitor monitor = new DataArchivingWorkerTaskMonitor(this,
            executionContext, jobContext, logInfo);
        TtlIntraTaskManager taskDelegate =
            new TtlIntraTaskManager(this, executionContext, jobContext, dataCleanupTaskSubmitter, monitor);
        taskDelegate.submitAndRunIntraTasks();
    }

    public Set<Integer> getCleanedPhyPartSet() {
        return cleanedPhyPartSet;
    }

    public void setCleanedPhyPartSet(Set<Integer> cleanedPhyPartSet) {
        this.cleanedPhyPartSet = cleanedPhyPartSet;
    }

    protected static class DataCleanupTaskSubmitter implements TtlWorkerTaskSubmitter {
        protected DdlTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext ttlJobContext;
        protected CleanupExpiredDataLogInfo logInfo;

        public DataCleanupTaskSubmitter(DdlTask parentDdlTask, ExecutionContext ec,
                                        TtlJobContext ttlJobContext,
                                        CleanupExpiredDataLogInfo logInfo) {
            this.parentDdlTask = parentDdlTask;
            this.ec = ec;
            this.ttlJobContext = ttlJobContext;
            this.logInfo = logInfo;
        }

        @Override
        public List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasks() {
            List<Pair<Future, TtlIntraTaskRunner>> result = submitWorkerTasksForDataCleaningAndArchiving();
            TtlJobUtil.getTtlJobStatInfoByJobContext(this.ttlJobContext).getTotalPhyPartCnt().set(result.size());
            return result;
        }

        /**
         * Submit the insertSelect tasks for all dn of one logical table
         */
        protected List<Pair<Future, TtlIntraTaskRunner>> submitWorkerTasksForDataCleaningAndArchiving() {

            /**
             * <pre>
             *     foreach part(zigzag concurrent by dn, use delete_thd_pool)
             *         exec insert select for one part by one trans
             *         update status for the one part
             * </pre>
             */

            /**
             * 1. build phy-part data archiving tasks of one logical tbl;
             * 2. submit phy-part data archiving tasks by using zigzag policy to balance dn workloads;
             * 3. return the submitted tasks futures
             */
            List<TtlIntraTaskRunner> fullTaskRunners = buildPhyPartDataArchivingTasksForOneLogicalTbl();
            List<TtlIntraTaskRunner> taskRunners = filerTaskRunnersForFinishedPhyPart(fullTaskRunners);
            List<TtlIntraTaskRunner> zigzagOrderTaskRunners = TtlJobUtil.zigzagSortTasks(taskRunners);
            List<Pair<Future, TtlIntraTaskRunner>> taskRunnerFutureInfos =
                submitAndExecTaskRunners(zigzagOrderTaskRunners);
            return taskRunnerFutureInfos;
        }

        protected List<TtlIntraTaskRunner> buildPhyPartDataArchivingTasksForOneLogicalTbl() {
            PhyPartSpecIterator phyPartItor = buildPhyPartIteratorByDeleteWhereCondExpr(ec);
            List<TtlIntraTaskRunner> taskList = new ArrayList<>();
            while (phyPartItor.hasNext()) {
                PartitionMetaUtil.PartitionMetaRecord partMeta = phyPartItor.next();
                DataCleaningUpIntraTask dataArcTask =
                    new DataCleaningUpIntraTask(parentDdlTask, ec, ttlJobContext, partMeta, logInfo);
                taskList.add(dataArcTask);
            }
            return taskList;
        }

        protected PhyPartSpecIterator buildPhyPartIteratorByDeleteWhereCondExpr(ExecutionContext ec) {

            TtlDefinitionInfo ttlInfo = ttlJobContext.getTtlInfo();
            String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
            TableMeta ttlTblMeta = ec.getSchemaManager(ttlTblSchema).getTableWithNull(ttlTblName);
            PartitionInfo partInfo = ttlTblMeta.getPartitionInfo();
            String cleanupLowerBound = ttlJobContext.getCleanUpLowerBound();

            Set<String> targetPhyPartNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            try {
                String whereCondExpr = TtlTaskSqlBuilder.buildDeleteWhereCondExpr(ec, ttlInfo, cleanupLowerBound);
                PartCondExprRouter condExprRouter = new PartCondExprRouter(partInfo, ec, whereCondExpr);
                condExprRouter.init();
                PartPrunedResult prunedResult = condExprRouter.route();
                if (prunedResult != null) {
                    List<PhysicalPartitionInfo> phyPartInfos = prunedResult.getPrunedPartitions();
                    for (int i = 0; i < phyPartInfos.size(); i++) {
                        PhysicalPartitionInfo phyInfo = phyPartInfos.get(i);
                        String phyPartName = phyInfo.getPartName();
                        targetPhyPartNameSet.add(phyPartName);
                    }
                }
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(String.format(
                    "Failed to optimize the phyPartitions for deleting of cleanup expired data, err is %s",
                    ex.getMessage()), ex);
                targetPhyPartNameSet.clear();
            }

            PhyPartSpecIterator phyPartItor = null;
            if (targetPhyPartNameSet.isEmpty()) {
                phyPartItor = new PhyPartSpecIterator(partInfo);
            } else {
                phyPartItor = new PhyPartSpecIterator(partInfo, targetPhyPartNameSet);
            }

            return phyPartItor;
        }

        protected List<TtlIntraTaskRunner> filerTaskRunnersForFinishedPhyPart(
            List<TtlIntraTaskRunner> fullTaskRunners) {
            CleanAndPrepareExpiredDataTask task = (CleanAndPrepareExpiredDataTask) parentDdlTask;
            Set<Integer> cleanedPhyPartSet = task.getCleanedPhyPartSet();
            if (cleanedPhyPartSet == null && cleanedPhyPartSet.isEmpty()) {
                return fullTaskRunners;
            }
            List<TtlIntraTaskRunner> finalRunners = new ArrayList<>();
            for (int i = 0; i < fullTaskRunners.size(); i++) {
                TtlIntraTaskRunner runner = fullTaskRunners.get(i);
                DataCleaningUpIntraTask intraTask = (DataCleaningUpIntraTask) runner;
                PartitionMetaUtil.PartitionMetaRecord partMeta = intraTask.getPhyPartData();
                if (cleanedPhyPartSet.contains(partMeta.getPartNum().intValue())) {
                    continue;
                }
                finalRunners.add(runner);
            }
            return finalRunners;
        }

        protected List<Pair<Future, TtlIntraTaskRunner>> submitAndExecTaskRunners(
            List<TtlIntraTaskRunner> taskRunners) {
            List<Pair<Future, TtlIntraTaskRunner>> futureInfos =
                TtlIntraTaskExecutor.getInstance().submitDeleteTaskRunners(taskRunners);
            return futureInfos;
        }
    }

    /**
     * <pre>
     * Execute task for do the followings:
     *
     * (1). insert into ttl_tmp select * from ttl_tbl partition(part) by using target interval
     * (2). delete from ttl_tbl partition(part) by using target interval
     * </pre>
     */
    @Data
    protected static class DataCleaningUpIntraTask extends TtlIntraTaskRunner {

        protected CleanAndPrepareExpiredDataTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext jobContext;
        protected PartitionMetaUtil.PartitionMetaRecord phyPartData;
        protected boolean needPerformArchiving = false;
        protected volatile boolean stopTask = false;
        protected volatile Object transConn;
        protected IntraTaskStatInfo currIntraTaskStatInfo = new IntraTaskStatInfo();
        protected CleanupExpiredDataLogInfo logInfo;

        public DataCleaningUpIntraTask(
            DdlTask parentDdlTask,
            ExecutionContext ec,
            TtlJobContext jobContext,
            PartitionMetaUtil.PartitionMetaRecord phyPartData,
            CleanupExpiredDataLogInfo logInfo
        ) {
            this.parentDdlTask = (CleanAndPrepareExpiredDataTask) parentDdlTask;
            this.ec = ec;
            this.jobContext = jobContext;
            this.phyPartData = phyPartData;
            /**
             * Check if a ttl table need do perform oss archiving
             */
            this.needPerformArchiving = jobContext.getTtlInfo().needPerformExpiredDataArchiving();
            this.logInfo = logInfo;
        }

        @Override
        public String getDnId() {
            return phyPartData.getRwDnId();
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
                    TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
                }
            }
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
            long totalBeginTs = System.nanoTime();
            String logMsgHeader = TtlLoggerUtil.buildIntraTaskLogMsgHeaderWithPhyPart(this.parentDdlTask, phyPartData);
            int batchCnt = this.jobContext.getDmlBatchSize();
            long rowLenAvg = this.jobContext.getRowLengthAvgOfTtlTbl();
            boolean finished = false;
            final AtomicLong batchRound = new AtomicLong(0);

            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
            String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
            String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
            String groupParallelismOfConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDmlConn());
            Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            sessionVariables.put("time_zone", ttlTimezoneStr);
            sessionVariables.put("names", charsetEncoding);
            sessionVariables.put("sql_mode", sqlModeSetting);
            sessionVariables.put("group_parallelism", groupParallelismOfConnStr);

            boolean useArcTrxPolicy = this.jobContext.getUseArcTrans();
            if (useArcTrxPolicy) {
                sessionVariables.put("transaction_policy", "archive");
            }

            while (true) {

                /**
                 * Check if task is interrupt
                 */
                if (checkTaskInterrupted()) {
                    doClearWorkForInterruptTask();
                    break;
                }

                final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
                int dmlBatchSize = this.jobContext.getDmlBatchSize();

                /**
                 * Try to acquire the rows speed permits of target dn if need
                 */
                boolean acquireSucc = tryAcquireRatePermits(dmlBatchSize);
                if (!acquireSucc) {
                    /**
                     * Failed to acquire rows speed permits of limiter, so ignore and next round
                     */
                    continue;
                }

                /**
                 * Prepare the sql for converging and  deleting the expired data,
                 */
                final String deleteSelectSql = buildDeleteSelectForExpiredDataArchiving(dmlBatchSize);
                final AtomicInteger insertRows = new AtomicInteger(0);
                final AtomicInteger deleteRows = new AtomicInteger(0);
                final AtomicLong insertTcNano = new AtomicLong(0);
                final AtomicLong deleteTcNano = new AtomicLong(0);

                /**
                 * Exec sql to delete and converge the expired data.
                 */
                final String ttlTblSchemaName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
                TtlJobUtil.wrapWithDistributedTrx(
                    serverConfigManager,
                    ttlTblSchemaName,
                    sessionVariables,
                    (transConn) -> {

                        /**
                         * Save the reference of transConn on IntraTask to use to do force closing
                         */
                        batchRound.incrementAndGet();
                        this.transConn = transConn;

                        /**
                         * <pre>
                         *     exec the sql:
                         *         delete from ttl_tbl partition(part)
                         *         where gmt_created <= up_bound
                         *         limit batch_cnt;
                         * </pre>
                         */
                        long deleteBeginTs = System.nanoTime();
                        int deleteArows = TtlJobUtil.execLogicalDmlOnInnerConnection(serverConfigManager,
                            ttlTblSchemaName, transConn, ec, deleteSelectSql);
                        deleteRows.set(deleteArows);
                        long deleteEndTs = System.nanoTime();

                        /**
                         * Stat the phy sql of intra task
                         */
                        deleteTcNano.set(deleteEndTs - deleteBeginTs);
                        this.currIntraTaskStatInfo.getTotalDeleteTimeCostNano().addAndGet(deleteTcNano.get());
                        this.currIntraTaskStatInfo.getTotalDeleteRows().addAndGet(deleteArows);
                        this.currIntraTaskStatInfo.getTotalDeleteSqlCount().incrementAndGet();

                        return deleteArows;
                    }
                );

                if (deleteRows.get() < batchCnt) {
                    /**
                     * if deleteRows is less than batchCnt, that means
                     * current batch is the last batch.
                     */
                    finished = true;
                    long totalEndTs = System.nanoTime();
                    long taskTimeCost = totalEndTs - totalBeginTs;
                    this.currIntraTaskStatInfo.getTotalExecTimeCostNano().set(taskTimeCost);
                    logOneIntraTaskExecResult(
                        logMsgHeader,
                        this.jobContext.getCleanUpLowerBound(),
                        batchRound.intValue(),
                        rowLenAvg,
                        insertRows,
                        deleteRows,
                        insertTcNano,
                        deleteTcNano,
                        true,
                        currIntraTaskStatInfo);
                    break;
                } else {
                    logOneIntraTaskExecResult(
                        logMsgHeader,
                        this.jobContext.getCleanUpLowerBound(),
                        batchRound.intValue(),
                        rowLenAvg,
                        insertRows,
                        deleteRows,
                        insertTcNano,
                        deleteTcNano,
                        false,
                        currIntraTaskStatInfo);
                }
            }

            if (finished) {
                /**
                 * Mark the phypart finishing cleaning persistently
                 */
                updateCleanStatusForOnePartition();
            } else {
                long totalEndTs = System.nanoTime();
                long taskTimeCost = totalEndTs - totalBeginTs;
                this.currIntraTaskStatInfo.getTotalExecTimeCostNano().addAndGet(taskTimeCost);
            }
        }

//        protected String buildInsertSelectForExpiredDataArchiving(int dmlBatchSize) {
//
//            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
//
//            String phyPartName = this.phyPartData.getPartName();
//            String minBoundToCleanup = this.jobContext.getCleanUpLowerBound();
//            String partBoundOfPartOfTtlMinVal = this.jobContext.getPreviousPartBoundOfTtlColMinVal();
//            boolean needAddIntervalLowerBound = !StringUtils.isEmpty(partBoundOfPartOfTtlMinVal);
//            String queryHintForInsert = TtlConfigUtil.getQueryHintForInsertExpiredData();
//            String insertSelectSqlTemp =
//                TtlTaskSqlBuilder.buildInsertSelectTemplate(ttlInfo, needAddIntervalLowerBound, queryHintForInsert);
//            String insertSelectSql = "";
//            if (needAddIntervalLowerBound) {
//                insertSelectSql =
//                    String.format(insertSelectSqlTemp, phyPartName, minBoundToCleanup, partBoundOfPartOfTtlMinVal,
//                        dmlBatchSize);
//            } else {
//                insertSelectSql = String.format(insertSelectSqlTemp, phyPartName, minBoundToCleanup, dmlBatchSize);
//            }
//
//            return insertSelectSql;
//        }

        protected String buildDeleteSelectForExpiredDataArchiving(int dmlBatchSize) {

            TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
            String previousPartBoundOfPartOfTtlMinVal = this.jobContext.getPreviousPartBoundOfTtlColMinVal();
            boolean needAddIntervalLowerBound = !StringUtils.isEmpty(previousPartBoundOfPartOfTtlMinVal);
            String phyPartName = this.phyPartData.getSubPartName();
            if (StringUtils.isEmpty(phyPartName)) {
                phyPartName = this.phyPartData.getPartName();
            }

            String minBoundToCleanup = this.jobContext.getCleanUpLowerBound();
            String forceIndexExpr = this.jobContext.getTtlColForceIndexExpr();
            String queryHintForDelete = TtlConfigUtil.getQueryHintForDeleteExpiredData();
            String deleteSqlTemp =
                TtlTaskSqlBuilder.buildDeleteTemplate(ttlInfo, needAddIntervalLowerBound, queryHintForDelete,
                    forceIndexExpr);

            boolean ttlColUseFuncExpr = ttlInfo.isTtlColUseFuncExpr();
            if (ttlColUseFuncExpr) {
                ColumnMeta ttlColMeta = ttlInfo.getTtlColMeta(ec);
                PartKeyLevel partKeyLevel = PartKeyLevel.PARTITION_KEY;
                boolean usePartFunc = false;

                if (!StringUtils.isEmpty(minBoundToCleanup)) {
                    TtlPartitionUtil.TtlColBoundValue minBoundToCleanupOnTtlColBndVal =
                        new TtlPartitionUtil.TtlColBoundValue(minBoundToCleanup, ttlColMeta, partKeyLevel, usePartFunc,
                            ttlInfo);
                    minBoundToCleanup =
                        minBoundToCleanupOnTtlColBndVal.getPartBoundValueStringByOriginalPartColDataType();
                }

                if (!StringUtils.isEmpty(previousPartBoundOfPartOfTtlMinVal)) {
                    TtlPartitionUtil.TtlColBoundValue minBoundToCleanupOnTtlColBndVal =
                        new TtlPartitionUtil.TtlColBoundValue(previousPartBoundOfPartOfTtlMinVal, ttlColMeta,
                            partKeyLevel, usePartFunc, ttlInfo);
                    previousPartBoundOfPartOfTtlMinVal =
                        minBoundToCleanupOnTtlColBndVal.getPartBoundValueStringByOriginalPartColDataType();
                }
            }

            String deleteSql = "";
            if (needAddIntervalLowerBound) {
                deleteSql =
                    String.format(deleteSqlTemp, phyPartName, minBoundToCleanup, previousPartBoundOfPartOfTtlMinVal,
                        dmlBatchSize);
            } else {
                deleteSql = String.format(deleteSqlTemp, phyPartName, minBoundToCleanup, dmlBatchSize);
            }

            if (StringUtils.isEmpty(logInfo.deleteSqlTemp)) {
                logInfo.deleteSqlTemp = deleteSql;
            }

            return deleteSql;
        }

        protected boolean checkTaskInterrupted() {
            if (Thread.currentThread().isInterrupted() || stopTask || ec.getDdlContext().isInterrupted()) {
                return true;
            }
            return false;
        }

        protected boolean tryAcquireRatePermits(int permitsVal) {
            boolean enableRowsSpeedLimit = TtlConfigUtil.isEnableTtlCleanupRowsSpeedLimit();
            if (!enableRowsSpeedLimit) {
                return true;
            }
            long waitPeriods = TtlConfigUtil.getMaxWaitAcquireRatePermitsPeriods();
            String rwDnId = this.phyPartData.getRwDnId();
            boolean acquireSucc = false;
            try {
                long waitPermitsBeginTs = System.nanoTime();
                acquireSucc =
                    TtlDataCleanupRateLimiter.getInstance()
                        .tryAcquire(rwDnId, permitsVal, waitPeriods, TimeUnit.MILLISECONDS);
                long waitPermitsEndTs = System.nanoTime();
                this.logInfo.jobStatInfo.getWaitPermitsTimeCostNano()
                    .addAndGet((waitPermitsEndTs - waitPermitsBeginTs));
                this.logInfo.jobStatInfo.getAcquirePermitsCount().incrementAndGet();
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(
                    String.format("Failed to acquire rows speeds permits from dn[%s]", rwDnId), ex);
                acquireSucc = false;
            }
            return acquireSucc;
        }

        protected void doClearWorkForInterruptTask() {
            /**
             * Just do nothing
             */
            String msg = String.format("ttl job has been interrupted, phypart is %s, table is %s.%s",
                phyPartData.getPartName(), phyPartData.getTableSchema(), phyPartData.getTableName());

            /**
             * Throw a exception to notify ddl-engine this task is not finished and restart at next time
             */
            throw new TtlJobInterruptedException(msg);
        }

        protected void updateCleanStatusForOnePartition() {

            /**
             * Update status to label the part has finished archive expired data
             */
            markPartFinishExpiredDataCleaning(parentDdlTask, phyPartData);
        }

        protected void logOneIntraTaskExecResult(
            String logMsgHeader,
            String minBoundToBeCleanUp,
            int batchRound,
            long rowLenAvg,
            AtomicInteger insertRowsCurrRound,
            AtomicInteger deleteRowsCurrRound,
            AtomicLong insertTcNanoCurrRound,
            AtomicLong deleteTcNanoCurrRound,
            boolean isIntraTaskFinish,
            IntraTaskStatInfo oneIntraTaskStatInfo) {

            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = this.logInfo.jobStatInfo;
            TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = TtlScheduledJobStatManager.getInstance()
                .getGlobalTtlJobStatInfo();

            long currDeleteRoundTcMs = (deleteTcNanoCurrRound.get()) / (1000 * 1000);
            long deleteRowsCurrRoundVal = deleteRowsCurrRound.get();
            long deleteDataLenCurrRoundVal = deleteRowsCurrRoundVal * rowLenAvg;

            globalStatInfo.getTotalDeleteSqlTimeCost().addAndGet(currDeleteRoundTcMs);
            globalStatInfo.getTotalDeleteSqlCount().incrementAndGet();
            globalStatInfo.getTotalCleanupRows().addAndGet(deleteRowsCurrRoundVal);
            globalStatInfo.getTotalCleanupDataLength().addAndGet(deleteDataLenCurrRoundVal);

            jobStatInfo.getDeleteSqlTimeCost().addAndGet(currDeleteRoundTcMs);
            jobStatInfo.getDeleteSqlCount().incrementAndGet();
            jobStatInfo.getCleanupRows().addAndGet(deleteRowsCurrRoundVal);
            jobStatInfo.getCleanupDataLength().addAndGet(deleteDataLenCurrRoundVal);

            if (isIntraTaskFinish) {
                jobStatInfo.getCleanedPhyPartCnt().incrementAndGet();
            }

            jobStatInfo.calcDeleteSqlAvgRt();
            jobStatInfo.calcDeleteRowsSpeed();//ROWS/S

            /**
             * <pre>
             *     cb: cleanup lower bound
             *     arc: is need perform archving data
             *     r: the batch roundNum of handling a part tbl for current cleanup lower bound
             *          e.g  for 1...roundNum
             *                  delete * from ttl_tbl where ttl_col <= xxx order by ttl_col asc limit batch_size
             *     tc: timecost of a intra task of DataArchivingIntraTask
             *     drows: the rows num of deleting sql
             *     dtAvg: the avg rt of deleting sql
             *     irows: the rows num of inserting sql
             *     itAvg: the avg rt of inserting sql
             * </pre>
             */
            if (TtlConfigUtil.isEnableCleanupIntraTaskInfoLog()) {
                long deleteRowsOfCurrIntraTask = oneIntraTaskStatInfo.getTotalDeleteRows().get();
                long deleteTcNanoOfCurrIntraTask = oneIntraTaskStatInfo.getTotalExecTimeCostNano().get();
                long deleteSqlCntOfCurrIntraTask = oneIntraTaskStatInfo.getTotalDeleteSqlCount().get();

                long deleteRtMsOfCurrIntraTask =
                    deleteTcNanoOfCurrIntraTask / (deleteSqlCntOfCurrIntraTask * 1000 * 1000);
                long deleteTcMsOfCurrIntraTask = deleteTcNanoOfCurrIntraTask / (1000 * 1000);

                String logMsg =
                    String.format("[%s] [cb=%s,arc=%s] [r=%d] [tc=%s, drows=%d, drtAvg=%d]",
                        logMsgHeader, minBoundToBeCleanUp, this.needPerformArchiving, batchRound,
                        deleteTcMsOfCurrIntraTask, deleteRowsOfCurrIntraTask,
                        deleteRtMsOfCurrIntraTask);
                TtlLoggerUtil.TTL_TASK_LOGGER.info(logMsg);
            }
        }
    }

    /**
     * The task monitor for the DataArchivingIntraTask
     */
    protected static class DataArchivingWorkerTaskMonitor implements TtlWorkerTaskMonitor {

        protected CleanAndPrepareExpiredDataTask parentDdlTask;
        protected ExecutionContext ec;
        protected TtlJobContext jobContext;
        protected volatile boolean stopTask = false;
        protected CleanupExpiredDataLogInfo logInfo;

        public DataArchivingWorkerTaskMonitor(
            DdlTask parentDdlTask,
            ExecutionContext ec,
            TtlJobContext jobContext,
            CleanupExpiredDataLogInfo logInfo
        ) {
            this.parentDdlTask = (CleanAndPrepareExpiredDataTask) parentDdlTask;
            this.ec = ec;
            this.jobContext = jobContext;
            this.logInfo = logInfo;
        }

        @Override
        public void doMonitoring() {

            String dbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
            String tbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = TtlScheduledJobStatManager.getInstance()
                .getTtlJobStatInfo(dbName, tbName);
            if (jobStatInfo == null) {
                return;
            }

            /**
             * Do some logging
             */
            logInfo.waitCleanupTaskRunningLoopRound++;

            logInfo.cleanupRows = jobStatInfo.getCleanupRows().get();
            logInfo.cleanupSpeed = jobStatInfo.calcCleanupSpeed();
            logInfo.cleanupRowsSpeed = jobStatInfo.calcCleanupRowsSpeed();
            logInfo.cleanupTimeCost = jobStatInfo.getCleanupTimeCost().get();

            logInfo.deleteRows = jobStatInfo.getCleanupRows().get();
            logInfo.deleteSqlCnt = jobStatInfo.getDeleteSqlCount().get();
            logInfo.deleteTimeCost = jobStatInfo.getDeleteSqlTimeCost().get();
            logInfo.deleteAvgRt = jobStatInfo.calcDeleteSqlAvgRt();
            logInfo.deleteRowsSpeed = jobStatInfo.calcDeleteRowsSpeed();

            logTaskExecResult(parentDdlTask, jobContext, logInfo);
        }

        @Override
        public boolean checkNeedStop() {
            boolean needStop = false;
            if (!this.jobContext.getTtlInfo().needPerformExpiredDataArchiving()) {
                return needStop;
            }
//            long arcTmpTblDataLength = TtlJobUtil.fetchArcTmlTableDataLength(ec, this.jobContext, null);
//            long arcTmpTblDataLengthLimit = TtlConfigUtil.maxTtlTmpTableDataLength;
//            needStop = TtlJobUtil.checkIfArcTmlTableDataLengthExceedLimit(arcTmpTblDataLength);
//            if (needStop) {
//                Long jobId = this.parentDdlTask.getJobId();
//                Long taskId = this.parentDdlTask.getTaskId();
//                String arcTblSchema = this.jobContext.getTtlInfo().getTmpTableSchema();
//                String arcTblName = this.jobContext.getTtlInfo().getTmpTableName();
//                String logMsg = String.format(
//                    "DdlTask[%s-%s] arcTmpTable[%s.%s] data length has exceeded, actual is %s, limit is %s, ",
//                    jobId, taskId, arcTblSchema, arcTblName, arcTmpTblDataLength, arcTmpTblDataLengthLimit);
//                TtlLoggerUtil.TTL_TASK_LOGGER.info(logMsg);
//            }

            this.logInfo.needStopCleanup = needStop;
            this.logInfo.arcTmpTblDataLength = 0;
            this.logInfo.arcTmpTblDataLengthLimit = 0;
            return needStop;
        }

        @Override
        public void handleResults(boolean isFinished,
                                  boolean interrupted,
                                  boolean withinMaintainableTimeFrame) {
            logInfo.isAllIntraTaskFinished = isFinished;
            logInfo.taskEndTs = System.currentTimeMillis();
            logInfo.stopByMaintainTime = !withinMaintainableTimeFrame;
            logInfo.isInterrupted = interrupted;
            logInfo.isTaskEnd = true;
            logTaskExecResult(parentDdlTask, jobContext, logInfo);
        }
    }

    protected static void markPartFinishExpiredDataCleaning(DdlTask task,
                                                            PartitionMetaUtil.PartitionMetaRecord parMetaRec) {
        final CleanAndPrepareExpiredDataTask currentTask = (CleanAndPrepareExpiredDataTask) task;
        final PartitionMetaUtil.PartitionMetaRecord phyPartMetaRec = parMetaRec;
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {

                synchronized (currentTask) {
                    /**
                     * Query Task Record By Using For Update
                     */
                    List<DdlEngineTaskRecord> taskRecords =
                        engineTaskAccessor.queryTasksForUpdate(currentTask.getJobId(), currentTask.getName());

                    if (taskRecords.isEmpty()) {
                        return 0;
                    }

                    DdlEngineTaskRecord currTaskRec = taskRecords.get(0);

                    /**
                     * Convert TaskRecord to TaskObj
                     */
                    CleanAndPrepareExpiredDataTask newTask =
                        (CleanAndPrepareExpiredDataTask) TaskHelper.fromDdlEngineTaskRecord(currTaskRec);

                    /**
                     * Fetch the newest bitset of phyPart
                     */
                    Set<Integer> newestCleanedPhyPartSet = newTask.getCleanedPhyPartSet();

                    /**
                     * Update the bitset to mark current part as finished
                     */
                    TtlJobPhyPartBitSetUtil.markOnePartAsFinished(newestCleanedPhyPartSet, phyPartMetaRec);

                    currentTask.setCleanedPhyPartSet(newestCleanedPhyPartSet);
                    currentTask.setState(DdlTaskState.DIRTY);

                    /**
                     * Convert newTask to taskRecord to store into metadb
                     */
                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(newTask);

                    /**
                     * Update Metadb
                     */
                    int updateRs = engineTaskAccessor.updateTask(taskRecord);
                    return updateRs;
                }
            }
        };
        delegate.execute();
    }

    protected static void storeCleanedPhyPartSetToTaskRecord(DdlTask task) {
        final CleanAndPrepareExpiredDataTask currentTask = (CleanAndPrepareExpiredDataTask) task;
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {

                synchronized (currentTask) {
                    /**
                     * Convert newTask to taskRecord to store into metadb
                     */
                    currentTask.setState(DdlTaskState.DIRTY);
                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);

                    /**
                     * Update ddl_engine_task of Metadb
                     */
                    int updateRs = engineTaskAccessor.updateTask(taskRecord);

                    return updateRs;
                }
            }
        };
        delegate.execute();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        executeImpl(executionContext);
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
//        boolean needPerformArchiving = this.jobContext.getTtlInfo().needPerformExpiredDataArchivingByOssTbl();
        Class previousTaskClass = PrepareCleanupIntervalTask.class;
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(), previousTaskClass,
            getSchemaName(), getLogicalTableName());
        this.jobContext = jobContext;
    }

    protected static void logTaskExecResult(DdlTask ddlTask,
                                            TtlJobContext jobContext,
                                            CleanupExpiredDataLogInfo logInfo) {
        logInfo.logTaskExecResult(ddlTask, jobContext);
    }
}