package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

public class TtlConfigUtil {

    /**
     * ===========================
     * Config Params for polardbx-inst-level
     * ===========================
     */

    /**
     * The max wait time for interrupting a running ttl intra-task, unit: ms, default is 5s
     * Unit: ms
     */
    public static volatile int intraTaskInterruptionMaxWaitTime =
        Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_INTERRUPTION_MAX_WAIT_TIME.getDefault());

    /**
     * The wait time of each round for the delegate of intra-tasks
     * Unit: ms, default 1s
     */
    public static volatile int intraTaskDelegateEachRoundWaitTime =
        Integer.valueOf(ConnectionParams.TTL_INTRA_TASK_MONITOR_EACH_ROUTE_WAIT_TIME.getDefault());

    /**
     * The max wait time for waiting a ddl-job from pause to running, unit: ms, default value is  30s
     */
    public static volatile int maxWaitTimeForDdlJobFromPauseToRunning = 30000;

    /**
     * The max data length of one partition of ttl-tmp table, default value is 1G
     */
    public static volatile long maxTtlTmpTableDataLength = 128 * 1024 * 1024;// for test ,use 128MB

    /**
     * Label if print the log for each intra-task of ClearAndPrepareExpiredDataTask
     */
    public static volatile boolean enableCleanupIntraTaskInfoLog = false;

    /**
     * The max percent of (data_free * 100 /  data_length) of ttl-table to perform optimize-table operation,
     * the default value if 60% , unit: %
     */
    public static volatile long maxDataFreePercentOfTtlTable =
        Long.valueOf(ConnectionParams.TTL_TBL_MAX_DATA_FREE_PERCENT.getDefault());

    /**
     * Label if auto perform the optimize-table operation for the ttl-table after data archiving
     */
    public static volatile boolean enablePerformAutoOptiTtlTableAfterArchiving = true;

    /**
     * Label if need control optimize-table ddl-job by ttl job automatically
     * <pre>
     *     if TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB=true, then:
     *     if opti-table job does not exists, auto submit;
     *     if opti-table  job exists and state is paused, auto continue ddl;
     *     if opti-table  job exists and state is running, auto wait it finished;
     * </pre>
     */
    public static volatile boolean enableAutoControlOptiTblByTtlJob =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_AUTO_OPTIMIZE_TABLE_IN_TTL_JOB.getDefault());
    ;

    /**
     * The interval of each add part of arc tbl
     */
    public static volatile int intervalOfPreBuildPartOfArcTbl = 1;

    /**
     * The pre-built part count of range base
     * on current_datetime() for creating columnar index
     */
    public static volatile int preBuiltPartCntForCreatColumnarIndex =
        Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_PRE_ALLOCATE_COUNT.getDefault());
    ;

    /**
     * The post-built part count of range base
     * on current_datetime() for creating columnar index
     */
    public static volatile int postBuiltPartCntForCreateColumnarIndex =
        Integer.valueOf(ConnectionParams.TTL_DEFAULT_ARC_POST_ALLOCATE_COUNT.getDefault());

    /**
     * Label if auto add partition for arc cci
     */
    public static volatile boolean enableAutoAddPartsForArcCci = true;

    /**
     * The max parallelism of the scheduled ttl job of tables,
     * that means the  at most tables of  maxTtlScheduledJobParallelism are allowed to
     * perform archive data at the same time
     */
    public static volatile int ttlScheduledJobMaxParallelism = 2;

    /**
     * The max thread count of threadpool of select sql of ttl job
     */
    public static volatile int ttlGlobalSelectWorkerCount =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_SELECT_WORKER_COUNT.getDefault());

    /**
     * The max thread count of threadpool of delete sql of ttl job
     */
    public static volatile int ttlGlobalDeleteWorkerCount =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_SELECT_WORKER_COUNT.getDefault());

    /**
     * The default batch size of delete/insert of ttl job
     */
    public static int ttlJobDefaultBatchSize =
        Integer.valueOf(ConnectionParams.TTL_JOB_DEFAULT_BATCH_SIZE.getDefault());

    /**
     * The interval count for computing minCleanupBound base on lowerBound(normalized minVal of ttl_col),
     * that means the delta = ttlMinCleanupBoundIntervalCount * ttlUnit, is the delta interval between
     * minCleanupBound and the lowerBound, default is 1
     */
    public static int ttlCleanupBoundIntervalCount =
        Integer.valueOf(ConnectionParams.TTL_CLEANUP_BOUND_INTERVAL_COUNT.getDefault());

    /**
     * Stop ttl-job scheduling for all ttl tables, used for handling critical situation
     */
    public static boolean stopAllTtlTableJobScheduling =
        Boolean.valueOf(ConnectionParams.TTL_STOP_ALL_JOB_SCHEDULING.getDefault());

    /**
     * Label if use archive trans policy for all trans of ttl-job
     */
    public static boolean useArchiveTransPolicy = true;

    /**
     * The default merge_union_size for the select sql of fetch ttl-col lower bound
     */
    public static int mergeUnionSizeForSelectLowerBound = 2;

    /**
     * Use merge group concurrent for the select sql of fetch ttl-col lower bound
     */
    public static boolean useMergeConcurrentForSelectLowerBound = true;

    /**
     * The query hint for the select stmt of fetch ttl-col lower bound,
     * which use to control the concurrent policy
     */
    public static String queryHintForSelectLowerBound =
        "/*+TDDL:cmd_extra(GROUP_CONCURRENT_BLOCK=true,PREFETCH_EXECUTE_POLICY=1,MERGE_UNION_SIZE=2,SOCKET_TIMEOUT=1800000)*/";

    /**
     * The query hint for delete stmt of deleting expired data, format is /TDDL: cmd_extra(k1=v1,k2=v2,...)/
     */
    public static String queryHintForDeleteExpiredData = "/*+TDDL:cmd_extra(SOCKET_TIMEOUT=1800000)*/";

    /**
     * The query hint for insert-select stmt of preparing expired data, format is /TDDL: cmd_extra(k1=v1,k2=v2,...)/
     */
    public static String queryHintForInsertExpiredData = "/*+TDDL:cmd_extra(SOCKET_TIMEOUT=1800000)*/";

    /**
     * The query hint for optimize-table stmt for ttl-tbl,format is /TDDL: cmd_extra(k1=v1,k2=v2,...)/
     */
    public static String queryHintForOptimizeTable =
        "/*+TDDL:cmd_extra(ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true, RETURN_JOB_ID_ON_ASYNC_DDL_MODE=true, OPTIMIZE_TABLE_PARALLELISM=4, SOCKET_TIMEOUT=1800000)*/";

    /**
     * The query hint for the select stmt of fetch ttl-col lower bound,
     * which use to control the concurrent policy
     */
    public static String queryHintForAutoAddParts = "/*+TDDL:cmd_extra(SOCKET_TIMEOUT=1800000)*/";

    /**
     * The default group_parallelism of conn of select stmt, 0 means use the default val of inst_config
     */
    public static Long defaultGroupParallelismOnDqlConn = 1L;

    /**
     * The default group_parallelism of conn of delete/insert stmt,0 means use the default val of inst_config
     */
    public static Long defaultGroupParallelismOnDmlConn = 0L;

    /**
     * Label if need auto add maxvalue partition for cci of arctmp
     */
    public static boolean autoAddMaxValuePartForCci =
        Boolean.valueOf(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING.getDefault());
    ;

    /**
     * The max periods of try waiting to acquire the rate permits, unit: ms
     */
    public static long maxWaitAcquireRatePermitsPeriods =
        Long.valueOf(ConnectionParams.TTL_MAX_WAIT_ACQUIRE_RATE_PERMITS_PERIODS.getDefault());

    /**
     * Label if need limit the cleanup rows speed for each dn
     */
    public static boolean enableTtlCleanupRowsSpeedLimit =
        Boolean.valueOf(ConnectionParams.TTL_ENABLE_CLEANUP_ROWS_SPEED_LIMIT.getDefault());

    /**
     * The default rowsSpeed limit for each dn, unit: rows/sec
     */
    public static long cleanupRowsSpeedLimitEachDn =
        Long.valueOf(ConnectionParams.TTL_CLEANUP_ROWS_SPEED_LIMIT_EACH_DN.getDefault());

    /**
     * Label if ignore maintain window in ttl job
     */
    public static boolean ignoreMaintainWindowInTtlJob =
        Boolean.valueOf(ConnectionParams.TTL_IGNORE_MAINTAIN_WINDOW_IN_DDL_JOB.getDefault());

    /**
     * The ratio of global-delete-worker / rw-dn-count, default is 2
     */
    public static int ttlGlobalWorkerDnRatio =
        Integer.valueOf(ConnectionParams.TTL_GLOBAL_WORKER_DN_RATIO.getDefault());

    /**
     * ===========================
     * Config Params for polardbx-session-level
     * ===========================
     */

    /**
     * The max part count in one archive ddl cmd when data length is less than limit
     */
    public static int maxPartCountOneArchivedCmd = 1;

    /**
     * The max subbpart count of ttl-tmp tbl and arc-tbl
     */
    public static int ttlTmpTblDefaultSubPartHashCount = 8;

    /**
     * The max part count in one archive ddl cmd when data length is more than limit
     */
    public static int maxPartCountOneArchivedCmdWhenDataLengthExceedLimit = 3;

    /**
     * The default charset of trans conn of ttl-job when exec sql
     */
    public static String defaultCharsetEncodingOnTransConn = "utf8mb4";

    /**
     * The default sql mode of trans conn of ttl-job when exec sql
     */
    public static String defaultSqlModeOnTransConn = "";

    /**
     * The parallelism of alter table ttl_tbl optimize partitions xxx
     */
    public static int optimizePartitionParallelism = 4;

    public static int getIntraTaskInterruptionMaxWaitTime() {
        return intraTaskInterruptionMaxWaitTime;
    }

    public static void setIntraTaskInterruptionMaxWaitTime(int intraTaskInterruptionMaxWaitTime) {
        TtlConfigUtil.intraTaskInterruptionMaxWaitTime = intraTaskInterruptionMaxWaitTime;
    }

    public static int getIntraTaskDelegateEachRoundWaitTime() {
        return intraTaskDelegateEachRoundWaitTime;
    }

    public static void setIntraTaskDelegateEachRoundWaitTime(int intraTaskDelegateEachRoundWaitTime) {
        TtlConfigUtil.intraTaskDelegateEachRoundWaitTime = intraTaskDelegateEachRoundWaitTime;
    }

    public static int getMaxWaitTimeForDdlJobFromPauseToRunning() {
        return maxWaitTimeForDdlJobFromPauseToRunning;
    }

    public static void setMaxWaitTimeForDdlJobFromPauseToRunning(int maxWaitTimeForDdlJobFromPauseToRunning) {
        TtlConfigUtil.maxWaitTimeForDdlJobFromPauseToRunning = maxWaitTimeForDdlJobFromPauseToRunning;
    }

    public static long getMaxTtlTmpTableDataLength() {
        return maxTtlTmpTableDataLength;
    }

    public static void setMaxTtlTmpTableDataLength(long maxTtlTmpTableDataLength) {
        TtlConfigUtil.maxTtlTmpTableDataLength = maxTtlTmpTableDataLength;
    }

    public static boolean isEnableCleanupIntraTaskInfoLog() {
        return enableCleanupIntraTaskInfoLog;
    }

    public static void setEnableCleanupIntraTaskInfoLog(boolean enableCleanupIntraTaskInfoLog) {
        TtlConfigUtil.enableCleanupIntraTaskInfoLog = enableCleanupIntraTaskInfoLog;
    }

    public static boolean isEnableAutoControlOptiTblByTtlJob() {
        return enableAutoControlOptiTblByTtlJob;
    }

    public static void setEnableAutoControlOptiTblByTtlJob(boolean enableAutoControlOptiTblByTtlJob) {
        TtlConfigUtil.enableAutoControlOptiTblByTtlJob = enableAutoControlOptiTblByTtlJob;
    }

    public static boolean isEnablePerformAutoOptiTtlTableAfterArchiving() {
        return enablePerformAutoOptiTtlTableAfterArchiving;
    }

    public static void setEnablePerformAutoOptiTtlTableAfterArchiving(
        boolean enablePerformAutoOptiTtlTableAfterArchiving) {
        TtlConfigUtil.enablePerformAutoOptiTtlTableAfterArchiving = enablePerformAutoOptiTtlTableAfterArchiving;
    }

    public static long getMaxDataFreePercentOfTtlTable() {
        return maxDataFreePercentOfTtlTable;
    }

    public static void setMaxDataFreePercentOfTtlTable(long maxDataFreePercentOfTtlTable) {
        TtlConfigUtil.maxDataFreePercentOfTtlTable = maxDataFreePercentOfTtlTable;
    }

    public static int getIntervalOfPreBuildPartOfArcTbl() {
        return intervalOfPreBuildPartOfArcTbl;
    }

    public static void setIntervalOfPreBuildPartOfArcTbl(int intervalOfPreBuildPartOfArcTbl) {
        TtlConfigUtil.intervalOfPreBuildPartOfArcTbl = intervalOfPreBuildPartOfArcTbl;
    }

    public static int getPreBuiltPartCntForCreatColumnarIndex() {
        return preBuiltPartCntForCreatColumnarIndex;
    }

    public static void setPreBuiltPartCntForCreatColumnarIndex(int preBuiltPartCntForCreatColumnarIndex) {
        TtlConfigUtil.preBuiltPartCntForCreatColumnarIndex = preBuiltPartCntForCreatColumnarIndex;
    }

    public static int getPostBuiltPartCntForCreateColumnarIndex() {
        return postBuiltPartCntForCreateColumnarIndex;
    }

    public static void setPostBuiltPartCntForCreateColumnarIndex(int postBuiltPartCntForCreateColumnarIndex) {
        TtlConfigUtil.postBuiltPartCntForCreateColumnarIndex = postBuiltPartCntForCreateColumnarIndex;
    }

    public static int getTtlScheduledJobMaxParallelism() {
        return ttlScheduledJobMaxParallelism;
    }

    public static void setTtlScheduledJobMaxParallelism(int ttlScheduledJobMaxParallelism) {
        TtlConfigUtil.ttlScheduledJobMaxParallelism = ttlScheduledJobMaxParallelism;
    }

    public static int getTtlGlobalSelectWorkerCount() {
        return ttlGlobalSelectWorkerCount;
    }

    public static void setTtlGlobalSelectWorkerCount(int ttlGlobalSelectWorkerCount) {
        TtlConfigUtil.ttlGlobalSelectWorkerCount = ttlGlobalSelectWorkerCount;
    }

    public static int getTtlGlobalDeleteWorkerCount() {
        return ttlGlobalDeleteWorkerCount;
    }

    public static void setTtlGlobalDeleteWorkerCount(int ttlGlobalDeleteWorkerCount) {
        TtlConfigUtil.ttlGlobalDeleteWorkerCount = ttlGlobalDeleteWorkerCount;
    }

    public static int getMaxPartCountOneArchivedCmd() {
        return maxPartCountOneArchivedCmd;
    }

    public static void setMaxPartCountOneArchivedCmd(int maxPartCountOneArchivedCmd) {
        TtlConfigUtil.maxPartCountOneArchivedCmd = maxPartCountOneArchivedCmd;
    }

    public static int getTtlTmpTblDefaultSubPartHashCount() {
        return ttlTmpTblDefaultSubPartHashCount;
    }

    public static void setTtlTmpTblDefaultSubPartHashCount(int ttlTmpTblDefaultSubPartHashCount) {
        TtlConfigUtil.ttlTmpTblDefaultSubPartHashCount = ttlTmpTblDefaultSubPartHashCount;
    }

    public static int getMaxPartCountOneArchivedCmdWhenDataLengthExceedLimit() {
        return maxPartCountOneArchivedCmdWhenDataLengthExceedLimit;
    }

    public static void setMaxPartCountOneArchivedCmdWhenDataLengthExceedLimit(
        int maxPartCountOneArchivedCmdWhenDataLengthExceedLimit) {
        TtlConfigUtil.maxPartCountOneArchivedCmdWhenDataLengthExceedLimit =
            maxPartCountOneArchivedCmdWhenDataLengthExceedLimit;
    }

    public static String getDefaultCharsetEncodingOnTransConn() {
        return defaultCharsetEncodingOnTransConn;
    }

    public static void setDefaultCharsetEncodingOnTransConn(String defaultCharsetEncodingOnTransConn) {
        TtlConfigUtil.defaultCharsetEncodingOnTransConn = defaultCharsetEncodingOnTransConn;
    }

    public static String getDefaultSqlModeOnTransConn() {
        return defaultSqlModeOnTransConn;
    }

    public static void setDefaultSqlModeOnTransConn(String defaultSqlModeOnTransConn) {
        TtlConfigUtil.defaultSqlModeOnTransConn = defaultSqlModeOnTransConn;
    }

    public static int getOptimizePartitionParallelism() {
        return optimizePartitionParallelism;
    }

    public static void setOptimizePartitionParallelism(int optimizePartitionParallelism) {
        TtlConfigUtil.optimizePartitionParallelism = optimizePartitionParallelism;
    }

    public static int getMergeUnionSizeForSelectLowerBound() {
        return mergeUnionSizeForSelectLowerBound;
    }

    public static void setMergeUnionSizeForSelectLowerBound(int mergeUnionSizeForSelectLowerBound) {
        TtlConfigUtil.mergeUnionSizeForSelectLowerBound = mergeUnionSizeForSelectLowerBound;
    }

    public static boolean isUseGsiInsteadOfCciForCreateColumnarArcTbl(ExecutionContext ec) {
        boolean useGsiAsCci = ec.getParamManager().getBoolean(ConnectionParams.TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL);
        return useGsiAsCci;
    }

    public static int getTtlJobDefaultBatchSize() {
        return ttlJobDefaultBatchSize;
    }

    public static void setTtlJobDefaultBatchSize(int ttlJobDefaultBatchSize) {
        TtlConfigUtil.ttlJobDefaultBatchSize = ttlJobDefaultBatchSize;
    }

    public static int getTtlCleanupBoundIntervalCount() {
        return ttlCleanupBoundIntervalCount;
    }

    public static void setTtlCleanupBoundIntervalCount(int ttlCleanupBoundIntervalCount) {
        TtlConfigUtil.ttlCleanupBoundIntervalCount = ttlCleanupBoundIntervalCount;
    }

    public static boolean isStopAllTtlTableJobScheduling() {
        return stopAllTtlTableJobScheduling;
    }

    public static void setStopAllTtlTableJobScheduling(boolean stopAllTtlTableJobScheduling) {
        TtlConfigUtil.stopAllTtlTableJobScheduling = stopAllTtlTableJobScheduling;
    }

    public static boolean isUseArchiveTransPolicy() {
        return useArchiveTransPolicy;
    }

    public static void setUseArchiveTransPolicy(boolean useArchiveTransPolicy) {
        TtlConfigUtil.useArchiveTransPolicy = useArchiveTransPolicy;
    }

    public static boolean isUseMergeConcurrentForSelectLowerBound() {
        return useMergeConcurrentForSelectLowerBound;
    }

    public static void setUseMergeConcurrentForSelectLowerBound(boolean useMergeConcurrentForSelectLowerBound) {
        TtlConfigUtil.useMergeConcurrentForSelectLowerBound = useMergeConcurrentForSelectLowerBound;
    }

    public static String getQueryHintForSelectLowerBound() {
        return queryHintForSelectLowerBound;
    }

    public static void setQueryHintForSelectLowerBound(String queryHintForSelectLowerBound) {
        TtlConfigUtil.queryHintForSelectLowerBound = queryHintForSelectLowerBound;
    }

    public static String getQueryHintForDeleteExpiredData() {
        return queryHintForDeleteExpiredData;
    }

    public static void setQueryHintForDeleteExpiredData(String queryHintForDeleteExpiredData) {
        TtlConfigUtil.queryHintForDeleteExpiredData = queryHintForDeleteExpiredData;
    }

    public static String getQueryHintForInsertExpiredData() {
        return queryHintForInsertExpiredData;
    }

    public static void setQueryHintForInsertExpiredData(String queryHintForInsertExpiredData) {
        TtlConfigUtil.queryHintForInsertExpiredData = queryHintForInsertExpiredData;
    }

    public static Long getDefaultGroupParallelismOnDqlConn() {
        return defaultGroupParallelismOnDqlConn;
    }

    public static void setDefaultGroupParallelismOnDqlConn(Long defaultGroupParallelismOnDqlConn) {
        TtlConfigUtil.defaultGroupParallelismOnDqlConn = defaultGroupParallelismOnDqlConn;
    }

    public static Long getDefaultGroupParallelismOnDmlConn() {
        return defaultGroupParallelismOnDmlConn;
    }

    public static void setDefaultGroupParallelismOnDmlConn(Long defaultGroupParallelismOnDmlConn) {
        TtlConfigUtil.defaultGroupParallelismOnDmlConn = defaultGroupParallelismOnDmlConn;
    }

    public static String getQueryHintForOptimizeTable() {
        return queryHintForOptimizeTable;
    }

    public static boolean isAutoAddMaxValuePartForCci() {
        return autoAddMaxValuePartForCci;
    }

    public static void setAutoAddMaxValuePartForCci(boolean autoAddMaxValuePartForCci) {
        TtlConfigUtil.autoAddMaxValuePartForCci = autoAddMaxValuePartForCci;
    }

    public static void setQueryHintForOptimizeTable(String queryHintForOptimizeTable) {
        TtlConfigUtil.queryHintForOptimizeTable = queryHintForOptimizeTable;
    }

    public static String getQueryHintForAutoAddParts() {
        return queryHintForAutoAddParts;
    }

    public static void setQueryHintForAutoAddParts(String queryHintForAutoAddParts) {
        TtlConfigUtil.queryHintForAutoAddParts = queryHintForAutoAddParts;
    }

    public static long getMaxWaitAcquireRatePermitsPeriods() {
        return maxWaitAcquireRatePermitsPeriods;
    }

    public static void setMaxWaitAcquireRatePermitsPeriods(long maxWaitAcquireRatePermitsPeriods) {
        TtlConfigUtil.maxWaitAcquireRatePermitsPeriods = maxWaitAcquireRatePermitsPeriods;
    }

    public static boolean isEnableTtlCleanupRowsSpeedLimit() {
        return enableTtlCleanupRowsSpeedLimit;
    }

    public static void setEnableTtlCleanupRowsSpeedLimit(boolean enableTtlCleanupRowsSpeedLimit) {
        TtlConfigUtil.enableTtlCleanupRowsSpeedLimit = enableTtlCleanupRowsSpeedLimit;
    }

    public static long getCleanupRowsSpeedLimitEachDn() {
        return cleanupRowsSpeedLimitEachDn;
    }

    public static void setCleanupRowsSpeedLimitEachDn(long cleanupRowsSpeedLimitEachDn) {
        TtlConfigUtil.cleanupRowsSpeedLimitEachDn = cleanupRowsSpeedLimitEachDn;
    }

    public static boolean isIgnoreMaintainWindowInTtlJob() {
        return ignoreMaintainWindowInTtlJob;
    }

    public static void setIgnoreMaintainWindowInTtlJob(boolean ignoreMaintainWindowInTtlJob) {
        TtlConfigUtil.ignoreMaintainWindowInTtlJob = ignoreMaintainWindowInTtlJob;
    }

    public static int getTtlGlobalWorkerDnRatio() {
        return ttlGlobalWorkerDnRatio;
    }

    public static void setTtlGlobalWorkerDnRatio(int ttlGlobalWorkerDnRatio) {
        TtlConfigUtil.ttlGlobalWorkerDnRatio = ttlGlobalWorkerDnRatio;
    }

    public static boolean isEnableAutoAddPartsForArcCci() {
        return enableAutoAddPartsForArcCci;
    }

    public static void setEnableAutoAddPartsForArcCci(boolean enableAutoAddPartsForArcCci) {
        TtlConfigUtil.enableAutoAddPartsForArcCci = enableAutoAddPartsForArcCci;
    }
}
