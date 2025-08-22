package com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author chenbhui.lch
 */
public class TtlScheduledJobStatManager extends AbstractLifecycle {

    protected static final TtlScheduledJobStatManager instance = new TtlScheduledJobStatManager();
    static DdlEngineStats.Metric METRIC_TTL_TOTAL_CLEANUP_TIMECOST =
        new DdlEngineStats.Metric("TTL_TOTAL_CLEANUP_TIMECOST"); // unit: ms
    static DdlEngineStats.Metric METRIC_TTL_TOTAL_CLEANUP_ROWS = new DdlEngineStats.Metric("TTL_TOTAL_CLEANUP_ROWS");
    // unit: bytes
    static DdlEngineStats.Metric METRIC_TTL_TOTAL_CLEANUP_ROWS_SPEED =
        new DdlEngineStats.Metric("TTL_TOTAL_CLEANUP_ROWS_SPEED");// unit: rows/s
    static DdlEngineStats.Metric METRIC_TTL_TOTAL_CLEANUP_DATA_LENGTH =
        new DdlEngineStats.Metric("TTL_TOTAL_CLEANUP_DATA_LENGTH");
    static DdlEngineStats.Metric METRIC_TTL_TOTAL_CLEANUP_SPEED = new DdlEngineStats.Metric("TTL_TOTAL_CLEANUP_SPEED");
// unit: bytes/rows

    static {
        METRIC_TTL_TOTAL_CLEANUP_TIMECOST.setValue(getInstance().getGlobalTtlJobStatInfo().getTotalCleanupTimeCost());
        METRIC_TTL_TOTAL_CLEANUP_ROWS.setValue(getInstance().getGlobalTtlJobStatInfo().getTotalCleanupRows());
        METRIC_TTL_TOTAL_CLEANUP_ROWS_SPEED.setValue(
            getInstance().getGlobalTtlJobStatInfo().getTotalCleanupRowsSpeed());
        METRIC_TTL_TOTAL_CLEANUP_DATA_LENGTH.setValue(
            getInstance().getGlobalTtlJobStatInfo().getTotalCleanupDataLength());
        METRIC_TTL_TOTAL_CLEANUP_SPEED.setValue(getInstance().getGlobalTtlJobStatInfo().getTotalCleanupSpeed());
    }

    protected final GlobalTtlJobStatInfo globalTtlJobStatInfo;
    protected volatile Map<String, Map<String, TtlJobStatInfo>> allTtlTblJobStatInfo;
    protected ReentrantLock mgrLock;

    protected GlobalJobStatCalculationTask globalJobStatCalculationTask = new GlobalJobStatCalculationTask();
    protected final ScheduledThreadPoolExecutor globalTtlJobStatCalcTaskScheduler =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("Ttl-Global-Stat-Calc-Scheduler", true),
            new ThreadPoolExecutor.DiscardPolicy());

    protected static class GlobalJobStatCalculationTask implements Runnable {
        public GlobalJobStatCalculationTask() {
        }

        @Override
        public void run() {
            TtlScheduledJobStatManager.getInstance().getGlobalTtlJobStatInfo().calcJobStatInfos();
        }
    }

//    public void reloadTtlJobStatInfos() {
//
//        mgrLock.lock();
//        try(Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
//            TableInfoManager tblInfoMgr = new TableInfoManager();
//            tblInfoMgr.setConnection(metaConn);
//            List<TtlInfoRecord> allTtlInfoRecs = tblInfoMgr.getAllTtlInfoRecords();
//
//            Map<String, TtlInfoRecord> newTtlTblRecsMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
//            for (int i = 0; i < allTtlInfoRecs.size(); i++) {
//                TtlInfoRecord rec = allTtlInfoRecs.get(i);
//                String fullTblName = buildFUllTtlTblName(rec.getTableSchema(), rec.getTableName());
//                newTtlTblRecsMap.put(fullTblName, rec);
//            }
//
//            Map<String, TtlJobStatInfo> currTtlTblJobStatMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
//            for (Map.Entry<String, Map<String, TtlJobStatInfo>> itemOfOneDb : allTtlTblJobStatInfo.entrySet()) {
//                String dbName = itemOfOneDb.getKey();
//                Map<String, TtlJobStatInfo> allTtlStatInfoOfOneDb = itemOfOneDb.getValue();
//                for (Map.Entry<String, TtlJobStatInfo> item : allTtlStatInfoOfOneDb.entrySet()) {
//                    String tblName = item.getKey();
//                    TtlJobStatInfo statInfo = item.getValue();
//                    String fullTblName = buildFUllTtlTblName(dbName, tblName);
//                    currTtlTblJobStatMap.put(fullTblName, statInfo);
//                }
//            }
//
//            /**
//             * Remove deleted-ttl-table
//             */
//            Map<String, Map<String, TtlJobStatInfo>> newAllTtlTblJobStatInfo = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
//            for(Map.Entry<String, TtlJobStatInfo> statInfoItem : currTtlTblJobStatMap.entrySet()) {
//                String fullTblName = statInfoItem.getKey();
//                TtlJobStatInfo statInfo = statInfoItem.getValue();
//                if (newTtlTblRecsMap.containsKey(fullTblName)) {
//                    String dbName = statInfo.getTableSchema();
//                    String tbName = statInfo.getTableName();
//                    Map<String ,TtlJobStatInfo> statInfoOfOneDb = newAllTtlTblJobStatInfo.get(dbName);
//                    if (statInfoOfOneDb == null) {
//                        statInfoOfOneDb = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
//                        newAllTtlTblJobStatInfo.put(dbName, statInfoOfOneDb);
//                    }
//                    statInfoOfOneDb.put(tbName, statInfo);
//                }
//            }
//
//            /**
//             * Add new-created ttl-table
//             */
//            for(Map.Entry<String, TtlInfoRecord> recItem : newTtlTblRecsMap.entrySet()) {
//                String fullTblName = recItem.getKey();
//                TtlInfoRecord rec = recItem.getValue();
//                if (!currTtlTblJobStatMap.containsKey(fullTblName)) {
//                    String dbName = rec.getTableSchema();
//                    String tbName = rec.getTableName();
//                    Map<String ,TtlJobStatInfo> statInfoOfOneDb = newAllTtlTblJobStatInfo.get(dbName);
//                    if (statInfoOfOneDb == null) {
//                        statInfoOfOneDb = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
//                        newAllTtlTblJobStatInfo.put(dbName, statInfoOfOneDb);
//                    }
//                    if (!statInfoOfOneDb.containsKey(tbName)) {
//                        TtlJobStatInfo statInfo = new TtlJobStatInfo();
//                        statInfo.setTableSchema(dbName);
//                        statInfo.setTableName(tbName);
//                        statInfoOfOneDb.put(tbName, statInfo);
//                    }
//                }
//            }
//            this.allTtlTblJobStatInfo = newAllTtlTblJobStatInfo;
//        } catch (Throwable ex) {
//            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
//        } finally {
//            mgrLock.unlock();
//        }
//    }
//    protected String buildFUllTtlTblName(String tableSchemaName, String tableName) {
//        return String.format("`%s`.`%s`", tableSchemaName, tableName).toLowerCase();
//    }

    protected TtlScheduledJobStatManager() {
        mgrLock = new ReentrantLock();
        globalTtlJobStatInfo = new GlobalTtlJobStatInfo();
        allTtlTblJobStatInfo = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    }

    protected static long divIgnoreZero(long a, long b) {
        if (b == 0) {
            return 0;
        }
        return a / b;
    }

    public static TtlScheduledJobStatManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    public static Map<String, DdlEngineStats.Metric> buildGlobalTtlMetrics() {
        Map<String, DdlEngineStats.Metric> ddlStatsForTtl = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        ddlStatsForTtl.put("TTL_TOTAL_CLEANUP_TIMECOST", METRIC_TTL_TOTAL_CLEANUP_TIMECOST);
        ddlStatsForTtl.put("TTL_TOTAL_CLEANUP_ROWS", METRIC_TTL_TOTAL_CLEANUP_ROWS);
        ddlStatsForTtl.put("TTL_TOTAL_CLEANUP_ROWS_SPEED", METRIC_TTL_TOTAL_CLEANUP_ROWS_SPEED);
        ddlStatsForTtl.put("TTL_TOTAL_CLEANUP_DATA_LENGTH", METRIC_TTL_TOTAL_CLEANUP_DATA_LENGTH);
        ddlStatsForTtl.put("TTL_TOTAL_CLEANUP_DATA_LENGTH_SPEED", METRIC_TTL_TOTAL_CLEANUP_SPEED);
        return ddlStatsForTtl;
    }

    public TtlJobStatInfo getTtlJobStatInfo(String schemaName, String tableName) {
        Map<String, TtlJobStatInfo> allTtlJobStatOfOneDb = this.allTtlTblJobStatInfo.get(schemaName);
        if (allTtlJobStatOfOneDb == null) {
            return null;
        }
        return allTtlJobStatOfOneDb.get(tableName);
    }

    public Map<String, Map<String, TtlJobStatInfo>> getAllTtlJobStatInfos() {
        return this.allTtlTblJobStatInfo;
    }

    public GlobalTtlJobStatInfo getGlobalTtlJobStatInfo() {
        return this.globalTtlJobStatInfo;
    }

    public void registerTtlTableIfNeed(String schemaName, String tableName) {
        mgrLock.lock();
        try {
            Map<String, TtlJobStatInfo> allTtlJobOfOneDb = this.allTtlTblJobStatInfo.get(schemaName);
            if (allTtlJobOfOneDb == null) {
                allTtlJobOfOneDb = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                this.allTtlTblJobStatInfo.put(schemaName, allTtlJobOfOneDb);
            }
            if (!allTtlJobOfOneDb.containsKey(tableName)) {
                allTtlJobOfOneDb.put(tableName, new TtlJobStatInfo());
            }
        } finally {
            mgrLock.unlock();
        }
    }

    public void unregisterTtlTableIfNeed(String schemaName, String tableName) {
        mgrLock.lock();
        try {
            Map<String, TtlJobStatInfo> allTtlJobOfOneDb = this.allTtlTblJobStatInfo.get(schemaName);
            if (allTtlJobOfOneDb != null) {
                allTtlJobOfOneDb.remove(tableName);
                if (allTtlJobOfOneDb.isEmpty()) {
                    this.allTtlTblJobStatInfo.remove(schemaName);
                }
            }
        } finally {
            mgrLock.unlock();
        }
    }

    @Override
    protected void doInit() {
        super.doInit();

//        globalTtlJobStatCalcTaskScheduler.scheduleWithFixedDelay(
//            globalJobStatCalculationTask,
//            1,
//            5,
//            TimeUnit.SECONDS
//        );
    }

    @Data
    public static class GlobalTtlJobStatInfo {

        protected final AtomicLong totalCleanupTimeCost = new AtomicLong(0L);
        protected final AtomicLong totalCleanupDataLength = new AtomicLong(0L);
        protected final AtomicLong totalCleanupRows = new AtomicLong(0L);

        protected final AtomicLong totalCleanupRowsSpeed = new AtomicLong(0L);
        protected final AtomicLong totalCleanupSpeed = new AtomicLong(0L);

        protected AtomicLong totalSelectSqlCount = new AtomicLong(0L);
        protected AtomicLong totalSelectSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong totalDeleteSqlCount = new AtomicLong(0L);
        protected AtomicLong totalDeleteSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong totalOptimizeSqlCount = new AtomicLong(0L);
        protected AtomicLong totalOptimizeSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong totalAddPartSqlCount = new AtomicLong(0L);
        protected AtomicLong totalAddPartSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong totalDropPartSqlCount = new AtomicLong(0L);
        protected AtomicLong totalDropPartSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong totalTtlSuccessJobCount = new AtomicLong(0L);
        protected AtomicLong totalTtlPausedJobCount = new AtomicLong(0L);

        protected long lastGlobalStatCalcTs = 0L;
        protected long lastTotalCleanupDataLength = 0L;
        protected long lastTotalCleanupTimeCost = 0L;
        protected long lastTotalCleanupRows = 0L;

        public GlobalTtlJobStatInfo() {
        }

        public void calcJobStatInfos() {
            long currTotalCleanupDataLength = totalCleanupDataLength.get();
            long currTotalCleanupTimeCost = totalCleanupTimeCost.get();
            long currTotalCleanupRows = totalCleanupRows.get();
            long currGlobalStatCalcTs = System.currentTimeMillis();

            long deltaDataLength = currTotalCleanupDataLength - lastTotalCleanupDataLength;
            long deltaRows = currTotalCleanupRows - lastTotalCleanupRows;
            long deltaTimeCast = currTotalCleanupTimeCost - lastTotalCleanupTimeCost;

            long rowsSpeed = divIgnoreZero(deltaRows * 1000, deltaTimeCast);
            long dataLenSpeed = divIgnoreZero(deltaDataLength * 1000, deltaTimeCast);
            this.totalCleanupRowsSpeed.set(rowsSpeed);
            this.totalCleanupSpeed.set(dataLenSpeed);

            this.lastTotalCleanupDataLength = currTotalCleanupDataLength;
            this.lastTotalCleanupRows = currTotalCleanupRows;
            this.lastTotalCleanupTimeCost = currTotalCleanupTimeCost;
            this.lastGlobalStatCalcTs = currGlobalStatCalcTs;
        }

        public long calcTotalCleanupAvgRowsSpeed() {
            long totalCleanupAvgRowsSpeed =
                divIgnoreZero(totalCleanupRows.get() * 1000, totalCleanupTimeCost.get());// unit: Byte/s
            return totalCleanupAvgRowsSpeed;
        }

        public long calcTotalCleanupRowsSpeed() {
            long totalCleanupRowsSpeed = 0L;
            Map<String, Map<String, TtlJobStatInfo>> allTtlJobStatInfos =
                TtlScheduledJobStatManager.getInstance().getAllTtlJobStatInfos();
            for (Map.Entry<String, Map<String, TtlJobStatInfo>> jobStatInfosOfOneDbItem : allTtlJobStatInfos.entrySet()) {
                Map<String, TtlJobStatInfo> jobStatInfoOfOneDbVal = jobStatInfosOfOneDbItem.getValue();
                for (Map.Entry<String, TtlJobStatInfo> jobStatInfoItem : jobStatInfoOfOneDbVal.entrySet()) {
                    TtlJobStatInfo jobStatInfo = jobStatInfoItem.getValue();
                    if (jobStatInfo.getCurrJobStage().equalsIgnoreCase("Finished")) {
                        continue;
                    }
                    long cleanupTimeCostVal = jobStatInfo.getCleanupTimeCost().get();//unit:ms
                    long cleanupRowsVal = jobStatInfo.getCleanupRows().get();
                    long cleanupDataLengthVal = jobStatInfo.getCleanupDataLength().get();
                    long cleanupSpeed = divIgnoreZero(cleanupDataLengthVal * 1000, cleanupTimeCostVal);// unit: Byte/s
                    long cleanupRowsSpeed = divIgnoreZero(cleanupRowsVal * 1000, cleanupTimeCostVal);//unit: rows/s
                    totalCleanupRowsSpeed += cleanupRowsSpeed;
                }
            }
            return totalCleanupRowsSpeed;
        }

        public long calcTotalCleanupSpeed() {
            long totalCleanupSpeed = 0L;
            Map<String, Map<String, TtlJobStatInfo>> allTtlJobStatInfos =
                TtlScheduledJobStatManager.getInstance().getAllTtlJobStatInfos();
            for (Map.Entry<String, Map<String, TtlJobStatInfo>> jobStatInfosOfOneDbItem : allTtlJobStatInfos.entrySet()) {
                Map<String, TtlJobStatInfo> jobStatInfoOfOneDbVal = jobStatInfosOfOneDbItem.getValue();
                for (Map.Entry<String, TtlJobStatInfo> jobStatInfoItem : jobStatInfoOfOneDbVal.entrySet()) {
                    TtlJobStatInfo jobStatInfo = jobStatInfoItem.getValue();
                    if (jobStatInfo.getCurrJobStage().equalsIgnoreCase("Finished")) {
                        continue;
                    }
                    long cleanupTimeCostVal = jobStatInfo.getCleanupTimeCost().get();//unit:ms
                    long cleanupRowsVal = jobStatInfo.getCleanupRows().get();
                    long cleanupDataLengthVal = jobStatInfo.getCleanupDataLength().get();
                    long cleanupSpeed = divIgnoreZero(cleanupDataLengthVal * 1000, cleanupTimeCostVal);// unit: Byte/s
                    long cleanupRowsSpeed = divIgnoreZero(cleanupRowsVal * 1000, cleanupTimeCostVal);//unit: rows/s
                    totalCleanupSpeed += cleanupSpeed;
                }
            }
            return totalCleanupSpeed;
        }

        public long calcTotalCleanupAvgSpeed() {
            long totalCleanupAvgSpeed =
                divIgnoreZero(totalCleanupDataLength.get() * 1000, totalCleanupTimeCost.get());// unit: Byte/s
            return totalCleanupAvgSpeed;
        }

        public long calcTotalSelectSqlAvgRt() {
            long totalSelectSqlAvgRt =
                divIgnoreZero(totalSelectSqlTimeCost.get(), totalSelectSqlCount.get());//unit: ms/r
            return totalSelectSqlAvgRt;
        }

        public long calcTotalDeleteSqlAvgRt() {
            long totalDeleteSqlAvgRt =
                divIgnoreZero(totalDeleteSqlTimeCost.get(), totalDeleteSqlCount.get());//unit: ms/r
            return totalDeleteSqlAvgRt;
        }

        public long calcTotalOptimizeSqlAvgRt() {
            long totalOptimizeSqlAvgRt =
                divIgnoreZero(totalOptimizeSqlTimeCost.get(), totalOptimizeSqlCount.get());//unit: ms/r
            return totalOptimizeSqlAvgRt;
        }

        public long calcTotalAddPartsSqlAvgRt() {
            long totalAddPartSqlAvgRt =
                divIgnoreZero(totalAddPartSqlTimeCost.get(), totalAddPartSqlCount.get());//unit: ms/r
            return totalAddPartSqlAvgRt;
        }

        public long calcTotalDropPartsSqlAvgRt() {
            long totalDropPartSqlAvgRt =
                divIgnoreZero(totalDropPartSqlTimeCost.get(), totalDropPartSqlCount.get());//unit: ms/r
            return totalDropPartSqlAvgRt;
        }

        public List<Pair<String, String>> toMetricInfo() {
            List<Pair<String, String>> keyValPairList = new ArrayList<Pair<String, String>>();

            long totalCleanupTimeCostVal = totalCleanupTimeCost.get();// unit: ms
            long totalCleanupDataLengthVal = totalCleanupDataLength.get();
            long totalCleanupRowsVal = totalCleanupRows.get();

            long totalSelectSqlCountVal = totalSelectSqlCount.get();
            long totalSelectSqlTimeCostVal = totalSelectSqlTimeCost.get();// unit: ms

            long totalDeleteSqlCountVal = totalDeleteSqlCount.get();
            long totalDeleteSqlTimeCostVal = totalDeleteSqlTimeCost.get();// unit: ms

            long totalOptimizeSqlCountVal = totalOptimizeSqlCount.get();
            long totalOptimizeSqlTimeCostVal = totalOptimizeSqlTimeCost.get();// unit: ms

            long totalAddPartSqlCountVal = totalAddPartSqlCount.get();
            long totalAddPartSqlTimeCostVal = totalAddPartSqlTimeCost.get();// unit: ms

            long totalDropPartSqlCountVal = totalDropPartSqlCount.get();
            long totalDropPartSqlTimeCostVal = totalDropPartSqlTimeCost.get();// unit: ms

            long totalTtlSuccessJobCountVal = totalTtlSuccessJobCount.get();
            long totalTtlPausedJobCountVal = totalTtlPausedJobCount.get();

            long totalCleanupAvgSpeed = calcTotalCleanupAvgSpeed();// unit: Byte/s
            long totalCleanupAvgRowsSpeed = calcTotalCleanupAvgRowsSpeed();//unit: rows/s

            long totalSelectSqlAvgRt = calcTotalSelectSqlAvgRt();//unit: ms/r
            long totalDeleteSqlAvgRt = calcTotalDeleteSqlAvgRt();//unit: ms/r

            long totalOptimizeSqlAvgRt = calcTotalOptimizeSqlAvgRt();//unit: ms/r
            long totalAddPartSqlAvgRt = calcTotalAddPartsSqlAvgRt();//unit: ms/r
            long totalDropPartSqlAvgRt = calcTotalDropPartsSqlAvgRt();//unit: ms/r

            long totalCleanupRowsSpeed = calcTotalCleanupRowsSpeed();
            long totalCleanupSpeed = calcTotalCleanupSpeed();

            keyValPairList.add(new Pair<>("TTL_TOTAL_CLEANUP_TIMECOST", String.valueOf(totalCleanupTimeCostVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_CLEANUP_DATA_LENGTH", String.valueOf(totalCleanupDataLengthVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_CLEANUP_ROWS", String.valueOf(totalCleanupRowsVal)));

            keyValPairList.add(new Pair<>("TTL_TOTAL_SELECT_SQL_COUNT", String.valueOf(totalSelectSqlCountVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_SELECT_SQL_TIMECOST", String.valueOf(totalSelectSqlTimeCostVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_DELETE_SQL_COUNT", String.valueOf(totalDeleteSqlCountVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_DELETE_SQL_TIMECOST", String.valueOf(totalDeleteSqlTimeCostVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_OPTIMIZE_SQL_COUNT", String.valueOf(totalOptimizeSqlCountVal)));
            keyValPairList.add(
                new Pair<>("TTL_TOTAL_OPTIMIZE_SQL_TIMECOST", String.valueOf(totalOptimizeSqlTimeCostVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_ADD_PART_SQL_COUNT", String.valueOf(totalAddPartSqlCountVal)));
            keyValPairList.add(
                new Pair<>("TTL_TOTAL_ADD_PART_SQL_TIMECOST", String.valueOf(totalAddPartSqlTimeCostVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_JOB_SUCCESS_COUNT", String.valueOf(totalTtlSuccessJobCountVal)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_JOB_PAUSED_COUNT", String.valueOf(totalTtlPausedJobCountVal)));

            keyValPairList.add(new Pair<>("TTL_TOTAL_CLEANUP_NOW_SPEED", String.valueOf(totalCleanupSpeed)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_CLEANUP_NOW_ROWS_SPEED", String.valueOf(totalCleanupRowsSpeed)));

            keyValPairList.add(new Pair<>("TTL_TOTAL_CLEANUP_AVG_SPEED", String.valueOf(totalCleanupAvgSpeed)));
            keyValPairList.add(
                new Pair<>("TTL_TOTAL_CLEANUP_AVG_ROWS_SPEED", String.valueOf(totalCleanupAvgRowsSpeed)));

            keyValPairList.add(new Pair<>("TTL_TOTAL_SELECT_SQL_AVG_RT", String.valueOf(totalSelectSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_DELETE_SQL_AVG_RT", String.valueOf(totalDeleteSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_OPTIMIZE_SQL_AVG_RT", String.valueOf(totalOptimizeSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_ADD_PART_AVG_RT", String.valueOf(totalAddPartSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_TOTAL_DROP_PART_AVG_RT", String.valueOf(totalDropPartSqlAvgRt)));

            return keyValPairList;
        }
    }

    @Data
    public static class TtlJobStatInfo {

        /**
         * //=========== metrics by collated from lass ddl job =========
         */
        protected String scheduledId;
        protected String tableSchema;
        protected String tableName;

        protected boolean lastJobFromScheduler = false;
        protected long lastJobBeginTs = 0;
        protected long lastJobEndTs = 0;
        protected String lastTtlColMinVal = "";
        protected String lastCleanupBound = "";
        protected String lastCleanupUpperBound = "";
        protected long lastJobDataFreePercent = 0L;
        protected long lastSelectSqlAvgRt = 0;//unit: ms
        protected long lastDeleteSqlAvgRt = 0;//unit: ms
        protected long lastOptimizeSqlAvgRt = 0;//unit: ms
        protected long lastAddPartsSqlAvgRt = 0;//unit: ms
        protected long lastCleanupTimeCost = 0L;//unit: ms
        protected long lastCleanupRows = 0L;
        protected long lastCleanupDataLength = 0L;
        protected long lastCleanupSpeed = 0;// unit: Byte/s
        protected long lastCleanupRowsSpeed = 0;//unit: rows/s
        protected BigDecimal lastTtlTblDataFreePercent = new BigDecimal(0);// unit: %
        protected long lastAcquirePermitsAvgRtNano = 0;// unit: Nano

        /**
         * //=========== metrics by collated from each ddl job =========
         */
        protected String currDataTimeVal = "";
        protected String currTtlColMinVal = "";
        protected String currCleanupBound = "";
        protected String currCleanupUpperBound = "";
        protected String currJobStage = "";

        protected long currJobBeginTs = 0;
        protected long currJobEndTs = 0;
        protected long currJobBeginDeletingTs = 0;
        protected boolean currJobFromScheduler = false;
        protected long currJobDataFreePercent = 0;
        protected long currJobDnRowsSpeedLimit = 0;
        protected boolean currJobStopByMaintainWindow = false;

        protected AtomicLong cleanupTimeCost = new AtomicLong(0L);
        protected AtomicLong cleanupRows = new AtomicLong(0L);
        protected AtomicLong cleanupDataLength = new AtomicLong(0L);
        protected AtomicLong waitPermitsTimeCostNano = new AtomicLong(0L);
        protected AtomicLong acquirePermitsCount = new AtomicLong(0L);

        protected AtomicLong selectSqlCount = new AtomicLong(0L);
        protected AtomicLong selectSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong deleteSqlCount = new AtomicLong(0L);
        protected AtomicLong deleteSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong optimizeSqlCount = new AtomicLong(0L);
        protected AtomicLong optimizeSqlTimeCost = new AtomicLong(0L);

        protected AtomicLong addPartSqlCount = new AtomicLong(0L);
        protected AtomicLong addPartSqlTimeCost = new AtomicLong(0L);// ns

        protected AtomicLong dropPartSqlCount = new AtomicLong(0L);
        protected AtomicLong dropPartSqlTimeCost = new AtomicLong(0L);// ns

        protected AtomicLong cleanedPhyPartCnt = new AtomicLong(0L);
        protected AtomicLong totalPhyPartCnt = new AtomicLong(0L);

        /**
         * //=========== metrics by computed =========
         */
        protected volatile long cleanupSpeed = 0;// unit: Byte/s
        protected volatile long cleanupRowsSpeed = 0;//unit: rows/s
        protected volatile long selectSqlAvgRt = 0;//unit: ms
        protected volatile long deleteSqlAvgRt = 0;//unit: ms
        protected volatile long optimizeSqlAvgRt = 0;//unit: ms
        protected volatile long addPartSqlAvgRt = 0;//unit: ms
        protected volatile long optimizeTableProgress = 0;// unit: %
        protected volatile BigDecimal ttlTblDataFreePercent = new BigDecimal(0);// unit: %

        public TtlJobStatInfo() {
        }

        public void resetFinishedJobStatInfo() {
            /**
             * ==== save stat info as last job ====
             */
            lastJobFromScheduler = currJobFromScheduler;
            lastJobBeginTs = currJobBeginTs;
            lastJobEndTs = currJobEndTs;
            lastTtlColMinVal = currTtlColMinVal;
            lastCleanupBound = currCleanupBound;
            lastCleanupUpperBound = currCleanupUpperBound;
            lastSelectSqlAvgRt = selectSqlAvgRt;//unit: ms
            lastDeleteSqlAvgRt = deleteSqlAvgRt;//unit: ms
            lastOptimizeSqlAvgRt = optimizeSqlAvgRt;//unit: ms
            lastAddPartsSqlAvgRt = addPartSqlAvgRt;//unit: ms
            lastCleanupTimeCost = cleanupTimeCost.get();//unit: ms
            lastCleanupRows = cleanupRows.get();
            lastCleanupDataLength = cleanupDataLength.get();
            lastCleanupSpeed = cleanupSpeed;// unit: Byte/s
            lastCleanupRowsSpeed = cleanupRowsSpeed;//unit: rows/s
            lastJobDataFreePercent = currJobDataFreePercent;// unit:%s
            lastTtlTblDataFreePercent = ttlTblDataFreePercent;// unit:%s
            lastAcquirePermitsAvgRtNano = calcAcquirePermitsAvgRtNano();

            /**
             * ==== reset curr stat info ====
             */
            currDataTimeVal = "";
            currTtlColMinVal = "";
            currCleanupBound = "";
            currCleanupUpperBound = "";
            currJobStage = "";

            currJobBeginTs = 0;
            currJobEndTs = 0;
            currJobFromScheduler = false;
            currJobDataFreePercent = 0;

            cleanupTimeCost = new AtomicLong(0L);
            cleanupRows = new AtomicLong(0L);
            cleanupDataLength = new AtomicLong(0L);

            selectSqlCount = new AtomicLong(0L);
            selectSqlTimeCost = new AtomicLong(0L);

            deleteSqlCount = new AtomicLong(0L);
            deleteSqlTimeCost = new AtomicLong(0L);

            optimizeSqlCount = new AtomicLong(0L);
            optimizeSqlTimeCost = new AtomicLong(0L);

            addPartSqlCount = new AtomicLong(0L);
            addPartSqlTimeCost = new AtomicLong(0L);

            dropPartSqlCount = new AtomicLong(0L);
            dropPartSqlTimeCost = new AtomicLong(0L);

            cleanedPhyPartCnt = new AtomicLong(0L);
            totalPhyPartCnt = new AtomicLong(0L);

            acquirePermitsCount = new AtomicLong(0);
            waitPermitsTimeCostNano = new AtomicLong(0);

            /**
             * //=========== metrics by computed =========
             */
            cleanupSpeed = 0;// unit: Byte/s
            cleanupRowsSpeed = 0;//unit: rows/s
            selectSqlAvgRt = 0;//unit: ms
            deleteSqlAvgRt = 0;//unit: ms
            optimizeSqlAvgRt = 0;//unit: ms
            addPartSqlAvgRt = 0;//unit: ms
        }

        public List<Pair<String, String>> toMetricInfo() {

            long cleanupTimeCostVal = cleanupTimeCost.get();//unit:ms
            long cleanupRowsVal = cleanupRows.get();
            long cleanupDataLengthVal = cleanupDataLength.get();

            long cleanedPhyPartCntVal = cleanedPhyPartCnt.get();
            long totalPhyPartCntVal = totalPhyPartCnt.get();

            boolean currJobEnableRowsSpeedLimit = TtlConfigUtil.isEnableTtlCleanupRowsSpeedLimit();
            long currJobDnRowsSpeedLimitVal = 0;
            if (currJobEnableRowsSpeedLimit) {
                currJobDnRowsSpeedLimitVal = TtlConfigUtil.getCleanupRowsSpeedLimitEachDn();
            }
            long ttlTblDataFreePercentLimit = TtlConfigUtil.getMaxDataFreePercentOfTtlTable();

            long cleanupSpeed = calcCleanupSpeed();// unit: Byte/s
            long cleanupRowsSpeed = calcCleanupRowsSpeed();//unit: rows/s
            long selectSqlAvgRt = calcSelectSqlAvgRt();//unit: ms
            long deleteSqlAvgRt = calcDeleteSqlAvgRt();//unit: ms
            long optimizeSqlAvgRt = calcOptimizeSqlAvgRt();//unit: ms
            long addPartSqlAvgRt = calcAddPartSqlAvgRt();//unit: ms
            long dropPartSqlAvgRt = calcDropPartSqlAvgRt();//unit: ms
            long currJobPercent = calcCurrJobPercent();
            long acquirePermitsAvgRtNano = calcAcquirePermitsAvgRtNano();//unit: nano

            List<Pair<String, String>> keyValPairList = new ArrayList<Pair<String, String>>();

            keyValPairList.add(new Pair<>("TTL_CURR_TTL_COL_MIN_VAL", String.valueOf(currTtlColMinVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_CLEANUP_BOUND", String.valueOf(currCleanupBound)));
            keyValPairList.add(new Pair<>("TTL_CURR_CLEANUP_UPPER_BOUND", String.valueOf(currCleanupUpperBound)));
            keyValPairList.add(new Pair<>("TTL_CURR_NEW_DATETIME_VAL", String.valueOf(currDataTimeVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_JOB_STAGE", String.valueOf(currJobStage)));
            keyValPairList.add(new Pair<>("TTL_CURR_JOB_BEGIN_TS", String.valueOf(currJobBeginTs)));
            keyValPairList.add(new Pair<>("TTL_CURR_JOB_END_TS", String.valueOf(currJobEndTs)));
            keyValPairList.add(new Pair<>("TTL_CURR_JOB_FROM_SCHEDULER", String.valueOf(currJobFromScheduler)));
            keyValPairList.add(
                new Pair<>("TTL_CURR_JOB_STOP_BY_MAINTAIN_WINDOW", String.valueOf(currJobStopByMaintainWindow)));

            keyValPairList.add(new Pair<>("TTL_CURR_DN_ROWS_SPEED_LIMIT", String.valueOf(currJobDnRowsSpeedLimitVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_CLEANED_PHY_PART_COUNT", String.valueOf(cleanedPhyPartCntVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_TOTAL_PHY_PART_COUNT", String.valueOf(totalPhyPartCntVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_JOB_PERCENT", String.valueOf(currJobPercent)));
            keyValPairList.add(
                new Pair<>("TTL_ACQUIRE_PERMITS_AVG_RT_NANO", String.valueOf(acquirePermitsAvgRtNano)));

            keyValPairList.add(new Pair<>("TTL_CURR_CLEANUP_TIMECOST", String.valueOf(cleanupTimeCostVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_CLEANUP_ROWS", String.valueOf(cleanupRowsVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_CLEANUP_ROWS_SPEED", String.valueOf(cleanupRowsSpeed)));
            keyValPairList.add(new Pair<>("TTL_CURR_CLEANUP_DATA_LENGTH", String.valueOf(cleanupDataLengthVal)));
            keyValPairList.add(new Pair<>("TTL_CURR_CLEANUP_SPEED", String.valueOf(cleanupSpeed)));
            keyValPairList.add(new Pair<>("TTL_CURR_SELECT_SQL_AVG_RT", String.valueOf(selectSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_CURR_DELETE_SQL_AVG_RT", String.valueOf(deleteSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_CURR_OPTIMIZE_SQL_AVG_RT", String.valueOf(optimizeSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_CURR_ADD_PART_AVG_RT", String.valueOf(addPartSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_CURR_DROP_PART_AVG_RT", String.valueOf(dropPartSqlAvgRt)));
            keyValPairList.add(
                new Pair<>("TTL_CURR_DATA_FREE_PERCENT_LIMIT", String.valueOf(ttlTblDataFreePercentLimit)));
            keyValPairList.add(new Pair<>("TTL_CURR_TTL_TBL_DATA_FREE_PERCENT", String.valueOf(ttlTblDataFreePercent)));
            keyValPairList.add(new Pair<>("TTL_CURR_OPTIMIZE_TTL_TBL_PROGRESS", String.valueOf(optimizeTableProgress)));

            keyValPairList.add(new Pair<>("TTL_LAST_JOB_FROM_SCHEDULER", String.valueOf(lastJobFromScheduler)));
            keyValPairList.add(new Pair<>("TTL_LAST_TTL_COL_MIN_VAL", String.valueOf(lastTtlColMinVal)));
            keyValPairList.add(new Pair<>("TTL_LAST_CLEANUP_BOUND", String.valueOf(lastCleanupBound)));
            keyValPairList.add(new Pair<>("TTL_LAST_CLEANUP_UPPER_BOUND", String.valueOf(lastCleanupUpperBound)));
            keyValPairList.add(new Pair<>("TTL_LAST_JOB_BEGIN_TS", String.valueOf(lastJobBeginTs)));
            keyValPairList.add(new Pair<>("TTL_LAST_JOB_END_TS", String.valueOf(lastJobEndTs)));
            keyValPairList.add(new Pair<>("TTL_LAST_SELECT_SQL_AVG_RT", String.valueOf(lastSelectSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_LAST_DELETE_SQL_AVG_RT", String.valueOf(lastDeleteSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_LAST_OPTIMIZE_SQL_AVG_RT", String.valueOf(lastOptimizeSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_LAST_ADD_PARTS_SQL_AVG_RT", String.valueOf(lastAddPartsSqlAvgRt)));
            keyValPairList.add(new Pair<>("TTL_LAST_CLEANUP_TIMECOST", String.valueOf(lastCleanupTimeCost)));
            keyValPairList.add(new Pair<>("TTL_LAST_CLEANUP_ROWS", String.valueOf(lastCleanupRows)));
            keyValPairList.add(new Pair<>("TTL_LAST_CLEANUP_ROWS_SPEED", String.valueOf(lastCleanupRowsSpeed)));
            keyValPairList.add(new Pair<>("TTL_LAST_CLEANUP_DATA_LENGTH", String.valueOf(lastCleanupDataLength)));
            keyValPairList.add(new Pair<>("TTL_LAST_CLEANUP_SPEED", String.valueOf(lastCleanupSpeed)));
            keyValPairList.add(
                new Pair<>("TTL_LAST_TTL_TBL_DATA_FREE_PERCENT", String.valueOf(lastTtlTblDataFreePercent)));

            return keyValPairList;
        }

        public long calcAddPartSqlAvgRt() {
            long addPartSqlAvgRtVal = divIgnoreZero(addPartSqlTimeCost.get(), addPartSqlCount.get());
            return addPartSqlAvgRtVal;
        }

        public long calcDropPartSqlAvgRt() {
            long dropPartSqlAvgRtVal = divIgnoreZero(dropPartSqlTimeCost.get(), dropPartSqlCount.get());
            return dropPartSqlAvgRtVal;
        }

        public long calcOptimizeSqlAvgRt() {
            long optimizeSqlAvgRtVal = divIgnoreZero(optimizeSqlTimeCost.get(), optimizeSqlCount.get());
            this.optimizeSqlAvgRt = optimizeSqlAvgRtVal;
            return optimizeSqlAvgRtVal;
        }

        public long calcDeleteSqlAvgRt() {
            long deleteSqlAvgRtVal = divIgnoreZero(deleteSqlTimeCost.get(), deleteSqlCount.get());
            this.deleteSqlAvgRt = deleteSqlAvgRtVal;
            return deleteSqlAvgRtVal;
        }

        public long calcSelectSqlAvgRt() {
            long selectSqlAvgRtVal = divIgnoreZero(selectSqlTimeCost.get(), selectSqlCount.get());
            this.selectSqlAvgRt = selectSqlAvgRtVal;
            return selectSqlAvgRtVal;
        }

        public long calcDeleteRowsSpeed() {
            long deleteRowSpeedVal = divIgnoreZero(cleanupRows.get() * 1000, deleteSqlTimeCost.get());
            return deleteRowSpeedVal;
        }

        public long calcCleanupRowsSpeed() {
            long cleanupRowsSpeedVal = divIgnoreZero(cleanupRows.get() * 1000, cleanupTimeCost.get());
            this.cleanupRowsSpeed = cleanupRowsSpeedVal;
            return cleanupRowsSpeedVal;
        }

        public long calcCleanupSpeed() {
            long cleanupSpeedVal = divIgnoreZero(cleanupDataLength.get() * 1000, cleanupTimeCost.get());
            this.cleanupSpeed = cleanupSpeedVal;
            return cleanupSpeedVal;
        }

        public long calcCurrJobPercent() {
            long currJobPercent = divIgnoreZero(cleanedPhyPartCnt.get() * 100, totalPhyPartCnt.get());
            return currJobPercent;
        }

        public long calcAcquirePermitsAvgRtNano() {
            long currJobPercent = divIgnoreZero(waitPermitsTimeCostNano.get(), acquirePermitsCount.get());
            return currJobPercent;
        }
    }
}
