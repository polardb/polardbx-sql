/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.NDVSketchService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticCollector;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row;
import com.alibaba.polardbx.optimizer.index.CandidateIndex;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Base64;

import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.CACHE_LINE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.HEAVY_HITTER;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.HISTOGRAM;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.MULTI;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.TOP_N;

public class StatisticManager extends AbstractLifecycle implements StatisticService {

    private static final Logger logger = LoggerFactory.getLogger("statistics");

    private Map<String, CacheLine> statisticCache = new ConcurrentHashMap<>();

    private final String schemaName;

    private ScheduledThreadPoolExecutor scheduler;

    private ThreadPoolExecutor executor;

    private StatisticCollector statisticCollector;

    private boolean alreadyStartCollection = false;

    private SystemTableTableStatistic systemTableTableStatistic;

    private SystemTableColumnStatistic systemTableColumnStatistic;

    private SystemTableNDVSketchStatistic ndvSketchStatistic;

    private static boolean USE_HEAVY_HITTER = false;

    private AutoAnalyzeTask autoAnalyzeTask;

    private StatisticLogInfo statisticLogInfo;

    private Boolean supportHLL = null;

    /**
     * TDataSource connection properties manager
     */
    private final ParamManager paramManager;

    private NDVSketchService ndvSketch;

    public StatisticManager(String schemaName,
                            SystemTableTableStatistic systemTableTableStatistic,
                            SystemTableColumnStatistic systemTableColumnStatistic,
                            SystemTableNDVSketchStatistic ndvSketchStatistic,
                            NDVSketchService ndvSketch,
                            Map<String, Object> connectionProperties) {
        this.schemaName = schemaName;
        this.systemTableTableStatistic = systemTableTableStatistic;
        this.systemTableColumnStatistic = systemTableColumnStatistic;
        this.ndvSketchStatistic = ndvSketchStatistic;
        this.ndvSketch = ndvSketch;
        this.paramManager = new ParamManager(connectionProperties);
        this.statisticLogInfo = new StatisticLogInfo();
    }

    @Override
    protected void doInit() {
        // Skip StatisticManager init in mock mode or build in schema
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return;
        }
        this.scheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "StatisticManager scheduler");
                thread.setDaemon(true);
                return thread;
            }
        });
        this.executor = new ThreadPoolExecutor(
            1, 1, 1800, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(8),
            new NamedThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r, "StatisticManager executor");
                    thread.setDaemon(true);
                    return thread;
                }
            },
            new ThreadPoolExecutor.DiscardPolicy());
        long start = System.currentTimeMillis();
        if (ConfigDataMode.isMasterMode()) {
            systemTableTableStatistic.createTableIfNotExist();
            systemTableColumnStatistic.createTableIfNotExist();
            if (ndvSketchStatistic != null) {
                ndvSketchStatistic.createTableIfNotExist();
            }
        }
        readStatistic();

        long end = System.currentTimeMillis();
        logger.info("StatisticManager init consuming " + (end - start) / 1000.0 + " seconds");
    }

    @Override
    protected void doDestroy() {
        if (scheduler == null && executor == null) {
            return;
        }
        scheduler.shutdown();
        executor.shutdown();
        try {
            scheduler.awaitTermination(20, TimeUnit.SECONDS);
            executor.awaitTermination(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("StatisticManager scheduler/executor awaitTermination error", e);
        }
        scheduler.shutdownNow();
        executor.shutdownNow();
        statisticCache.clear();
    }

    private void readStatistic() {
        systemTableTableStatistic.selectAll(this, 0);
        systemTableColumnStatistic.selectAll(this, 0);
        reloadNDV();
    }

    private void readStatistic(long sinceTime) {
        systemTableTableStatistic.selectAll(this, sinceTime);
        systemTableColumnStatistic.selectAll(this, sinceTime);
    }

    private void reloadNDV() {
        ndvSketch.parse(ndvSketchStatistic.loadAll(schemaName));
    }

    public void reloadNDVbyTableName(String tableName) {
        ndvSketch.parse(ndvSketchStatistic.loadByTableName(schemaName, tableName));
    }

    public void startCollectForeverAsync(StatisticCollector statisticCollector) {
        if (alreadyStartCollection) {
            return;
        } else {
            alreadyStartCollection = true;
        }
        this.statisticCollector = statisticCollector;

        // stop statistic scheduler job
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return;
        }

        scheduler.scheduleWithFixedDelay(statisticCollector,
            statisticCollector.getInitialDelay(),
            statisticCollector.getPeriod(),
            statisticCollector.getTimeUnit());

        /** auto analyze */
        startAutoAnalyze();

        /** auto analyze ndv */
        startAutoAnalyzeNdv();

        scheduler.scheduleWithFixedDelay(new Runnable() {

            private long lastReadTime = unixTimeStamp();

            @Override
            public void run() {
                MDC.put(MDC.MDC_KEY_APP, getSchemaName().toLowerCase());
                if (paramManager.getBoolean(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION)) {
                    readStatistic(lastReadTime);
                    lastReadTime = unixTimeStamp();
                }
            }
        }, 300, 300, TimeUnit.SECONDS);

        logger.info("startCollectForeverAsync");
    }

    private void startAutoAnalyzeNdv() {
        scheduler.scheduleWithFixedDelay(() -> {
            MDC.put(MDC.MDC_KEY_APP, getSchemaName().toLowerCase());
            if (!paramManager.getBoolean(ConnectionParams.ENABLE_HLL)) {
                StatisticUtils.logInfo(schemaName, "ndv stopped by config: ENABLE_HLL");
                return;
            }
            StatisticUtils.logInfo(schemaName, "start detecting ndv");
            Map<String, Set<String>> tableColumnsMap =
                OptimizerContext.getContext(schemaName).getPlanManager().columnsInvolvedByPlan();
            for (String t : tableColumnsMap.keySet()) {
                long rowCount = getCacheLine(t).getRowCount();
                // small table use cache_line
                if (rowCount < DEFAULT_SAMPLE_SIZE) {
                    continue;
                }

                Set<String> cols = tableColumnsMap.get(t);
                for (String col : cols) {
                    try {
                        ndvSketch.updateStockShardParts(t, col);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        // stop by sql exception
                        return;
                    }
                }
            }
        }, 300, 300, TimeUnit.SECONDS);
    }

    private void startAutoAnalyze() {
        LocalTime startTime = LocalTime.of(2, 0, 0);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, 1);
        calendar.set(Calendar.HOUR_OF_DAY, startTime.getHour());
        calendar.set(Calendar.MINUTE, startTime.getMinute());
        calendar.set(Calendar.SECOND, startTime.getSecond());

        long delay = calendar.getTimeInMillis() - System.currentTimeMillis();
        int autoAnalyzePeriodInHours = paramManager.getInt(ConnectionParams.AUTO_ANALYZE_PERIOD_IN_HOURS);
        autoAnalyzeTask =
            new AutoAnalyzeTask(schemaName, statisticLogInfo, delay, autoAnalyzePeriodInHours, statisticCollector);
        scheduler.scheduleWithFixedDelay(autoAnalyzeTask, delay, autoAnalyzePeriodInHours * 3600 * 1000,
            TimeUnit.MILLISECONDS);
    }

    public void startCollectOnceSync() {
        if (SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return;
        }
        if (statisticCollector != null) {
            statisticCollector.collectTables();
        }
        logger.info("startCollectOnceSync");
    }

    public Map<String, CacheLine> getStatisticCache() {
        return statisticCache;
    }

    public CacheLine getCacheLine(String logicalTableName) {
        return getCacheLine(logicalTableName, false);
    }

    public CacheLine getCacheLine(String logicalTableName, boolean byPassGsi) {

        int idx = logicalTableName.indexOf(CandidateIndex.WHAT_IF_GSI_INFIX);
        if (idx != -1) {
            logicalTableName = logicalTableName.substring(0, idx);
        }

        if (!byPassGsi) {
            TableMeta tableMeta = null;
            try {
                tableMeta =
                    OptimizerContext.getContext(getSchemaName()).getLatestSchemaManager().getTable(logicalTableName);
            } catch (Throwable t) {
                // pass
            }
            if (tableMeta != null && tableMeta.isGsi()) {
                GsiMetaManager.GsiTableMetaBean tableMetaBean = tableMeta.getGsiTableMetaBean();
                if (tableMetaBean != null
                    && tableMetaBean.gsiMetaBean != null
                    && tableMetaBean.gsiMetaBean.tableName != null
                    && tableMetaBean.gsiMetaBean.indexStatus == IndexStatus.PUBLIC) {
                    logicalTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                }
            }
        }
        CacheLine cacheLine = statisticCache.get(logicalTableName.toLowerCase());
        if (cacheLine == null) {
            cacheLine = new CacheLine();
            statisticCache.put(logicalTableName.toLowerCase(), cacheLine);
        }
        return cacheLine;
    }

    public void setCacheLine(String logicalTableName, CacheLine cacheLine) {
        statisticCache.put(logicalTableName.toLowerCase(), cacheLine);
    }

    /**
     * @return table row count if cache miss return 0
     */
    public StatisticResult getRowCount(String logicalTableName) {
        if (SystemTables.contains(logicalTableName) || SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return StatisticResult.build().setValue(0L);
        }

        CacheLine cacheLine = getCacheLine(logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        return StatisticResult.build(CACHE_LINE).setValue(cacheLine.getRowCount());
    }

    public void setRowCount(String logicalTableName, long rowCount) {
        if (SystemTables.contains(logicalTableName) || SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return;
        }

        long currTime = unixTimeStamp();
        CacheLine cacheLine = getCacheLine(logicalTableName);
        cacheLine.setRowCount(rowCount);
        cacheLine.setLastModifyTime(currTime);
    }

    /**
     * return cardinality of a column of logicalTable
     * if not exists return -1
     */
    public StatisticResult getCardinality(String logicalTableName, String columnName) {
        CacheLine cacheLine = getCacheLine(logicalTableName);
        if (ndvSketch != null) {
            StatisticResult c = ndvSketch.getCardinality(logicalTableName, columnName);
            if (c != StatisticResult.EMPTY) {
                return c;
            }
        }

        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Long> cardinalityMap = cacheLine.getCardinalityMap();
        if (cardinalityMap != null) {
            Long cardinality = cardinalityMap.get(columnName.toLowerCase());
            if (cardinality == null) {
                return StatisticResult.EMPTY;
            } else {
                return StatisticResult.build(CACHE_LINE).setValue(cardinality);
            }
        } else {
            return StatisticResult.EMPTY;
        }
    }

    public StatisticResult getFrequency(String logicalTableName, String columnName, Row.RowValue rowValue) {
        long frequency = 0L;
        // for corner case
        if (rowValue == null) {
            return StatisticResult.EMPTY;
        }
        for (Object value : rowValue.getValues()) {
            StatisticResult statisticResult = getFrequency(logicalTableName, columnName, value.toString());
            if (statisticResult != StatisticResult.EMPTY) {
                frequency += statisticResult.getLongValue();
            }
        }
        if (frequency == 0L) {
            return StatisticResult.EMPTY;
        }
        return StatisticResult.build(MULTI).setValue(frequency);
    }

    /**
     * return frequency of a value of column of logicalTable
     * if not exists return -1
     */
    public StatisticResult getFrequency(String logicalTableName, String columnName, String value) {
        columnName = columnName.toLowerCase();
        // heavy hitter code
        if (USE_HEAVY_HITTER) {
            CacheLine cacheLine = getCacheLine(logicalTableName);
            cacheLine.setLastAccessTime(unixTimeStamp());
            Map<String, CountMinSketch> countMinSketchMap = cacheLine.getCountMinSketchMap();
            if (countMinSketchMap != null && countMinSketchMap.get(columnName) != null) {
                CountMinSketch countMinSketch = countMinSketchMap.get(columnName);
                if (isHeavyHitter(countMinSketch, value)) {
                    return StatisticResult.build(HEAVY_HITTER)
                        .setValue((long) (countMinSketch.estimateCount(value) / cacheLine.getSampleRate()));
                }
            }
        }

        StatisticResult cardinality = getCardinality(logicalTableName, columnName);
        if (cardinality.getLongValue() > 0) {
            if (cardinality.getLongValue() < 100) {
                // small enough, use histogram
                StatisticResult rangeCount = getRangeCount(logicalTableName, columnName, value, true, value, true);
                if (rangeCount.getLongValue() >= 0) {
                    return rangeCount;
                }
            }
            CacheLine cacheLine = getCacheLine(logicalTableName);
            if (cacheLine.getTopNMap() != null && cacheLine.getTopNMap().get(columnName) != null) {
                TopN topN = cacheLine.getTopNMap().get(columnName);
                long topNCount = topN.rangeCount(value, true, value, true);
                if (topNCount != 0) {
                    return StatisticResult.build(TOP_N).setValue(topNCount);
                }
            }
            return StatisticResult.build(CACHE_LINE)
                .setValue(Math.max(getRowCount(logicalTableName).getLongValue() / cardinality.getLongValue(), 1));
        } else {
            return StatisticResult.EMPTY;
        }
    }

    private boolean isHeavyHitter(CountMinSketch countMinSketch, String value) {
        final int k = 20;
        long estimateCount = countMinSketch.estimateCount(value);
        long size = countMinSketch.size();
        return estimateCount > size / k;
    }

    /**
     * return null value count of a column of logicalTable
     * if not exists return -1
     */
    public StatisticResult getNullCount(String logicalTableName, String columnName) {
        CacheLine cacheLine = getCacheLine(logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Long> nullCountMap = cacheLine.getNullCountMap();
        if (nullCountMap != null) {
            Long nullCount = nullCountMap.get(columnName.toLowerCase());
            if (nullCount == null) {
                return StatisticResult.EMPTY;
            } else {
                return StatisticResult.build(CACHE_LINE).setValue((long) (nullCount / cacheLine.getSampleRate()));
            }
        } else {
            return StatisticResult.EMPTY;
        }
    }

    /**
     * return Histogram
     * if not exists return null
     */
    private Histogram getHistogram(String logicalTableName, String columnName) {
        CacheLine cacheLine = getCacheLine(logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
        if (histogramMap != null) {
            return histogramMap.get(columnName.toLowerCase());
        }
        return null;
    }

    public String getHistogramSerializable(String logicalTableName, String columnName) {
        Histogram histogram = getHistogram(logicalTableName, columnName);
        if (histogram == null) {
            return null;
        }
        return Histogram.serializeToJson(histogram);
    }

    public DataType getDataType(String logicalTableName, String columnName) {
        CacheLine cacheLine = getCacheLine(logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
        if (histogramMap != null && histogramMap.get(columnName.toLowerCase()) != null) {
            return histogramMap.get(columnName.toLowerCase()).getDataType();
        }
        return null;
    }

    /**
     * return Histograms
     * if not exists return null
     */
    public List<Histogram> getHistograms(String logicalTableName, List<String> columnNames) {
        if (columnNames == null) {
            return null;
        }
        List<Histogram> histograms = new ArrayList<>(columnNames.size());
        for (String columnName : columnNames) {
            Histogram histogram = getHistogram(logicalTableName, columnName);
            if (histogram != null) {
                histograms.add(histogram);
            }
        }
        if (histograms.size() > 0) {
            return histograms;
        }

        return null;
    }

    /**
     * return range count of column value of logicalTable
     * if not exists return -1
     */
    public StatisticResult getRangeCount(String logicalTableName, String columnName, Object lower,
                                         boolean lowerInclusive,
                                         Object upper, boolean upperInclusive) {
        CacheLine cacheLine = getCacheLine(logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
        if (histogramMap != null && histogramMap.get(columnName.toLowerCase()) != null) {
            Histogram histogram = histogramMap.get(columnName.toLowerCase());
            if (histogram != null) {
                long rangeCountInHistogram = histogram.rangeCount(lower, lowerInclusive, upper, upperInclusive);
                if (cacheLine.getTopNMap() != null) {
                    TopN topN = cacheLine.getTopNMap().get(columnName.toLowerCase());
                    if (topN != null) {
                        long rangeCountInTopN = topN.rangeCount(lower, lowerInclusive, upper, upperInclusive);
                        rangeCountInHistogram += rangeCountInTopN;
                    }
                }

                long rangeCount = (long) (rangeCountInHistogram / cacheLine.getSampleRate());
                return StatisticResult.build(HISTOGRAM).setValue(Math.max(rangeCount, 1));
            }
        }
        return StatisticResult.EMPTY;
    }

    public void addUpdateRowCount(String logicalTableName, long affectRow) {
        if (SystemTables.contains(logicalTableName) || SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return;
        }
        if (affectRow == 0) {
            return;
        }
        CacheLine cacheLine = getCacheLine(logicalTableName);
        long updateRowCount = cacheLine.addUpdateRowCount(affectRow);
        long originRowCount = cacheLine.getOriginRowCount();
        if (shouldCollectUpdate(updateRowCount, originRowCount)) {
            cacheLine.setRowCount(updateRowCount);
            collectLogicalTableAsync(logicalTableName);
        }
    }

    public void collectLogicalTableAsync(String logicalTableName) {
        if (SystemTables.contains(logicalTableName) || SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return;
        }
        executor.execute(() -> {
            MDC.put(MDC.MDC_KEY_APP, getSchemaName().toLowerCase());
            if (paramManager.getBoolean(ConnectionParams.ENABLE_STATISTIC_FEEDBACK)) {
                StatisticUtils.logInfo(schemaName,
                    "statistics feedback analyze " + logicalTableName + " tables statistics " + "start");
                long start = System.currentTimeMillis();
                statisticCollector.collectRowCount(logicalTableName); // row count statistic
                // column statistic
                AutoAnalyzeTask.analyzeTable(schemaName, logicalTableName, false, statisticCollector);
                long end = System.currentTimeMillis();
                StatisticUtils
                    .logInfo(schemaName, "statistics feedback analyze " + logicalTableName + " tables statistics "
                        + "consuming "
                        + (end - start) / 1000.0 + " seconds");
            }
        });
    }

    public StatisticCollector getStatisticCollector() {
        return statisticCollector;
    }

    private boolean shouldCollectUpdate(long updateRowCount, long originalRowCount) {
        if (SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return false;
        }
        if (updateRowCount == originalRowCount) {
            return false;
        }
        if (Math.abs(updateRowCount - originalRowCount) < 8000) {
            return false;
        }

        if (updateRowCount > originalRowCount) {
            return updateRowCount > 1.2 * originalRowCount;
        } else {
            return originalRowCount > 1.2 * updateRowCount;
        }
    }

    public void renameTable(String oldLogicalTableName, String newLogicalTableName) {
        if (oldLogicalTableName.equalsIgnoreCase(newLogicalTableName)) {
            return;
        }
        CacheLine cacheLine = getCacheLine(oldLogicalTableName);
        statisticCache.put(newLogicalTableName.toLowerCase(), cacheLine);
        statisticCache.remove(oldLogicalTableName.toLowerCase());
        systemTableTableStatistic.renameTable(oldLogicalTableName.toLowerCase(), newLogicalTableName.toLowerCase());
        systemTableColumnStatistic.renameTable(oldLogicalTableName.toLowerCase(), newLogicalTableName.toLowerCase());
    }

    public void removeLogicalColumnList(String logicalTableName, List<String> columnNameList) {
        CacheLine cacheLine = getCacheLine(logicalTableName);
        for (String columnName : columnNameList) {
            columnName = columnName.toLowerCase();
            Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
            if (histogramMap != null) {
                histogramMap.remove(columnName);
            }

            Map<String, CountMinSketch> countMinSketchMap = cacheLine.getCountMinSketchMap();
            if (countMinSketchMap != null) {
                countMinSketchMap.remove(columnName);
            }

            Map<String, Long> cardinalityMap = cacheLine.getCardinalityMap();
            if (cardinalityMap != null) {
                cardinalityMap.remove(columnName);
            }

            Map<String, Long> nullCountMap = cacheLine.getNullCountMap();
            if (nullCountMap != null) {
                nullCountMap.remove(columnName);
            }
        }
        systemTableColumnStatistic.removeLogicalTableColumnList(logicalTableName, columnNameList);
    }

    @Override
    public Set<String> getTableNamesCollected() {
        return Sets.newHashSet(statisticCache.keySet());
    }

    public void removeLogicalTableList(List<String> logicalTableNameList) {
        if (SystemDbHelper.isDBBuildIn(getSchemaName())) {
            return;
        }
        for (String logicalTableName : logicalTableNameList) {
            this.statisticCache.remove(logicalTableName.toLowerCase());
        }
        systemTableTableStatistic.removeLogicalTableList(logicalTableNameList);
        systemTableColumnStatistic.removeLogicalTableList(logicalTableNameList);
    }

    /**
     * 查询该逻辑表名对应的所有 Group,TableName
     */
    public List<Pair<String, String>> buildStatisticKey(String logicalTableName, ExecutionContext executionContext) {

        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
            PartitionPruneStep partitionPruneStep = PartitionPruneStepBuilder.generateFullScanPrueStepInfo(schemaName,
                logicalTableName, executionContext);
            PartPrunedResult partPrunedResult =
                PartitionPruner.doPruningByStepInfo(partitionPruneStep, executionContext);
            List<TargetDB> targetDbs = PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(partPrunedResult);

            List<Pair<String, String>> result = new ArrayList<>();
            for (TargetDB targetDb : targetDbs) {
                for (String tableName : targetDb.getTableNames()) {
                    result.add(Pair.of(targetDb.getDbIndex(), tableName));
                }
            }
            return result;
        }

        TableRule tableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(logicalTableName);
        String dbIndex;
        if (tableRule == null) {
            // 设置为同名，同名不做转化
            dbIndex = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(logicalTableName);

            return ImmutableList.of(new Pair<>(dbIndex, logicalTableName));
        } else {
            Map<String, Set<String>> topology = tableRule.getStaticTopology();
            if (topology == null || topology.size() == 0) {
                topology = tableRule.getActualTopology();
            }

            List<Pair<String, String>> statistics = new ArrayList<>();
            for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
                dbIndex = entry.getKey();
                Set<String> tableNames = entry.getValue();
                if (tableNames == null || tableNames.isEmpty()) {
                    continue;
                }

                for (String table : tableNames) {
                    statistics.add(new Pair<>(dbIndex, table));
                }
            }
            return statistics;
        }
    }

    public StatisticLogInfo getStatisticLogInfo() {
        return statisticLogInfo;
    }

    public AutoAnalyzeTask getAutoAnalyzeTask() {
        return autoAnalyzeTask;
    }

    public enum AutoAnalyzeState {
        RUNNING,
        WAITING
    }

    public void updateAllShardParts(String tableName, String columnName) {
        try {
            ndvSketch.updateAllShardParts(tableName, columnName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void updateStockShardParts(String tableName, String columnName) {
        try {
            ndvSketch.updateStockShardParts(tableName, columnName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void rebuildShardParts(String tableName, String columnName) {
        try {
            ndvSketch.reBuildShardParts(tableName, columnName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public String getSchemaName() {
        return schemaName;
    }

    public static class CacheLine {
        private long originRowCount = 0;
        private AtomicLong updateRowCount = new AtomicLong(0);
        private Map<String, Long> cardinalityMap;
        private Map<String, CountMinSketch> countMinSketchMap;
        private Map<String, Long> nullCountMap;
        private Map<String, Histogram> histogramMap;
        private Map<String, TopN> topNMap = Maps.newHashMap();
        private float sampleRate = 1;
        private long lastModifyTime = 0;
        private long lastAccessTime = 0;

        public CacheLine() {
        }

        public CacheLine(long rowCount, long lastModifyTime, long lastAccessTime) {
            this.originRowCount = rowCount;
            this.updateRowCount = new AtomicLong(rowCount);
            this.lastModifyTime = lastModifyTime;
            this.lastAccessTime = lastAccessTime;
        }

        public long getOriginRowCount() {
            return originRowCount;
        }

        public long addUpdateRowCount(long n) {
            long result = this.updateRowCount.addAndGet(n);
            if (result < 0) {
                this.updateRowCount.set(0);
                result = 0;
            }
            return result;
        }

        public long getRowCount() {
            return updateRowCount.get();
        }

        public void setRowCount(long rowCount) {
            originRowCount = rowCount;
            updateRowCount.set(rowCount);
        }

        public Map<String, Long> getCardinalityMap() {
            return cardinalityMap;
        }

        public void setCardinalityMap(Map<String, Long> cardinalityMap) {
            this.cardinalityMap = cardinalityMap;
        }

        public Map<String, CountMinSketch> getCountMinSketchMap() {
            return countMinSketchMap;
        }

        public void setCardinality(String columnName, Long cardinality) {
            if (this.cardinalityMap == null) {
                this.cardinalityMap = new HashMap<>();
            }
            this.cardinalityMap.put(columnName.toLowerCase(), cardinality);
        }

        public void setCountMinSketchMap(Map<String, CountMinSketch> countMinSketchMap) {
            this.countMinSketchMap = countMinSketchMap;
        }

        public void setCountMinSketch(String columnName, CountMinSketch countMinSketchMap) {
            if (this.countMinSketchMap == null) {
                this.countMinSketchMap = new HashMap<>();
            }
            this.countMinSketchMap.put(columnName, countMinSketchMap);
        }

        public Map<String, Long> getNullCountMap() {
            return nullCountMap;
        }

        public void setNullCountMap(Map<String, Long> nullCountMap) {
            this.nullCountMap = nullCountMap;
        }

        public void setNullCount(String columnName, Long nullCount) {
            if (this.nullCountMap == null) {
                this.nullCountMap = new HashMap<>();
            }
            this.nullCountMap.put(columnName, nullCount);
        }

        public Map<String, Histogram> getHistogramMap() {
            return histogramMap;
        }

        public void setHistogramMap(Map<String, Histogram> histogramMap) {
            this.histogramMap = histogramMap;
        }

        public void setHistogram(String columnName, Histogram histogram) {
            if (this.histogramMap == null) {
                this.histogramMap = new HashMap<>();
            }
            this.histogramMap.put(columnName, histogram);
        }

        public void setTopN(String columnName, TopN topN) {
            if (this.topNMap == null) {
                this.topNMap = new HashMap<>();
            }
            this.topNMap.put(columnName, topN);
        }

        public float getSampleRate() {
            return sampleRate;
        }

        public void setSampleRate(float sampleRate) {
            this.sampleRate = sampleRate;
        }

        public long getLastModifyTime() {
            return lastModifyTime;
        }

        public void setLastModifyTime(long lastModifyTime) {
            this.lastModifyTime = lastModifyTime;
        }

        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public void setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
        }

        public static String serializeToJson(CacheLine cacheLine) {
            JSONObject cacheLineJson = new JSONObject();
            cacheLineJson.put("rowCount", cacheLine.getOriginRowCount());
            cacheLineJson.put("cardinalityMap", cacheLine.getCardinalityMap());
            cacheLineJson.put("nullCountMap", cacheLine.getNullCountMap());
            cacheLineJson.put("sampleRate", cacheLine.getSampleRate());

            Map<String, String> countMinSketchMap = new HashMap<>();
            for (String columnName : cacheLine.getCountMinSketchMap().keySet()) {
                countMinSketchMap.put(columnName, Base64
                    .encodeBase64String(CountMinSketch.serialize(cacheLine.getCountMinSketchMap().get(columnName))));
            }
            cacheLineJson.put("countMinSketchMap", countMinSketchMap);

            Map<String, String> histogramMap = new HashMap<>();
            for (String columnName : cacheLine.getHistogramMap().keySet()) {
                histogramMap.put(columnName, Histogram.serializeToJson(cacheLine.getHistogramMap().get(columnName)));
            }
            cacheLineJson.put("histogramMap", histogramMap);

            Map<String, String> topNMap = new HashMap<>();
            for (String columnName : cacheLine.getTopNMap().keySet()) {
                topNMap.put(columnName, TopN.serializeToJson(cacheLine.getTopNMap().get(columnName)));
            }
            cacheLineJson.put("topNMap", topNMap);

            return cacheLineJson.toJSONString();
        }

        public static CacheLine deserializeFromJson(String json) {
            JSONObject cacheLineJson = JSON.parseObject(json);
            long rowCount = cacheLineJson.getLong("rowCount");
            long currentTime = unixTimeStamp();
            CacheLine cacheLine = new CacheLine(rowCount, currentTime, currentTime);
            cacheLine.setSampleRate(cacheLineJson.getFloatValue("sampleRate"));

            Map<String, Long> cardinalityMap = new HashMap<>();
            JSONObject cardinalityMapJsonObject = cacheLineJson.getJSONObject("cardinalityMap");
            for (String columnName : cardinalityMapJsonObject.keySet()) {
                cardinalityMap.put(columnName, cardinalityMapJsonObject.getLongValue(columnName));
            }
            cacheLine.setCardinalityMap(cardinalityMap);

            Map<String, Long> nullCountMap = new HashMap<>();
            JSONObject nullCountMapJsonObject = cacheLineJson.getJSONObject("nullCountMap");
            for (String columnName : nullCountMapJsonObject.keySet()) {
                nullCountMap.put(columnName, nullCountMapJsonObject.getLongValue(columnName));
            }
            cacheLine.setNullCountMap(nullCountMap);

            Map<String, CountMinSketch> countMinSketchMap = new HashMap<>();
            JSONObject countMinSketchMapJsonObject = cacheLineJson.getJSONObject("countMinSketchMap");
            for (String columnName : countMinSketchMapJsonObject.keySet()) {
                countMinSketchMap.put(columnName,
                    CountMinSketch.deserialize(Base64.decodeBase64(countMinSketchMapJsonObject.getString(columnName))));
            }
            cacheLine.setCountMinSketchMap(countMinSketchMap);

            Map<String, Histogram> histogramMap = new HashMap<>();
            JSONObject histogramMapJsonObject = cacheLineJson.getJSONObject("histogramMap");
            for (String columnName : histogramMapJsonObject.keySet()) {
                histogramMap
                    .put(columnName, Histogram.deserializeFromJson(histogramMapJsonObject.getString(columnName)));
            }
            cacheLine.setHistogramMap(histogramMap);

            Map<String, TopN> topNMap = new HashMap<>();
            JSONObject topNMapJsonObject = cacheLineJson.getJSONObject("topNMap");
            for (String columnName : topNMapJsonObject.keySet()) {
                topNMap.put(columnName, TopN.deserializeFromJson(topNMapJsonObject.getString(columnName)));
            }
            cacheLine.setTopNMap(topNMap);
            return cacheLine;
        }

        public Map<String, TopN> getTopNMap() {
            return topNMap;
        }

        public void setTopNMap(Map<String, TopN> topNMap) {
            this.topNMap = topNMap;
        }
    }

}
