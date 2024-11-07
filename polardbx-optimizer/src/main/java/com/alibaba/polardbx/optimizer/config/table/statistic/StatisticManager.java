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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleInfo;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.index.CandidateIndex;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.util.JsonBuilder;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CACHELINE_COMPENSATION_BLACKLIST;
import static com.alibaba.polardbx.common.properties.ConnectionParams.CACHELINE_INDICATE_UPDATE_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionParams.ENABLE_CACHELINE_COMPENSATION;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_STATISTIC_FEEDBACK;
import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_SKIPPED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DATA_MAX_LEN;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildSketchKey;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.digestForStatisticTrace;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.CACHE_LINE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.CORRECTIONS;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.HISTOGRAM;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.HLL_SKETCH;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.MULTI;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.NULL;
import static com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.TOP_N;
import static com.alibaba.polardbx.optimizer.view.VirtualViewType.COLUMN_STATISTICS;
import static com.alibaba.polardbx.optimizer.view.VirtualViewType.STATISTICS;
import static com.alibaba.polardbx.optimizer.view.VirtualViewType.STATISTICS_DATA;
import static com.alibaba.polardbx.optimizer.view.VirtualViewType.VIRTUAL_STATISTIC;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.HLL_TASK_FAIL;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.HLL_TASK_SUCC;

public class StatisticManager extends AbstractLifecycle implements StatisticService, ModuleInfo {

    private static final Logger logger = LoggerUtil.statisticsLogger;

    private final Map<String, Map<String, CacheLine>> statisticCache = new ConcurrentHashMap<>();

    public static StatisticDataSource sds;

    private static ThreadPoolExecutor executor;

    private final Map<String, Long> cardinalitySketch = Maps.newConcurrentMap();

    private Map<String, String> correctionsMap = Maps.newConcurrentMap();

    private static StatisticManager sm;

    public StatisticManager() {
        init();
    }

    public static StatisticManager getInstance() {
        if (sm == null) {
            sm = new StatisticManager();
        }
        return sm;
    }

    public static void setExecutor(ThreadPoolExecutor executor) {
        StatisticManager.executor = executor;
    }

    @Override
    protected void doInit() {
        // Skip StatisticManager init in mock mode or build in schema
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        long start = System.currentTimeMillis();
        readStatistic(0L);
        this.executor = new ThreadPoolExecutor(
            1, 1, 1800, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(8),
            new NamedThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r, "Statistic feedback executor");
                    thread.setDaemon(true);
                    return thread;
                }
            },
            new ThreadPoolExecutor.DiscardPolicy());
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                LogPattern.START_OVER, new String[] {Module.STATISTICS.name()},
                LogLevel.NORMAL);
        long end = System.currentTimeMillis();
        logger.info("StatisticManager init consuming " + (end - start) / 1000.0 + " seconds");
    }

    @Override
    protected void doDestroy() {
        statisticCache.clear();
        cardinalitySketch.clear();
    }

    private void readStatistic(long sinceTime) {
        Collection<SystemTableTableStatistic.Row> tableRowList = getSds().loadAllTableStatistic(sinceTime);
        for (SystemTableTableStatistic.Row row : tableRowList) {
            StatisticManager.CacheLine cacheLine = getCacheLine(row.getSchema(), row.getTableName(), true);
            cacheLine.setRowCount(row.getRowCount());
            cacheLine.setLastModifyTime(row.getUnixTime());
        }

        Collection<SystemTableColumnStatistic.Row> columnRowList = getSds().loadAllColumnStatistic(sinceTime);
        for (SystemTableColumnStatistic.Row row : columnRowList) {
            StatisticManager.CacheLine cacheLine = getCacheLine(row.getSchema(), row.getTableName(), true);
            cacheLine.setCardinality(row.getColumnName(), row.getCardinality());
            cacheLine.setHistogram(row.getColumnName(), row.getHistogram());
            cacheLine.setTopN(row.getColumnName(), row.getTopN());
            cacheLine.setNullCount(row.getColumnName(), row.getNullCount());
            cacheLine.setSampleRate(row.getSampleRate());
            // handle extendField
            cacheLine.setExtend(row.getExtendField());
            cacheLine.decodeExtend();
        }
        cardinalitySketch.putAll(getSds().loadAllCardinality());
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                LogPattern.LOAD_DATA,
                new String[] {
                    "row count:" + tableRowList.size() + ",column:" + columnRowList.size() + ",ndv:"
                        + cardinalitySketch.size()
                },
                LogLevel.NORMAL);
    }

    public void clearAndReloadData() {
        statisticCache.clear();
        cardinalitySketch.clear();
        getSds().clearCache();
        readStatistic(0L);
    }

    public void reloadNDVbyTableName(String schema, String tableName) {
        getSds().reloadNDVbyTableName(schema, tableName);
        cardinalitySketch.putAll(getSds().syncCardinality());
    }

    public boolean hasNdvSketch(String schema, String tableName) {
        for (String key : cardinalitySketch.keySet()) {
            if (key.startsWith(schema + ":" + tableName + ":")) {
                return true;
            }
        }
        return false;
    }

    /*
     * Warning: to call statisticCache.get(schema) or put(), etc
     * schema must be lower case
     */
    public Map<String, Map<String, CacheLine>> getStatisticCache() {
        return statisticCache;
    }

    public CacheLine getCacheLine(String schema, String logicalTableName) {
        return getCacheLine(schema, logicalTableName, false);
    }

    public CacheLine getCacheLine(String schema, String logicalTableName, boolean byPassGsi) {

        int idx = logicalTableName.indexOf(CandidateIndex.WHAT_IF_GSI_INFIX);
        if (idx != -1) {
            logicalTableName = logicalTableName.substring(0, idx);
        }

        if (!byPassGsi) {
            logicalTableName = getSourceTableName(schema, logicalTableName);
        }
        String schemaLower = schema.toLowerCase();
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return new CacheLine();
        }
        if (!statisticCache.containsKey(schemaLower)) {
            statisticCache.put(schemaLower, Maps.newConcurrentMap());
        }
        CacheLine cacheLine = statisticCache.get(schemaLower).get(logicalTableName.toLowerCase());
        if (cacheLine == null) {
            cacheLine = new CacheLine();
            statisticCache.get(schemaLower).put(logicalTableName.toLowerCase(), cacheLine);
        }
        return cacheLine;
    }

    public static String getSourceTableName(String schema, String logicalTableName) {
        if (schema == null || logicalTableName == null) {
            return logicalTableName;
        }
        TableMeta tableMeta = null;
        try {
            tableMeta =
                OptimizerContext.getContext(schema).getLatestSchemaManager().getTableWithNull(logicalTableName);
        } catch (Throwable t) {
            // pass
        }
        if (tableMeta != null && (tableMeta.isGsi() || tableMeta.isColumnar())) {
            GsiMetaManager.GsiTableMetaBean tableMetaBean = tableMeta.getGsiTableMetaBean();
            if (tableMetaBean != null
                && tableMetaBean.gsiMetaBean != null
                && tableMetaBean.gsiMetaBean.tableName != null
                && tableMetaBean.gsiMetaBean.indexStatus == IndexStatus.PUBLIC) {
                return tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName.toLowerCase();
            }
        }
        return logicalTableName.toLowerCase();
    }

    public void setCacheLine(String schema, String logicalTableName, CacheLine cacheLine) {
        String schemaLower = schema.toLowerCase();
        Map<String, CacheLine> cacheLineMap = statisticCache.get(schemaLower);
        if (cacheLineMap == null) {
            cacheLineMap = Maps.newConcurrentMap();
            statisticCache.put(schemaLower, cacheLineMap);
        }
        cacheLineMap.put(logicalTableName.toLowerCase(), cacheLine);
    }

    /**
     * @return table row count if cache miss return 0
     */
    public StatisticResult getRowCount(String schema, String logicalTableName, boolean isNeedTrace) {
        logicalTableName = getSourceTableName(schema, logicalTableName);
        // try corrections first
        StatisticResult sr = getCorrectionResult(schema + "," + logicalTableName,
            "getRowCount", isNeedTrace);
        if (sr != null) {
            return sr;
        }

        if (SystemTables.contains(logicalTableName) || SystemDbHelper.isDBBuildIn(schema)) {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(schema + "," + logicalTableName,
                    "getRowCount", 0L, NULL,
                    -1L, "") : null;
            return StatisticResult.build().setValue(0L, statisticTrace);
        }

        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());

        long rowCount = cacheLine.getRowCount();
        StatisticTrace statisticTrace = isNeedTrace ?
            StatisticUtils.buildTrace(schema + "," + logicalTableName,
                "getRowCount", rowCount, CACHE_LINE,
                cacheLine.getLastModifyTime(), "") : null;
        return StatisticResult.build(CACHE_LINE).setValue(rowCount, statisticTrace);
    }

    protected StatisticResult getCorrectionResult(String catalog, String method, boolean isNeedTrace) {
        if (correctionsMap.isEmpty()) {
            return null;
        }
        String key = "Catalog:" + catalog + "\nAction:" + method;
        key = GeneralUtil.removeIdxSuffix(key);
        String val = correctionsMap.get(key.toLowerCase());
        if (!StringUtils.isEmpty(val)) {
            try {
                Long rs = Long.parseLong(val);
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(catalog, method, rs, CORRECTIONS, -1L, "") : null;
                return StatisticResult.build(CORRECTIONS).setValue(rs, statisticTrace);
            } catch (NumberFormatException nfe) {
                // try boolean
                if ("true".equalsIgnoreCase(val) ||
                    "false".equalsIgnoreCase(val)) {
                    boolean rs = Boolean.parseBoolean(val);
                    StatisticTrace statisticTrace = isNeedTrace ?
                        StatisticUtils.buildTrace(catalog, method, rs, CORRECTIONS, -1L, "") : null;
                    return StatisticResult.build(CORRECTIONS).setValue(rs, statisticTrace);
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    public void setRowCount(String schema, String logicalTableName, long rowCount) {
        if (SystemTables.contains(logicalTableName) || SystemDbHelper.isDBBuildIn(schema)) {
            return;
        }

        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        cacheLine.setRowCount(rowCount);
    }

    /**
     * return cardinality of a column of logicalTable
     * if not exists return -1
     */
    public StatisticResult getCardinality(String schema, String logicalTableName, String columnName,
                                          boolean fromOptimizer, boolean isNeedTrace) {
        logicalTableName = getSourceTableName(schema, logicalTableName);
        // try corrections first
        StatisticResult sr = getCorrectionResult(schema + "," + logicalTableName + "," + columnName,
            "getCardinality", isNeedTrace);
        if (sr != null) {
            return sr;
        }
        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        Long cardinality = cardinalitySketch.get(buildSketchKey(schema, logicalTableName, columnName));

        if (fromOptimizer) {
            cacheLine.setLastAccessTime(unixTimeStamp());
        }
        if (DynamicConfig.getInstance().enableHll() &&
            !isCardinalityOutliers(cardinality, columnName, cacheLine)) {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                    "getCardinality", cardinality, HLL_SKETCH,
                    sds.ndvModifyTime(schema, logicalTableName, columnName), "") : null;
            return StatisticResult.build(HLL_SKETCH).setValue(cardinality, statisticTrace);
        }

        Map<String, Long> cardinalityMap = cacheLine.getCardinalityMap();
        if (cardinalityMap != null) {
            cardinality = cardinalityMap.get(columnName.toLowerCase());
            if (cardinality == null) {
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                        "getCardinality", -1L, NULL,
                        -1, "") : null;
                return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
            } else {
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                        "getCardinality", cardinality, CACHE_LINE,
                        cacheLine.lastModifyTime, "") : null;
                return StatisticResult.build(CACHE_LINE).setValue(cardinality, statisticTrace);
            }
        } else {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                    "getCardinality", -1L, NULL,
                    -1L, "") : null;
            return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
        }
    }

    private boolean isCardinalityOutliers(Long cardinality, String columnName, CacheLine cacheLine) {
        if (StringUtils.isEmpty(columnName) || cacheLine == null) {
            throw new IllegalArgumentException("columnName or cacheLine is null");
        }
        if (cardinality == null || cardinality < 0) {
            return true;
        } else if (cardinality == 0L) {
            boolean topNNull = cacheLine.getTopN(columnName) == null;
            Histogram histogram =
                cacheLine.getHistogramMap() == null ? null : cacheLine.getHistogramMap().get(columnName);
            boolean bothNull = topNNull && histogram == null;
            // if rowcount > 0 and column topn/histogram has values, then cardinality(ndv) couldn't be 0
            if (cacheLine.getRowCount() > 0 && !bothNull) {
                return true;
            }
        }
        return false;
    }

    public StatisticResult getFrequency(String schema, String logicalTableName, String columnName,
                                        List rowValue, boolean isNeedTrace) {
        logicalTableName = getSourceTableName(schema, logicalTableName);
        // try corrections first
        StatisticResult sr = getCorrectionResult(
            schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(rowValue),
            "getFrequency", isNeedTrace);
        if (sr != null) {
            return sr;
        }

        long frequency = 0L;
        // for corner case
        if (rowValue == null) {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                    "getFrequency", -1L, NULL,
                    -1, "") : null;
            return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
        }

        // Taking performance into consideration. if the number of row value list is too much,
        // then use (rowCount/ndv)*row_size instead of calculating frequency for each row value.
        if (rowValue.size() > DynamicConfig.getInstance().getInDegradationNum()) {
            StatisticResult rowCount = getRowCount(schema, logicalTableName, isNeedTrace);
            StatisticResult ndv = getCardinality(schema, logicalTableName, columnName, true, isNeedTrace);
            return handleInFrequencyDegradation(rowCount, ndv, schema, logicalTableName, columnName, rowValue,
                isNeedTrace, frequency);
        }
        StatisticResultSource source = null;
        List<StatisticTrace> childStatisticTraces = Lists.newArrayList();
        for (Object value : rowValue) {
            StatisticResult statisticResult =
                getFrequency(schema, logicalTableName, columnName, value == null ? null : value.toString(),
                    isNeedTrace);
            if (statisticResult.getSource() != NULL) {
                frequency += statisticResult.getLongValue();
            }
            if (source == null) {
                source = statisticResult.getSource();
            } else if (source != statisticResult.getSource()) {
                source = MULTI;
            }
            if (isNeedTrace && statisticResult.getTrace() != null) {
                childStatisticTraces.add(statisticResult.getTrace());
            }
        }
        if (source == NULL) {
            frequency = -1L;
        }
        StatisticTrace statisticTrace = isNeedTrace ?
            StatisticUtils.buildTrace(
                schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(rowValue),
                "getFrequency", frequency, source, -1L, "sum") : null;
        if (statisticTrace != null) {
            statisticTrace.addChildren(childStatisticTraces);
        }
        return StatisticResult.build(source).setValue(frequency, statisticTrace);
    }

    static protected StatisticResult handleInFrequencyDegradation(StatisticResult rowCount,
                                                                  StatisticResult ndv,
                                                                  String schema,
                                                                  String logicalTableName,
                                                                  String columnName,
                                                                  List rowValue,
                                                                  boolean isNeedTrace,
                                                                  long frequency) {
        if (rowCount.getLongValue() <= 0L || ndv.getLongValue() <= 0L) {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(
                    schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(rowValue),
                    "getFrequency", frequency, CACHE_LINE, -1L, "rowCount <= 0 or ndv <= 0") : null;
            return StatisticResult.build(CACHE_LINE).setValue(0L, statisticTrace);
        }
        long average = rowCount.getLongValue() / ndv.getLongValue();
        frequency = rowValue.size() * average;
        StatisticTrace statisticTrace = isNeedTrace ?
            StatisticUtils.buildTrace(
                schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(rowValue),
                "getFrequency", frequency, CACHE_LINE, -1L,
                "(rowCount/ndv)*row_size = (" + rowCount.getLongValue() + "/" + ndv.getLongValue() + ")*"
                    + rowValue.size()) : null;
        if (statisticTrace != null) {
            statisticTrace.addChild(rowCount.getTrace());
            statisticTrace.addChild(ndv.getTrace());
        }
        return StatisticResult.build(CACHE_LINE).setValue(frequency, statisticTrace);
    }

    public StatisticResult getFrequencyUpperLimit(String schema, String logicalTableName, String columnName,
                                                  boolean isNeedTrace) {
        // try corrections first
        StatisticResult correctionsSr =
            getCorrectionResult(
                schema + "," + logicalTableName + "," + columnName,
                "getFrequencyUpperLimit", isNeedTrace);
        if (correctionsSr != null) {
            return correctionsSr;
        }

        columnName = columnName.toLowerCase();

        StatisticResult cardinality = getCardinality(schema, logicalTableName, columnName, true, isNeedTrace);
        if (cardinality.getLongValue() > 0) {
            CacheLine cacheLine = getCacheLine(schema, logicalTableName);
            TopN topN = cacheLine.getTopN(columnName);
            if (topN != null) {
                long topNCount = topN.getMaxCount();
                // for a table with 100000k rows, sample 1/1k rows which is 100k rows.
                // A value appeared more than 10k times is sampled >=5 W.H.P.
                // A value appeared less than 2k times is sampled <5 W.H.P.
                if (topNCount >= 5 && cacheLine.getSampleRate() > 0) {
                    Object statisticValue = topNCount / cacheLine.getSampleRate();
                    StatisticTrace statisticTrace = isNeedTrace ?
                        StatisticUtils.buildTrace(
                            schema + "," + logicalTableName + "," + columnName,
                            "getFrequencyUpperLimit", statisticValue,
                            TOP_N, cacheLine.getLastModifyTime(),
                            "topN count(" + topNCount + ")/sample rate(" + cacheLine.getSampleRate() + ")") : null;
                    return StatisticResult.build(TOP_N).setValue(statisticValue, statisticTrace);
                }
            }
            StatisticResult sr = getRowCount(schema, logicalTableName, isNeedTrace);
            Object statisticValue = Math.max(sr.getLongValue() / cardinality.getLongValue(), 1);
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(
                    schema + "," + logicalTableName + "," + columnName,
                    "getFrequency", statisticValue,
                    CACHE_LINE, cacheLine.getLastModifyTime(),
                    "max(rowcount(" + sr.getLongValue() + ")/cardinality(" + cardinality.getLongValue() + "), 1)") :
                null;
            return StatisticResult.build(CACHE_LINE).setValue(statisticValue, statisticTrace);
        } else {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(
                    schema + "," + logicalTableName + "," + columnName,
                    "getFrequency", cardinality.getLongValue(),
                    NULL, -1L, "").addChild(cardinality.getTrace()) : null;

            // return -1 value
            return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
        }
    }

    public StatisticResult getFrequency(String schema, String logicalTableName, String columnName, String value,
                                        boolean isNeedTrace) {
        DataType dataType = dataTypeCheck(schema, logicalTableName, columnName);
        value = truncateStringTypeValue(schema, logicalTableName, columnName, value);
        StatisticResult statisticResult = getFrequencyInner(schema, logicalTableName, columnName, value, isNeedTrace);
        if (dataType != null) {
            return datetimeTypeCompensation(schema, logicalTableName, columnName, value, true, value, true,
                isNeedTrace,
                dataType, statisticResult);
        } else {
            return statisticResult;
        }
    }

    public String truncateStringTypeValue(String schema, String logicalTableName, String columnName, String value) {
        if (StringUtils.isEmpty(value)) {
            return value;
        }
        DataType columnRealType = getRealDataType(schema, logicalTableName, columnName);
        return (DataTypeUtil.isStringType(columnRealType) && value.length() > DATA_MAX_LEN) ?
            value.substring(0, DATA_MAX_LEN) : value;
    }

    /**
     * return frequency of a value of column of logicalTable
     * if not exists return -1
     */
    public StatisticResult getFrequencyInner(String schema, String logicalTableName, String columnName, String value,
                                             boolean isNeedTrace) {
        schema = schema.toLowerCase();
        logicalTableName = logicalTableName.toLowerCase();
        columnName = columnName.toLowerCase();

        logicalTableName = getSourceTableName(schema, logicalTableName);
        // try corrections first
        StatisticResult correctionsSr =
            getCorrectionResult(
                schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(value),
                "getFrequency", isNeedTrace);
        if (correctionsSr != null) {
            return correctionsSr;
        }

        // Adaptive Processing : To decide whether to use histogram or topn or rowcount/ndv to calculate frequency
        // If topn contains this value, then use topn;
        // If cardinality was from sample process, then should use rowcount/ndv;
        // If cardinality was from ndv sketch, then we should compare it with sample ndv,
        // If two numbers are similar below a threshold, then should use histogram, otherwise use rowcount/ndv
        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        TopN topN = cacheLine.getTopN(columnName);
        if (topN != null) {
            Long topNCount = topN.rangeCount(value, true, value, true);
            if (topNCount > 0) {
                // topn branch
                Long statisticValue = (long) (topNCount / cacheLine.getSampleRate());
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(
                        schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(value),
                        "getFrequency", statisticValue,
                        TOP_N, cacheLine.getLastModifyTime(),
                        "topN count(" + topNCount + ")/sample rate(" + cacheLine.getSampleRate() + ")") : null;
                return StatisticResult.build(TOP_N).setValue(statisticValue, statisticTrace);
            }
        }

        StatisticResult cardinality = getCardinality(schema, logicalTableName, columnName, true, isNeedTrace);
        if (cardinality.getSource() == HLL_SKETCH) {
            long hllNdv = cardinality.getLongValue();

            Long sampleNdv = cacheLine.getCardinalityMap().get(columnName);
            if (sampleNdv != null &&
                GeneralUtil.isWithinPercentage(hllNdv, sampleNdv,
                    DynamicConfig.getInstance().getNdvAlikePercentageThreshold())) {
                StatisticResult statisticResult =
                    getSingleValFrequencyFromHistogram(schema, logicalTableName, columnName, value,
                        cacheLine.getHistogramMap(),
                        isNeedTrace, cacheLine.getLastModifyTime(), cacheLine.getSampleRate());
                if (statisticResult != null) {
                    return statisticResult;
                }
            }
        }

        StatisticResult sr = getRowCount(schema, logicalTableName, isNeedTrace);
        if (cardinality != null && sr != null && cardinality.getLongValue() > 0L && sr.getLongValue() != -1L) {
            Object statisticValue = Math.max(sr.getLongValue() / cardinality.getLongValue(), 1);
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(
                    schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(value),
                    "getFrequency", statisticValue,
                    CACHE_LINE, cacheLine.getLastModifyTime(),
                    "max(rowcount(" + sr.getLongValue() + ")/cardinality(" + cardinality.getLongValue() + "), 1)") :
                null;
            if (statisticTrace != null) {
                statisticTrace.addChild(sr.getTrace());
                statisticTrace.addChild(cardinality.getTrace());
            }
            return StatisticResult.build(CACHE_LINE).setValue(statisticValue, statisticTrace);
        }

        StatisticTrace statisticTrace = null;
        if (isNeedTrace) {
            statisticTrace = StatisticUtils.buildTrace(
                schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(value),
                "getFrequency", cardinality == null ? -1L : cardinality.getLongValue(),
                NULL, -1L, "");
            if (cardinality != null) {
                statisticTrace.addChild(cardinality.getTrace());
            }
            if (sr != null) {
                statisticTrace.addChild(sr.getTrace());
            }
        }

        // return -1 value
        return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
    }

    protected static StatisticResult getSingleValFrequencyFromHistogram(String schema,
                                                                        String table,
                                                                        String columnName,
                                                                        String value,
                                                                        Map<String, Histogram> histogramMap,
                                                                        boolean isNeedTrace,
                                                                        long lastModifyTime,
                                                                        float cachelineSampleRate) {
        // If two numbers are similar below exita threshold, then should use histogram
        if (histogramMap != null && histogramMap.get(columnName) != null) {
            Histogram histogram = histogramMap.get(columnName);
            if (histogram != null) {
                long rangeCountInHistogram = histogram.rangeCount(value, true, value, true);
                rangeCountInHistogram = (long) Math.max((rangeCountInHistogram) / cachelineSampleRate, 0);
                if (rangeCountInHistogram > 0) {
                    StatisticTrace statisticTrace = isNeedTrace ? StatisticUtils.buildTrace(
                        schema + "," + table + "," + columnName + "," + digestForStatisticTrace(value),
                        "getFrequency", rangeCountInHistogram, HISTOGRAM, lastModifyTime,
                        "histogram count/sample rate(" + cachelineSampleRate + ")") : null;
                    return StatisticResult.build(HISTOGRAM).setValue(rangeCountInHistogram, statisticTrace);
                }
            }
        }
        return null;
    }

    public DataType getRealDataType(String schemaName, String logicalTableName, String columnName) {

        TableMeta tableMeta;
        try {
            tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            return null;
        }

        if (tableMeta == null) {
            logger.error("no tableMeta for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            return null;
        }

        ColumnMeta cm = tableMeta.getColumn(columnName);
        return cm.getDataType();
    }

    private DataType dataTypeCheck(String schemaName, String logicalTableName, String columnName) {
        boolean enable = InstConfUtil.getBool(ENABLE_CACHELINE_COMPENSATION);
        if (!enable) {
            return null;
        }
        // blacklist check
        String blacklist = InstConfUtil.getOriginVal(CACHELINE_COMPENSATION_BLACKLIST);
        if (blacklist.contains(schemaName + "." + logicalTableName + "." + columnName + ";")) {
            return null;
        }
        CacheLine cacheLine = getCacheLine(schemaName, logicalTableName);
        if (cacheLine == null || cacheLine.hasExpire()) {
            return null;
        }
        TableMeta tableMeta;
        try {
            tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            return null;
        }

        if (tableMeta == null) {
            logger.error("no tableMeta for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            return null;
        }

        ColumnMeta cm = tableMeta.getColumn(columnName);
        if (DataTypeUtil.anyMatchSemantically(cm.getDataType(),
            DataTypes.DateType,
            DataTypes.DatetimeType,
            DataTypes.TimestampType)) {
            return cm.getDataType();
        } else {
            return null;
        }
    }

    /**
     * compensation calculation for datetime types
     */
    public StatisticResult datetimeTypeCompensation(String schema,
                                                    String logicalTableName,
                                                    String columnName,
                                                    Object lower,
                                                    boolean lowerInclusive,
                                                    Object upper,
                                                    boolean upperInclusive,
                                                    boolean isNeedTrace,
                                                    DataType dataType,
                                                    StatisticResult statisticResult) {
        long now = getNowTimeMillis();
        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        // current max val in statistic
        long dataUpValue = maxStatisticVal(cacheLine, columnName);
        if (dataUpValue == -1L) {
            return statisticResult;
        }
        long update = getStatisticUpdateTime(cacheLine);
        long start = transformDateToUnixTimestamp(lower, dataType);
        long end = transformDateToUnixTimestamp(upper, dataType);
        if (start == -1L && end == -1L) {
            return statisticResult;
        } else if (start == -1L) {
            start = dataUpValue;
        } else if (end == -1L) {
            end = dataUpValue + now - update;
        }

        long startMax = Math.max(start, dataUpValue);
        long endMin = Math.min(end, dataUpValue + now - update);
        long finalStart = startMax - now + update;
        long finalEnd = endMin - now + update;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String compensationStart = sdf.format(new Date(finalStart));
        String compensationEnd = sdf.format(new Date(finalEnd));
        StatisticResult statisticCompensation =
            getRangeCountInner(schema,
                logicalTableName,
                columnName,
                compensationStart,
                lowerInclusive,
                compensationEnd,
                upperInclusive,
                isNeedTrace);
        if (statisticCompensation.getLongValue() <= 1L) {
            return statisticResult;
        }
        long finalCount = statisticResult.getLongValue() + statisticCompensation.getLongValue();
        StatisticTrace statisticTrace = isNeedTrace ?
            StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName + "," + lower + "_" + upper,
                "datetimeTypeCompensation", finalCount, NULL, cacheLine.lastModifyTime,
                "normal val:" + statisticResult.getLongValue() +
                    "\ncompensation value " + compensationStart + "_"
                    + compensationEnd + ":"
                    + statisticCompensation.getLongValue()) : null;
        statisticResult.setValue(finalCount, statisticTrace);
        return statisticResult;
    }

    private long getNowTimeMillis() {
        // for testing and manual intervention
        long cacheLineUpdateTime = InstConfUtil.getLong(CACHELINE_INDICATE_UPDATE_TIME);
        if (cacheLineUpdateTime > 0L) {
            return cacheLineUpdateTime * 1000 + 3 * 24 * 60 * 60 * 1000L;
        }
        return System.currentTimeMillis();
    }

    private long getStatisticUpdateTime(CacheLine cacheLine) {
        long update = cacheLine.getLastModifyTime() * 1000;

        // for testing and manual intervention
        long cacheLineUpdateTime = InstConfUtil.getLong(CACHELINE_INDICATE_UPDATE_TIME);
        if (cacheLineUpdateTime > 0L) {
            update = cacheLineUpdateTime * 1000;
        }
        return update;
    }

    /**
     * transform datetime to unix timestamp
     *
     * @param obj Object to be transformed
     */
    private long transformDateToUnixTimestamp(Object obj, DataType dataType) {
        if (DataTypeUtil.equalsSemantically(dataType, DataTypes.DatetimeType) ||
            DataTypeUtil.equalsSemantically(dataType, DataTypes.TimestampType)) {
            Timestamp timestamp = (Timestamp) dataType.convertFrom(obj);
            if (timestamp == null) {
                return -1L;
            }
            return timestamp.getTime();
        } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.DateType)) {
            java.sql.Date date = (java.sql.Date) dataType.convertFrom(obj);
            if (date == null) {
                return -1L;
            }
            return date.getTime();
        }
        return -1L;
    }

    private long maxStatisticVal(CacheLine cacheLine, String columnName) {
        Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
        TopN topN = cacheLine.getTopN(columnName);
        if (histogramMap == null || topN == null) {
            return -1L;
        }
        Histogram histogram = histogramMap.get(columnName);
        if (histogram == null) {
            return -1L;
        }
        List<Histogram.Bucket> buckets = histogram.getBuckets();
        long dataUpValue = -1L;
        if (buckets.size() != 0) {
            Histogram.Bucket lastBucket = buckets.get(buckets.size() - 1);
            if (!(lastBucket.getUpper() instanceof Long)) {
                return -1L;
            }
            dataUpValue = (long) lastBucket.getUpper();
        }

        for (Object obj : topN.getValueArr()) {
            // obj should be packed long value
            if (!(obj instanceof Long)) {
                return -1L;
            }
            Long tmp = (Long) obj;
            if (dataUpValue < tmp) {
                dataUpValue = tmp;
            }
        }

        dataUpValue = TimeStorage.readTimestamp(dataUpValue).toEpochMillsForDatetime();
        return dataUpValue;
    }

    /**
     * return null value count of a column of logicalTable
     * if not exists return -1
     */
    public StatisticResult getNullCount(String schema, String logicalTableName, String columnName,
                                        boolean isNeedTrace) {
        logicalTableName = getSourceTableName(schema, logicalTableName);
        // try corrections first
        StatisticResult sr = getCorrectionResult(schema + "," + logicalTableName + "," + columnName,
            "getNullCount", isNeedTrace);
        if (sr != null) {
            return sr;
        }

        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Long> nullCountMap = cacheLine.getNullCountMap();
        if (nullCountMap != null) {
            Long nullCount = nullCountMap.get(columnName.toLowerCase());
            if (nullCount == null) {
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                        "getNullCount", -1L, NULL,
                        -1, "") : null;
                return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
            } else {
                long statisticValue = (long) (nullCount / cacheLine.getSampleRate());
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                        "getNullCount", statisticValue,
                        CACHE_LINE, cacheLine.getLastModifyTime(),
                        "null count(" + nullCount + ")/sample rate(" + cacheLine.getSampleRate() + ")") :
                    null;
                return StatisticResult.build(CACHE_LINE).setValue(statisticValue, statisticTrace);
            }
        } else {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName,
                    "getNullCount", -1L, NULL,
                    -1, "") : null;
            return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
        }
    }

    /**
     * return Histogram
     * if not exists return null
     */
    private Histogram getHistogram(String schema, String logicalTableName, String columnName) {
        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
        if (histogramMap != null) {
            return histogramMap.get(columnName.toLowerCase());
        }
        return null;
    }

    public String getHistogramSerializable(String schema, String logicalTableName, String columnName) {
        Histogram histogram = getHistogram(schema, logicalTableName, columnName);
        if (histogram == null) {
            return null;
        }
        return Histogram.serializeToJson(histogram);
    }

    public DataType getDataType(String schema, String logicalTableName, String columnName) {
        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
        if (histogramMap != null && histogramMap.get(columnName.toLowerCase()) != null) {
            return histogramMap.get(columnName.toLowerCase()).getDataType();
        }
        return null;
    }

    /**
     * return range count of column value of logicalTable
     * if not exists return -1
     */
    public StatisticResult getRangeCount(String schema,
                                         String logicalTableName,
                                         String columnName,
                                         Object lower,
                                         boolean lowerInclusive,
                                         Object upper,
                                         boolean upperInclusive,
                                         boolean isNeedTrace) {
        schema = schema.toLowerCase();
        columnName = columnName.toLowerCase();

        logicalTableName = getSourceTableName(schema, logicalTableName);
        DataType dataType = dataTypeCheck(schema, logicalTableName, columnName);
        StatisticResult statisticResult =
            getRangeCountInner(schema, logicalTableName, columnName, lower, lowerInclusive, upper, upperInclusive,
                isNeedTrace);
        if (dataType != null) {
            return datetimeTypeCompensation(schema, logicalTableName, columnName, lower, lowerInclusive, upper,
                upperInclusive,
                isNeedTrace,
                dataType, statisticResult);
        } else {
            return statisticResult;
        }
    }

    /**
     * return range count of column value of logicalTable
     * if not exists return -1
     */
    private StatisticResult getRangeCountInner(String schema, String logicalTableName, String columnName, Object lower,
                                               boolean lowerInclusive,
                                               Object upper, boolean upperInclusive, boolean isNeedTrace) {
        // try corrections first
        StatisticResult sr =
            getCorrectionResult(
                schema + "," + logicalTableName + "," + columnName + "," + digestForStatisticTrace(lower) + "_"
                    + digestForStatisticTrace(upper),
                "getRangeCount", isNeedTrace);
        if (sr != null) {
            return sr;
        }

        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        cacheLine.setLastAccessTime(unixTimeStamp());
        Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
        if (histogramMap != null && histogramMap.get(columnName.toLowerCase()) != null) {
            Histogram histogram = histogramMap.get(columnName.toLowerCase());
            if (histogram != null) {
                long rangeCountInHistogram = histogram.rangeCount(lower, lowerInclusive, upper, upperInclusive);
                TopN topN = cacheLine.getTopN(columnName);
                if (topN != null) {
                    long rangeCountInTopN = topN.rangeCount(lower, lowerInclusive, upper, upperInclusive);
                    rangeCountInHistogram += rangeCountInTopN;
                }

                long rangeCount = (long) (rangeCountInHistogram / cacheLine.getSampleRate());
                long statisticValue = Math.max(rangeCount, 1);
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(
                        schema + "," + logicalTableName + "," + columnName + ","
                            + digestForStatisticTrace(lower) + "_"
                            + digestForStatisticTrace(upper),
                        "getRangeCount", statisticValue,
                        CACHE_LINE, cacheLine.getLastModifyTime(),
                        "(long) (rangeCountInHistogram[" + rangeCountInHistogram + "] /sample rate["
                            + cacheLine.getSampleRate() + "])") :
                    null;
                return StatisticResult.build(HISTOGRAM).setValue(statisticValue, statisticTrace);
            }
        }
        StatisticTrace statisticTrace = isNeedTrace ?
            StatisticUtils.buildTrace(schema + "," + logicalTableName + "," + columnName + "," + lower + "_" + upper,
                "getRangeCount", -1L, NULL, -1, "") : null;
        return StatisticResult.build(NULL).setValue(-1L, statisticTrace);
    }

    public void addUpdateRowCount(String schema, String logicalTableName, long affectRow) {
        if (SystemTables.contains(logicalTableName) || SystemDbHelper.isDBBuildIn(schema)) {
            return;
        }
        if (affectRow == 0) {
            return;
        }
        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        long updateRowCount = cacheLine.addUpdateRowCount(affectRow);
        long originRowCount = cacheLine.getOriginRowCount();
        if (shouldCollectUpdate(schema, updateRowCount, originRowCount)) {
            cacheLine.resetUpdateRowCount();
            executor.execute(() ->
                GmsSyncManagerHelper.sync(new StatisticFeedbackSyncAction(schema, logicalTableName),
                    SystemDbHelper.DEFAULT_DB_NAME,
                    SyncScope.MASTER_ONLY));
        }
    }

    private boolean shouldCollectUpdate(String schema, long updateRowCount, long originalRowCount) {
        if (SystemDbHelper.isDBBuildIn(schema)) {
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

    public Map<String, CacheLine> getCacheLineBySchema(String schema) {
        String schemaLower = schema.toLowerCase();
        if (!statisticCache.containsKey(schemaLower)) {
            statisticCache.put(schemaLower, Maps.newConcurrentMap());
        }
        return statisticCache.get(schemaLower);
    }

    public void renameTable(String schema, String oldLogicalTableName, String newLogicalTableName) {
        if (oldLogicalTableName.equalsIgnoreCase(newLogicalTableName)) {
            return;
        }
        CacheLine cacheLine = getCacheLine(schema, oldLogicalTableName);
        Map<String, CacheLine> m = getCacheLineBySchema(schema);
        m.put(newLogicalTableName.toLowerCase(), cacheLine);
        m.remove(oldLogicalTableName.toLowerCase());
        List<String> removeList = Lists.newLinkedList();
        for (String key : cardinalitySketch.keySet()) {
            if (key.startsWith(schema + ":" + oldLogicalTableName)) {
                removeList.add(key);
            }
        }
        removeList.forEach(key -> cardinalitySketch.remove(key));
        getSds().renameTable(schema, oldLogicalTableName.toLowerCase(), newLogicalTableName.toLowerCase());
        getSds().reloadNDVbyTableName(schema, newLogicalTableName);
        cardinalitySketch.putAll(getSds().syncCardinality());
    }

    @Override
    public void removeLogicalColumnList(String schema, String logicalTableName, List<String> columnNameList) {
        CacheLine cacheLine = getCacheLine(schema, logicalTableName);
        for (String columnName : columnNameList) {
            columnName = columnName.toLowerCase();
            Map<String, Histogram> histogramMap = cacheLine.getHistogramMap();
            if (histogramMap != null) {
                histogramMap.remove(columnName);
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
        getSds().removeLogicalTableColumnList(schema, logicalTableName, columnNameList);
    }

    @Override
    public Set<String> getTableNamesCollected(String schema) {
        return Sets.newHashSet(getCacheLineBySchema(schema).keySet());
    }

    public void removeLogicalTableList(String schema, List<String> logicalTableNameList) {
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return;
        }
        if (logicalTableNameList.isEmpty()) {
            return;
        }
        String schemaLower = schema.toLowerCase();
        if (statisticCache.containsKey(schemaLower)) {
            Map<String, CacheLine> cacheLineMap = getCacheLineBySchema(schemaLower);
            for (String logicalTableName : logicalTableNameList) {
                cacheLineMap.remove(logicalTableName.toLowerCase());
            }
        }
        getSds().removeLogicalTableList(schema, logicalTableNameList);
    }

    public void removeLogicalTable(String schema, String logicalTableName) {
        schema = schema.toLowerCase(Locale.ROOT);
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return;
        }
        Map<String, CacheLine> schemaMap = this.statisticCache.get(schema);
        if (schemaMap != null) {
            schemaMap.remove(logicalTableName.toLowerCase());
        }
        List<String> tbls = Lists.newArrayList();
        tbls.add(logicalTableName);
        getSds().removeLogicalTableList(schema, tbls);
    }

    /**
     * 查询该逻辑表名对应的所有 Group,TableName
     */
    public List<Pair<String, String>> buildStatisticKey(String schema, String logicalTableName,
                                                        ExecutionContext executionContext) {
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schema).getPartitionInfoManager();
        if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
            PartitionPruneStep partitionPruneStep =
                PartitionPruneStepBuilder.genFullScanAllPhyPartsStepInfoByDbNameAndTbName(schema,
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

        TableRule tableRule = OptimizerContext.getContext(schema).getRuleManager().getTableRule(logicalTableName);
        String dbIndex;
        if (tableRule == null) {
            // 设置为同名，同名不做转化
            dbIndex = OptimizerContext.getContext(schema).getRuleManager().getDefaultDbIndex(logicalTableName);

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

    public StatisticDataSource getSds() {
        return sds;
    }

    public void removeNdvLogicalTable(String schema, String logicalTableName) {
        List<String> removeList = Lists.newLinkedList();
        for (String key : cardinalitySketch.keySet()) {
            if (key.startsWith(schema + ":" + logicalTableName)) {
                removeList.add(key);
            }
        }
        removeList.forEach(key -> cardinalitySketch.remove(key));
        sds.removeNdvLogicalTable(schema, logicalTableName);
    }

    @Override
    public String state() {
        return ModuleInfo.buildStateByArgs(
            ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION,
            ConnectionParams.ENABLE_STATISTIC_FEEDBACK,
            ConnectionParams.STATISTIC_VISIT_DN_TIMEOUT,
            ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME,
            ConnectionParams.HISTOGRAM_BUCKET_SIZE,
            ConnectionParams.SAMPLE_PERCENTAGE
        );

    }

    @Override
    public String status(long since) {
        // check how many cache line expired
        int fullCount = 0;
        int expiredCount = 0;
        for (Map<String, CacheLine> clMap : statisticCache.values()) {
            for (CacheLine cl : clMap.values()) {
                fullCount++;
                if (cl.hasExpire()) {
                    expiredCount++;
                }
            }
        }
        String expireStr = expiredCount + " expired/" + fullCount + " all cache line";

        return expireStr;
    }

    @Override
    public String resources() {
        // check how many cache line expired
        int clCount = 0;
        int cardinalitySampleSize = 0;
        int histogramSize = 0;
        int topNSize = 0;
        for (Map<String, CacheLine> clMap : statisticCache.values()) {
            for (CacheLine cl : clMap.values()) {
                clCount++;
                if (cl.cardinalityMap != null) {
                    cardinalitySampleSize += cl.cardinalityMap.size();
                }
                if (cl.histogramMap != null) {
                    histogramSize += cl.histogramMap.size();
                }
                if (cl.topNMap != null) {
                    topNSize += cl.topNMap.size();
                }
            }
        }

        StringBuilder s = new StringBuilder();
        s.append(
            "cache line info,clCount:" + clCount +
                ", cardinalitySampleSize:" + cardinalitySampleSize +
                ", histogramSize:" + histogramSize +
                ", topNSize:" + topNSize +
                ", sampleNdvSize:" + cardinalitySketch.size() +
                ", ndvSize:" + sds.loadAllCardinality().size());
        return s.toString();
    }

    @Override
    public String scheduleJobs() {
        if (!LeaderStatusBridge.getInstance().hasLeadership()) {
            return "";
        }
        return sds.scheduleJobs();
    }

    @Override
    public String views() {
        return VIRTUAL_STATISTIC + "," + STATISTICS + "," + COLUMN_STATISTICS + "," + STATISTICS_DATA;
    }

    @Override
    public String workload() {
        return null;
    }

    /**
     * schemaName:table name:columns name -> sketch
     */
    Map<String, Long> getCardinalitySketch() {
        return cardinalitySketch;
    }

    public void feedback(String schema, String table) {
        // check statistic info update time for target table, if it was updated within 1 min, then drop this feedback
        CacheLine cacheLine = getCacheLine(schema, table);
        if (cacheLine == null) {
            ModuleLogInfo.getInstance()
                .logError(Module.STATISTICS, "FEEDBACK" + schema + ":" + table, new Exception("cacheline is null"));
            return;
        }
        if (cacheLine.getLastModifyTime() > System.currentTimeMillis() / 1000 - 60) {
            ModuleLogInfo.getInstance()
                .logInfo(Module.STATISTICS, PROCESS_END, new String[] {"FEEDBACK", schema + ":" + table});
            return;
        }
        cacheLine.resetUpdateRowCount();
        executor.execute(() -> {
            feedbackJob(schema, table, cacheLine);
        });
    }

    protected void feedbackJob(String schema, String table, CacheLine cacheLine) {
        // check time again
        if (cacheLine.getLastModifyTime() > System.currentTimeMillis() / 1000 - 60) {
            ModuleLogInfo.getInstance()
                .logInfo(Module.STATISTICS, PROCESS_END, new String[] {"FEEDBACK", schema + ":" + table});
            return;
        }
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_STATISTIC_FEEDBACK)) {
            long start = System.currentTimeMillis();
            sds.sampleColumns(schema, table);
            // column statistic
            long end = System.currentTimeMillis();
            ModuleLogInfo.getInstance().logInfo(Module.STATISTICS, PROCESS_END,
                new String[] {
                    "statistic feedback " + schema + "," + table,
                    "consuming " + (end - start) / 1000.0 + " seconds"});
        } else {
            ModuleLogInfo.getInstance().logInfo(Module.STATISTICS, PROCESS_SKIPPED,
                new String[] {
                    "statistic feedback " + schema + "," + table,
                    ENABLE_STATISTIC_FEEDBACK + " is false"});
        }

    }

    public enum AutoAnalyzeState {
        RUNNING,
        WAITING
    }

    public void updateAllShardParts(String schema, String tableName, String columnName, ExecutionContext ec,
                                    ThreadPoolExecutor sketchHllExecutor) {
        try {
            getSds().updateColumnCardinality(schema, tableName, columnName, ec, sketchHllExecutor);
            String key = buildSketchKey(schema, tableName, columnName);
            cardinalitySketch.put(key, getSds().syncCardinality().get(key));
            FeatureStats.getInstance().increment(HLL_TASK_SUCC);
        } catch (Exception sqlException) {
            FeatureStats.getInstance().increment(HLL_TASK_FAIL);
            logger.error("error when updateAllShardParts:" + tableName + "," + columnName, sqlException);
        }
    }

    public void rebuildShardParts(String schema, String tableName, String columnName, ExecutionContext ec) {
        rebuildShardParts(schema, tableName, columnName, null, null);
    }

    public void rebuildShardParts(String schema, String tableName, String columnName, ExecutionContext ec,
                                  ThreadPoolExecutor sketchHllExecutor) {
        try {
            getSds().rebuildColumnCardinality(schema, tableName, columnName, ec, sketchHllExecutor);
            cardinalitySketch.putAll(getSds().syncCardinality());
            FeatureStats.getInstance().increment(HLL_TASK_SUCC);
        } catch (Exception sqlException) {
            FeatureStats.getInstance().increment(HLL_TASK_FAIL);
            logger.error("error when rebuildShardParts:" + tableName + "," + columnName, sqlException);
        }
    }

    public void resetCorrectionMap(String correction) {
        try {
            correctionsMap = GeneralUtil.decode(correction);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class CacheLine {
        public static final String SKEW_COLS = "SKEW_COLS";

        private long originRowCount = 0;
        private AtomicLong updateRowCount = new AtomicLong(0);
        private Map<String, Long> cardinalityMap = Maps.newHashMap();
        private Map<String, Long> nullCountMap = Maps.newHashMap();
        private Map<String, Histogram> histogramMap = Maps.newHashMap();
        private Map<String, TopN> topNMap = Maps.newHashMap();
        private List<Set<String>> skewCols = Lists.newArrayList();
        private float sampleRate = 1;
        private long lastModifyTime = 0;
        private long lastAccessTime = 0;
        private String extend;

        private Set<String> allNullCols = Sets.newHashSet();

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
            return originRowCount;
        }

        public void setRowCount(long rowCount) {
            originRowCount = rowCount;
            updateRowCount.set(rowCount);
        }

        public void resetUpdateRowCount() {
            updateRowCount.set(originRowCount);
        }

        public Map<String, Long> getCardinalityMap() {
            return cardinalityMap;
        }

        public void setCardinalityMap(Map<String, Long> cardinalityMap) {
            this.cardinalityMap = cardinalityMap;
        }

        public void setCardinality(String columnName, Long cardinality) {
            if (this.cardinalityMap == null) {
                this.cardinalityMap = new HashMap<>();
            }
            this.cardinalityMap.put(columnName.toLowerCase(), cardinality);
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
            this.nullCountMap.put(columnName.toLowerCase(), nullCount);
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
            this.histogramMap.put(columnName.toLowerCase(), histogram);
        }

        public void setTopN(String columnName, TopN topN) {
            if (StringUtils.isEmpty(columnName)) {
                return;
            }
            if (this.topNMap == null) {
                this.topNMap = new HashMap<>();
            }
            if (topN == null) {
                this.topNMap.remove(columnName.toLowerCase());
            } else {
                this.topNMap.put(columnName.toLowerCase(), topN);
            }
        }

        public TopN getTopN(String columnName) {
            if (StringUtils.isEmpty(columnName) || topNMap == null) {
                return null;
            }

            columnName = columnName.toLowerCase(Locale.ROOT);
            return topNMap.get(columnName);
        }

        public Set<String> getTopNColumns() {
            if (topNMap == null) {
                return Collections.emptySet();
            }
            return topNMap.keySet();
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

        public boolean hasExpire() {
            return ((topNMap == null || topNMap.isEmpty()) &&
                (histogramMap == null || histogramMap.isEmpty()) &&
                originRowCount > 0)
                ||
                (unixTimeStamp() - getLastModifyTime()) >
                    InstConfUtil.getInt(ConnectionParams.STATISTIC_EXPIRE_TIME);
        }

        public boolean hasExpireForCollection() {
            return (unixTimeStamp() - getLastModifyTime()) > InstConfUtil.getInt(
                ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME);
        }

        public static String serializeToJson(CacheLine cacheLine) {
            JSONObject cacheLineJson = new JSONObject();
            cacheLineJson.put("rowCount", cacheLine.getOriginRowCount());
            cacheLineJson.put("cardinalityMap", cacheLine.getCardinalityMap());
            cacheLineJson.put("nullCountMap", cacheLine.getNullCountMap());
            cacheLineJson.put("allNullCols", cacheLine.getAllNullCols());
            cacheLineJson.put("sampleRate", cacheLine.getSampleRate());
            cacheLineJson.put("extend", cacheLine.getExtend());

            Map<String, String> histogramMap = new HashMap<>();
            if (cacheLine.getHistogramMap() != null) {
                for (String columnName : cacheLine.getHistogramMap().keySet()) {
                    columnName = columnName.toLowerCase(Locale.ROOT);
                    histogramMap.put(columnName,
                        Histogram.serializeToJson(cacheLine.getHistogramMap().get(columnName)));
                }
            }

            cacheLineJson.put("histogramMap", histogramMap);

            Map<String, String> topNMap = new HashMap<>();
            for (String columnName : cacheLine.getTopNColumns()) {
                columnName = columnName.toLowerCase(Locale.ROOT);
                TopN topN = cacheLine.getTopN(columnName);
                if (topN == null) {
                    continue;
                }
                topNMap.put(columnName, TopN.serializeToJson(topN));
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
            JSONArray allNullJson = cacheLineJson.getJSONArray("allNullCols");
            cacheLine.getAllNullCols().addAll(allNullJson.toJavaList(String.class));

            Map<String, Long> cardinalityMap = new HashMap<>();
            JSONObject cardinalityMapJsonObject = cacheLineJson.getJSONObject("cardinalityMap");
            for (String columnName : cardinalityMapJsonObject.keySet()) {
                columnName = columnName.toLowerCase(Locale.ROOT);
                cardinalityMap.put(columnName, cardinalityMapJsonObject.getLongValue(columnName));
            }
            cacheLine.setCardinalityMap(cardinalityMap);

            Map<String, Long> nullCountMap = new HashMap<>();
            JSONObject nullCountMapJsonObject = cacheLineJson.getJSONObject("nullCountMap");
            for (String columnName : nullCountMapJsonObject.keySet()) {
                columnName = columnName.toLowerCase(Locale.ROOT);
                nullCountMap.put(columnName, nullCountMapJsonObject.getLongValue(columnName));
            }
            cacheLine.setNullCountMap(nullCountMap);

            Map<String, Histogram> histogramMap = new HashMap<>();
            JSONObject histogramMapJsonObject = cacheLineJson.getJSONObject("histogramMap");
            for (String columnName : histogramMapJsonObject.keySet()) {
                columnName = columnName.toLowerCase(Locale.ROOT);
                Histogram histogram = Histogram.deserializeFromJson(histogramMapJsonObject.getString(columnName));
                if (histogram != null) {
                    histogramMap.put(columnName, histogram);
                }
            }
            cacheLine.setHistogramMap(histogramMap);

            Map<String, TopN> topNMap = new HashMap<>();
            JSONObject topNMapJsonObject = cacheLineJson.getJSONObject("topNMap");
            for (String columnName : topNMapJsonObject.keySet()) {
                columnName = columnName.toLowerCase(Locale.ROOT);
                TopN topN = TopN.deserializeFromJson(topNMapJsonObject.getString(columnName));
                if (topN != null) {
                    topNMap.put(columnName, topN);
                }
            }
            cacheLine.setTopNMap(topNMap);
            cacheLine.setExtend(cacheLineJson.getString("extend"));
            cacheLine.decodeExtend();
            return cacheLine;
        }

        private void setTopNMap(Map<String, TopN> topNMap) {
            this.topNMap = topNMap;
        }

        public Set<String> getAllNullCols() {
            return allNullCols;
        }

        public void setAllNullCols(Set<String> allNullCols) {
            this.allNullCols = allNullCols;
        }

        public void setSkewCols(List<Set<String>> map) {
            skewCols = map;
            // refresh extend
            setExtend(encodeExtend());
        }

        /**
         * columns collections that is skew
         */
        public List<Set<String>> getSkewCols() {
            return skewCols;
        }

        public void decodeExtend() {
            if (StringUtils.isEmpty(extend)) {
                return;
            }
            this.skewCols = Lists.newArrayList();
            JSONArray jsonArray = JSON.parseObject(extend).getJSONArray(SKEW_COLS);
            if (jsonArray != null) {
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONArray columnJson = jsonArray.getJSONArray(i);
                    Set<String> columns = Sets.newHashSet();
                    for (int j = 0; j < columnJson.size(); j++) {
                        columns.add(columnJson.get(j).toString());
                    }
                    skewCols.add(columns);
                }
            }
        }

        public String getExtend() {
            return extend;
        }

        public void setExtend(String extend) {
            this.extend = extend;
        }

        public String encodeExtend() {
            final JsonBuilder jsonBuilder = new JsonBuilder();

            Map<String, Object> extendMap = Maps.newHashMap();
            extendMap.put(SKEW_COLS, skewCols);
            return jsonBuilder.toJsonString(extendMap);
        }

        public void remainColumns(List<ColumnMeta> columnList) {
            List<String> remove;
            if (cardinalityMap != null) {
                remove = getColumnsListToRemove(cardinalityMap.keySet(), columnList);
                for (String key : remove) {
                    cardinalityMap.remove(key);
                }
            }

            if (nullCountMap != null) {
                remove = getColumnsListToRemove(nullCountMap.keySet(), columnList);
                for (String key : remove) {
                    nullCountMap.remove(key);
                }
            }

            if (histogramMap != null) {
                remove = getColumnsListToRemove(histogramMap.keySet(), columnList);
                for (String key : remove) {
                    histogramMap.remove(key);
                }
            }

            if (topNMap != null) {
                remove = getColumnsListToRemove(topNMap.keySet(), columnList);
                for (String key : remove) {
                    topNMap.remove(key);
                }
            }
        }

        private List<String> getColumnsListToRemove(Set<String> cols, List<ColumnMeta> columnList) {
            List<String> removeColList = Lists.newArrayList();
            for (String key : cols) {
                boolean found = false;
                for (ColumnMeta cm : columnList) {
                    if (cm.getName().equalsIgnoreCase(key)) {
                        found = true;
                    }
                }
                if (!found) {
                    removeColList.add(key);
                }
            }
            return removeColList;
        }
    }

    /**
     * check whether the statistics of table are expired
     *
     * @param schema schema name of table, can't be null
     * @param table table name, can't be null
     * @return true if the statistics are expired
     */
    public static boolean expired(String schema, String table) {
        if (!ConfigDataMode.isPolarDbX()) {
            return false;
        }
        if (StringUtils.isEmpty(schema)) {
            return false;
        }
        if (StringUtils.isEmpty(table)) {
            return false;
        }
        schema = schema.toLowerCase();
        table = table.toLowerCase();
        return StatisticManager.getInstance().getCacheLine(schema, table).hasExpire();
    }

    /**
     * check whether a list of columns can be hot
     *
     * @param schema schema name
     * @param table table name
     * @param columns columns of the table
     * @return false if no parameter can make the column set hot
     */
    public StatisticResult hotColumns(String schema, String table, Collection<String> columns, boolean isNeedTrace) {
        if (StringUtils.isEmpty(schema)) {
            return StatisticResult.build().setValue(false, null);
        }
        if (StringUtils.isEmpty(table)) {
            return StatisticResult.build().setValue(false, null);
        }
        if (CollectionUtils.isEmpty(columns)) {
            return StatisticResult.build().setValue(false, null);
        } else {
            //  to lower case and be sorted
            columns = columns.stream().map(String::toLowerCase).sorted().collect(Collectors.toList());
        }
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return StatisticResult.build().setValue(false, null);
        }

        int idx = table.indexOf(CandidateIndex.WHAT_IF_GSI_INFIX);
        if (idx != -1) {
            table = table.substring(0, idx);
        }

        TableMeta tableMeta = null;
        try {
            tableMeta =
                OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(table);
        } catch (Throwable t) {
            // pass
        }
        if (tableMeta != null && tableMeta.isGsi()) {
            GsiMetaManager.GsiTableMetaBean tableMetaBean = tableMeta.getGsiTableMetaBean();
            if (tableMetaBean != null
                && tableMetaBean.gsiMetaBean != null
                && tableMetaBean.gsiMetaBean.tableName != null
                && tableMetaBean.gsiMetaBean.indexStatus == IndexStatus.PUBLIC) {
                table = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
        }

        CacheLine cacheLine = this.getCacheLine(schema, table);
        if (CollectionUtils.isEmpty(columns)) {
            StatisticTrace statisticTrace = isNeedTrace ?
                StatisticUtils.buildTrace(schema + "," + table, "hotColumns", false, null, -1, "") : null;
            return StatisticResult.build().setValue(false, statisticTrace);
        }

        // try corrections first
        String key = StatisticUtils.skewKey(columns);
        StatisticResult sr = getCorrectionResult(schema + "," + table + "," + key, "hotColumns", isNeedTrace);
        if (sr != null) {
            return sr;
        }

        // check skew
        List<Set<String>> skewColumnsCollect = cacheLine.getSkewCols();
        for (Set<String> cols : skewColumnsCollect) {
            if (cols.containsAll(columns)) {
                StatisticTrace statisticTrace = isNeedTrace ?
                    StatisticUtils.buildTrace(schema + "," + table + "," + key, "hotColumns", true, CACHE_LINE, -1,
                        "") : null;
                return StatisticResult.build().setValue(true, statisticTrace);
            }
        }

        StatisticTrace statisticTrace = isNeedTrace ?
            StatisticUtils.buildTrace(schema + "," + table + "," + key, "hotColumns", false, CACHE_LINE, -1,
                "hot columns return false caused by skew flag not exists") : null;
        return StatisticResult.build().setValue(false, statisticTrace);
    }

}
