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

package com.alibaba.polardbx.executor.statistic;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.SchemaMetaUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.slice.Slice;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.executor.statistic.MysqlStatisticCollector.USE_BC_GEE_CARDINALITY;
import static com.alibaba.polardbx.executor.statistic.MysqlStatisticCollector.USE_GEE_CARDINALITY;
import static com.alibaba.polardbx.executor.statistic.MysqlStatisticCollector.USE_HLL_CARDINALITY;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildColumnsName;

/**
 * @author dylan
 */
public class StatisticBuilder {

    private static final Logger logger = LoggerFactory.getLogger("statistics");

    private StatisticManager statisticManager;
    private DataSource tDataSource;
    private final String logicalTableName;
    private List<ColumnMeta> columnMetaList;
    private final float statisticSampleRate;
    private final int histogramMaxSampleSize;
    private int histogramBucketSize;
    private final int analyzeTableSpeedLimit;
    private boolean enableInnodbBtreeSampling;
    private int topNSize;
    private int topNMinNum;
    private boolean analyzeHll;
    private boolean isFromAnalyze;
    private Engine engine;

    private int sampleSize;
    private float sampleRate;
    private long tableCacheRowCount;
    private Object[][] sampleRows;
    private int[] columnSampleSize;
    private Random rand;
    private long rowCount = 0;

    static final int DATA_MAX_LEN = 128;

    /**
     * HLL
     */
    private List<HyperLogLog> hyperLogLogList = new ArrayList<>();
    private List<MysqlStatisticCollector.GEESample> geeSamples = new ArrayList<>();
    private Map<String, Long> cardinalityMap = new HashMap<>();
    // column -> node -> list physical table
    private Map<String, Map<String, String[]>> needUpdate = Maps.newHashMap();

    /**
     * CMSKETCHE
     */
    private List<CountMinSketch> countMinSketchList = new ArrayList<>();
    private Map<String, CountMinSketch> countMinSketchMap = new HashMap<>();

    /**
     * NULL_COUNT
     */
    private List<Long> nullCountList = new ArrayList<>();
    private Map<String, Long> nullCountMap = new HashMap<>();

    /**
     * HISTOGRAM
     */
    private List<Histogram> histogramList = new ArrayList<>();
    private Map<String, Histogram> histogramMap = new HashMap<>();

    public StatisticBuilder(StatisticManager statisticManager, DataSource tDataSource, ParamManager paramManager,
                            String logicalTableName,
                            List<ColumnMeta> columnMetaList, boolean analyzeHll, boolean isFromAnalyze, Engine engine) {

        this.statisticManager = statisticManager;
        this.tDataSource = tDataSource;
        this.logicalTableName = logicalTableName;
        this.columnMetaList = columnMetaList;
        this.statisticSampleRate = paramManager.getFloat(ConnectionParams.STATISTIC_SAMPLE_RATE);
        this.histogramMaxSampleSize = paramManager.getInt(ConnectionParams.HISTOGRAM_MAX_SAMPLE_SIZE);
        this.histogramBucketSize = paramManager.getInt(ConnectionParams.HISTOGRAM_BUCKET_SIZE);
        this.analyzeTableSpeedLimit = paramManager.getInt(ConnectionParams.ANALYZE_TABLE_SPEED_LIMITATION);
        this.enableInnodbBtreeSampling = paramManager.getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);
        this.topNSize = paramManager.getInt(ConnectionParams.TOPN_SIZE);
        this.topNMinNum = paramManager.getInt(ConnectionParams.TOPN_MIN_NUM);
//        this.useHll = paramManager.getBoolean(ConnectionParams.ENABLE_HLL);
        this.isFromAnalyze = isFromAnalyze;
        this.engine = engine;
        this.analyzeHll = analyzeHll;
        if (Engine.isFileStore(engine)) {
            this.enableInnodbBtreeSampling = false;
            this.analyzeHll = false;
        }
    }

    public void prepare() {
        /** SAMPLE_RATE */
        sampleRate = 1;
        tableCacheRowCount = statisticManager.getCacheLine(logicalTableName).getRowCount();
        if (tableCacheRowCount <= 0) {
            if (Engine.isFileStore(engine)) {
                try (Connection conn = tDataSource.getConnection()) {
                    try (Statement stmt = conn.createStatement()) {
                        try (ResultSet resultSet = stmt.executeQuery("/*+TDDL:cmd_extra(MERGE_CONCURRENT=true)*/ select count(*) from " + logicalTableName)) {
                            resultSet.next();
                            tableCacheRowCount = resultSet.getLong(1);
                        }
                    }
                } catch (Throwable t) {
                    tableCacheRowCount = 10000000;
                }
            }
        }
        if (tableCacheRowCount > 0) {
            sampleRate = (float) DEFAULT_SAMPLE_SIZE / tableCacheRowCount;
            if (sampleRate > 1f) {
                sampleRate = 1f;
            } else if (sampleRate < 0.000001f) {
                sampleRate = 0.000001f;
            }
        }

        if (statisticSampleRate > 0f && statisticSampleRate <= 1f) {
            sampleRate = statisticSampleRate;
        }

        if (sampleRate * tableCacheRowCount >= Integer.MAX_VALUE) {
            throw GeneralUtil.nestedException("Size of sampling is too large");
        }

        sampleSize = (int) (sampleRate * tableCacheRowCount);

        final int depth;
        final int width;
        if (sampleSize == 0) {
            depth = 1;
            width = 1;
            histogramBucketSize = 1;
        } else if (sampleSize <= 10) {
            depth = 2;
            width = 5;
            histogramBucketSize = 4;
        } else if (sampleSize <= 100) {
            depth = 3;
            width = 10;
            histogramBucketSize = Math.min(histogramBucketSize, 8);
        } else if (sampleSize <= 1000) {
            depth = 4;
            width = 20;
            histogramBucketSize = Math.min(histogramBucketSize, 16);
        } else if (sampleSize <= 10000) {
            depth = 5;
            width = 34;
            histogramBucketSize = Math.min(histogramBucketSize, 64);
        } else {
            depth = 6;
            width = 100;
        }

        for (ColumnMeta columnMeta : columnMetaList) {
            hyperLogLogList.add(new HyperLogLog(16));
            countMinSketchList.add(new CountMinSketch(depth, width, (int) (System.currentTimeMillis() & 0xff)));
            nullCountList.add(0L);
            geeSamples.add(new MysqlStatisticCollector.GEESample(sampleSize));
        }

        sampleRows = new Object[columnMetaList.size()][histogramMaxSampleSize];
        columnSampleSize = new int[columnMetaList.size()];
        rand = new Random(System.currentTimeMillis());
    }

    private void scanAnalyze() throws SQLException {
        IConnection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        // scan sampling
        try {
            connection = (IConnection) tDataSource.getConnection();
            connection.setTrxPolicy(ITransactionPolicy.ALLOW_READ_CROSS_DB);
            statement = connection.createStatement();
            String sql = constructScanSamplingSql(logicalTableName, columnMetaList, sampleRate);
            resultSet = statement.executeQuery(sql);
            RateLimiter rateLimiter = RateLimiter.create(analyzeTableSpeedLimit);
            while (resultSet.next()) {
                GeneralUtil.checkInterrupted();
                processRow(resultSet);
                rateLimiter.acquire((int) (1.0 / sampleRate));
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }
    }

    private void hllAnalyze() throws SQLException {
        // useHll come from exec context
        if (!analyzeHll) {
            return;
        }
        Map<String, Set<String>> colMap =
                OptimizerContext.getContext(statisticManager.getSchemaName()).getPlanManager().columnsInvolvedByPlan();
        Set<String> colSet = colMap.get(logicalTableName);
        if (isFromAnalyze) {
            statisticManager.getSds().removeLogicalTableList(Lists.newArrayList(logicalTableName));
        }
        /**
         * handle columns needed by plan
         */
        for (ColumnMeta columnMeta : columnMetaList) {
            String columnName = columnMeta.getOriginColumnName();
            if (colSet == null || !colSet.contains(columnName)) {
                continue;
            }
            if (isFromAnalyze) {
                // analyze table would rebuild full ndv sketch info
                statisticManager.rebuildShardParts(logicalTableName, columnName);
            } else {
                // schedule job only update ndv sketch info
                statisticManager.updateAllShardParts(logicalTableName, columnName);
            }
        }

        /**
         * handle columns inside index
         */
        String schemaName = statisticManager.getSchemaName();
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        Map<String, Map<String, List<String>>> indexColsMap = GlobalIndexMeta.getTableIndexMap(tableMeta, null);

        for (String tblName : indexColsMap.keySet()) {
            // index key -> columns
            Map<String, List<String>> indexColumnMap = indexColsMap.get(tblName);
            for (List<String> cols : indexColumnMap.values()) {
                if (cols != null && cols.size() == 1 && colMap.get(tblName) != null && colMap.get(tblName)
                        .contains(cols.iterator().next())) {
                    continue;
                }
                for (int i = 0; i < cols.size() - 1; i++) {
                    String colsName = buildColumnsName(cols, i+1);
                    if (isFromAnalyze) {
                        // analyze table would rebuild full ndv sketch info
                        statisticManager.rebuildShardParts(tblName, colsName);
                    } else {
                        // schedule job only update ndv sketch info
                        statisticManager.updateAllShardParts(tblName, colsName);
                    }
                }

                String columnsName = buildColumnsName(cols);
                if (isFromAnalyze) {
                    // analyze table would rebuild full ndv sketch info
                    statisticManager.rebuildShardParts(tblName, columnsName);
                } else {
                    // schedule job only update ndv sketch info
                    statisticManager.updateAllShardParts(tblName, columnsName);
                }
            }
        }
    }

    private void processRow(ResultSet resultSet) {
        rowCount++;
        for (int i = 0; i < columnMetaList.size(); i++) {
            Object columnValue;
            try {
                columnValue = resultSet.getObject(i + 1);

                if (columnValue instanceof Slice) {
                    columnValue = ((Slice) columnValue).toStringUtf8();
                } else if (columnValue instanceof Decimal) {
                    columnValue = ((Decimal) columnValue).toBigDecimal();
                }

                // pruning too long data
                if (columnValue instanceof String) {
                    String s = (String) columnValue;
                    if (s.length() > DATA_MAX_LEN) {
                        columnValue = s.substring(0, DATA_MAX_LEN);
                    }
                } else if (columnValue instanceof byte[]) {
                    byte[] byteArray = (byte[]) columnValue;
                    if (byteArray.length > DATA_MAX_LEN) {
                        columnValue = Arrays.copyOfRange(byteArray, 0, DATA_MAX_LEN);
                    }
                }
            } catch (Throwable e) {
                // deal with TResultSet getObject error
                continue;
            }

            hyperLogLogList.get(i).offer(columnValue);
            if (columnValue != null) {
                String columnValueString = columnValue.toString();
                if (USE_GEE_CARDINALITY) {
                    geeSamples.get(i).addElement(columnValueString);
                } else if (USE_BC_GEE_CARDINALITY) {
                    geeSamples.get(i).addElement(columnValueString);
                }
                countMinSketchList.get(i).add(columnValueString, 1);
                /** sample for histogram */
                if (columnSampleSize[i] < histogramMaxSampleSize) {
                    sampleRows[i][columnSampleSize[i]] = columnValue;
                    columnSampleSize[i]++;
                } else if (rand.nextDouble() < (double) (histogramMaxSampleSize) / (rowCount - nullCountList
                        .get(i))) {
                    sampleRows[i][rand.nextInt(histogramMaxSampleSize)] = columnValue;
                }
            } else {
                nullCountList.set(i, nullCountList.get(i) + 1);
            }

            // TODO handle topN For Multi Columns
        }
    }

    public void analyze() throws SQLException {
        scanAnalyze();
        long rowCount = statisticManager.getCacheLine(logicalTableName).getRowCount();
        // small table use cache_line
        if (rowCount > DEFAULT_SAMPLE_SIZE) {
            hllAnalyze();
        }
    }

    public StatisticManager.CacheLine build() {
        StatisticManager.CacheLine cacheLine = new StatisticManager.CacheLine();
        for (int i = 0; i < columnMetaList.size(); i++) {
            String columnName = columnMetaList.get(i).getName().toLowerCase();
            Object[] data = new Object[columnSampleSize[i]];
            System.arraycopy(sampleRows[i], 0, data, 0, columnSampleSize[i]);
            float histogramSampleRate;
            if (rowCount == nullCountList.get(i)) {
                histogramSampleRate = 1;
            } else {
                histogramSampleRate = (float) (data.length) / (rowCount - nullCountList.get(i));
            }
            DataType dataType = columnMetaList.get(i).getField().getDataType();
            histogramList.add(new Histogram(histogramBucketSize, dataType,
                    histogramSampleRate));
            TopN topN = new TopN(dataType);
            Arrays.stream(data).forEach(obj -> topN.offer(obj));
            topN.build(topNSize, topNMinNum);
            cacheLine.getTopNMap().put(columnName, topN);
            histogramList.get(i).buildFromData(Arrays.stream(data).filter(d -> topN.get(d) == 0).collect(
                    Collectors.toList()).toArray());
        }

        for (int i = 0; i < columnMetaList.size(); i++) {
            String columnName = columnMetaList.get(i).getName().toLowerCase();
            double cardinality;
            if (USE_HLL_CARDINALITY) {
                cardinality = (hyperLogLogList.get(i).cardinality() / sampleRate);
            } else if (USE_GEE_CARDINALITY) {
                final MysqlStatisticCollector.GEESample f = geeSamples.get(i);
                cardinality = Math.sqrt(1 / sampleRate) * f.getCountFresh() + f.getCountDuplicated();
            } else if (USE_BC_GEE_CARDINALITY) {
                final MysqlStatisticCollector.GEESample f = geeSamples.get(i);
                double d = hyperLogLogList.get(i).cardinality();
                double f1 = f.getCountFresh();
                double sumf2tofn = f.getCountDuplicated();
                double lowerBound;
                double n = rowCount;
                double N = rowCount / sampleRate;
                if (n <= 0) {
                    n = 1;
                }
                if (N <= 0) {
                    N = 1;
                }
                if (f1 >= n * Math.pow(1 - 1.0 / n, n - 1) && n != 1) {
                    lowerBound = 1.0 / (1 - Math.pow(f1 / n, 1 / (n - 1)));
                } else {
                    lowerBound = f1 / Math.pow(1 - 1.0 / n, n - 1);
                }
                double upperBound = d / (1 - Math.pow(1 - 1.0 / N, n));

                lowerBound = Math.max(d, Math.min(lowerBound, N));
                upperBound = Math.max(d, Math.min(upperBound, N));

                double lbc = Math.max(f1, lowerBound - sumf2tofn);
                double ubc = Math.min(f1 * N / n, upperBound - sumf2tofn);
                cardinality = Math.sqrt(lbc * ubc) + sumf2tofn;
            }
            cardinalityMap.put(columnName, (long) cardinality);
            countMinSketchMap.put(columnName, countMinSketchList.get(i));
            nullCountMap.put(columnName, nullCountList.get(i));
            histogramMap.put(columnName, histogramList.get(i));
        }

        cacheLine.setRowCount((long) (rowCount / sampleRate));
        cacheLine.setSampleRate(sampleRate);
        cacheLine.setLastModifyTime(unixTimeStamp());

        if (cacheLine.getCardinalityMap() == null) {
            cacheLine.setCardinalityMap(cardinalityMap);
        } else {
            cacheLine.getCardinalityMap().putAll(cardinalityMap);
        }

        if (cacheLine.getCountMinSketchMap() == null) {
            cacheLine.setCountMinSketchMap(countMinSketchMap);
        } else {
            cacheLine.getCountMinSketchMap().putAll(countMinSketchMap);
        }

        if (cacheLine.getNullCountMap() == null) {
            cacheLine.setNullCountMap(nullCountMap);
        } else {
            cacheLine.getNullCountMap().putAll(nullCountMap);
        }

        if (cacheLine.getHistogramMap() == null) {
            cacheLine.setHistogramMap(histogramMap);
        } else {
            cacheLine.getHistogramMap().putAll(histogramMap);
        }

        return cacheLine;
    }

    private String constructScanSamplingSql(String logicalTableName, List<ColumnMeta> columnMetaList,
                                            float sampleRate) {
        StringBuilder sql = new StringBuilder();

        boolean supportFastSample = checkSupportFastSample();
        String cmdExtraSamplePercentage = "";
        if (supportFastSample) {
            cmdExtraSamplePercentage = ",sample_percentage=" + sampleRate * 100;
        }
        String concurrentHint;
        if (Engine.isFileStore(engine)) {
            concurrentHint = "MERGE_CONCURRENT=true";
        } else {
            concurrentHint = "MERGE_UNION=false";
        }
        sql.append("/*+TDDL:cmd_extra(")
            .append(concurrentHint)
            .append(",ENABLE_DIRECT_PLAN=false")
            .append(cmdExtraSamplePercentage)
            .append(") */ select ");
        boolean first = true;
        for (ColumnMeta columnMeta : columnMetaList) {
            if (first) {
                first = false;
            } else {
                sql.append(",");
            }
            sql.append("`" + columnMeta.getName() + "`");
        }
        sql.append(" from ");
        sql.append("`" + logicalTableName + "`");
        if (!supportFastSample && sampleRate > 0f && sampleRate < 1f) {
            sql.append(" where rand() < ");
            sql.append(sampleRate);
        }
        return sql.toString();
    }

    private boolean checkSupportFastSample() {
        if (!enableInnodbBtreeSampling) {
            return false;
        }
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = tDataSource.getConnection();
            statement = connection.createStatement();
            // FIXME
            resultSet = statement.executeQuery("show variables like 'innodb_innodb_btree_sampling'");
            if (resultSet.next()) {
                String value = resultSet.getString(2);
                if ("ON".equalsIgnoreCase(value)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } catch (SQLException e) {
            return false;
        } catch (Throwable e) {
            throw e;
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }
    }

}
