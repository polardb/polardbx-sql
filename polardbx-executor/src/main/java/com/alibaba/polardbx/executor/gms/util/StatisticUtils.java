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

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.membership.BloomFilter;
import com.clearspring.analytics.stream.membership.BloomFilter;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.slice.Slice;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.CollectionUtils;
import org.glassfish.jersey.internal.guava.Sets;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.STATISTIC_VISIT_DN_TIMEOUT;
import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.executor.scheduler.executor.statistic.StatisticSampleCollectionScheduledJob.DATA_MAX_LEN;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.STATISTIC_ROWCOUNT_COLLECTION;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildColumnsName;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas;

/**
 * @author fangwu
 */
public class StatisticUtils {

    private static final Logger logger = LoggerFactory.getLogger("statistics");

    private static String PRESENT_SQL = "select sum(extent_size) as size from files where "
        + "logical_schema_name = '%s' and logical_table_name = '%s' and remove_ts is null and " +
        "life_cycle = " + OSSMetaLifeCycle.READY.ordinal();

    private static String DATA_FREE_SQL = "select sum(extent_size) as size from files where "
        + "logical_schema_name = '%s' and logical_table_name = '%s' and remove_ts is not null and " +
        "life_cycle = " + OSSMetaLifeCycle.READY.ordinal();

    /**
     * select table rows sql, need to concat with where filter
     */
    static final String SELECT_TABLE_ROWS_SQL =
        "SELECT table_schema, table_name, table_rows FROM information_schema.tables";

    public static boolean forceAnalyzeColumns(String schema, String logicalTableName) {
        try {
            collectRowCount(schema, logicalTableName);
            sampleTable(schema, logicalTableName);
//            sketchTable(schema, logicalTableName, true);

            /** persist */
            persistStatistic(schema, logicalTableName, true);
            /** sync other nodes */
            SyncManagerHelper.sync(
                new UpdateStatisticSyncAction(
                    schema,
                    logicalTableName,
                    StatisticManager.getInstance().getCacheLine(schema, logicalTableName)),
                schema);
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
        return true;
    }

    public static void collectRowCount(String schema, String logicalTableName) throws SQLException {
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                PROCESS_START,
                new String[] {
                    STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE",
                    schema + "," + logicalTableName
                },
                NORMAL);
        long sum = 0;
        if (isFileStore(schema, logicalTableName)) {
            try {
                Map<String, Long> fileStorageStatisticMap = getFileStoreStatistic(schema, logicalTableName);
                sum = fileStorageStatisticMap.get("TABLE_ROWS");
            } catch (Throwable e) {
                String remark = "file storage statistic collection rowcount error: " + e.getMessage();
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        UNEXPECTED,
                        new String[] {
                            STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE",
                            schema + "," + logicalTableName + ":" + remark
                        },
                        WARNING);
                throw e;
            }
        } else {
            Map<String, Set<String>> topologyMap = getTopology(schema, logicalTableName);
            if (topologyMap == null) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.STATISTICS,
                        UNEXPECTED,
                        new String[] {
                            STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE",
                            schema + "," + logicalTableName + " topology is null"
                        },
                        WARNING);
            }
            Collection<String> tbls = Sets.newHashSet();
            topologyMap.values().stream().forEach(names -> tbls.addAll(names));

            Set<String> dnIds =
                StorageHaManager.getInstance().getMasterStorageList().stream().filter(s -> !s.isMetaDb())
                    .map(StorageInstHaContext::getStorageInstId).collect(
                        Collectors.toSet());

            Map<String, Map<String, Long>> rowCountMap = Maps.newHashMap();
            for (String dnId : dnIds) {
                try {
                    Map<String, Map<String, Long>> rowRs = collectRowCountAll(dnId, tbls);
                    if (rowRs != null) {
                        rowCountMap.putAll(rowRs);
                    }
                } catch (Throwable e) {
                    String remark = "statistic collection rowcount error: " + e.getMessage();
                    ModuleLogInfo.getInstance()
                        .logRecord(
                            Module.STATISTICS,
                            UNEXPECTED,
                            new String[] {
                                STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE",
                                schema + "," + logicalTableName + ":" + remark
                            },
                            WARNING);
                    throw e;
                }
            }

            sum = sumRowCount(topologyMap, rowCountMap);
        }
        StatisticManager.CacheLine cacheLine = StatisticManager.getInstance().getCacheLine(schema, logicalTableName);
        cacheLine.setRowCount(sum);
        ModuleLogInfo.getInstance()
            .logRecord(
                Module.STATISTICS,
                PROCESS_END,
                new String[] {
                    STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE",
                    schema + "," + logicalTableName + ":" + sum
                },
                NORMAL);
    }

    public static boolean sampleColumns(String schema, String logicalTableName) {
        try {
            collectRowCount(schema, logicalTableName);
            sampleTable(schema, logicalTableName);

            /** persist */
            persistStatistic(schema, logicalTableName, true);
            /** sync other nodes */
            SyncManagerHelper.sync(
                new UpdateStatisticSyncAction(
                    schema,
                    logicalTableName,
                    StatisticManager.getInstance().getCacheLine(schema, logicalTableName)),
                schema);

        } catch (Exception e) {
            logger.error(e);
            return false;
        }
        return true;
    }

    public static void persistStatistic(String schema, String logicalTableName, boolean withColumnStatistic) {
        long start = System.currentTimeMillis();
        ArrayList<SystemTableTableStatistic.Row> rowList = new ArrayList<>();
        StatisticManager.CacheLine cacheLine = StatisticManager.getInstance().getCacheLine(schema, logicalTableName);
        rowList.add(new SystemTableTableStatistic.Row(schema, logicalTableName.toLowerCase(), cacheLine.getRowCount(),
            cacheLine.getLastModifyTime()));
        StatisticManager.getInstance().getSds().batchReplace(rowList);

        ArrayList<SystemTableColumnStatistic.Row> columnRowList = new ArrayList<>();
        if (withColumnStatistic && cacheLine.getCardinalityMap() != null
            && cacheLine.getHistogramMap() != null && cacheLine.getNullCountMap() != null) {
            for (String columnName : cacheLine.getCardinalityMap().keySet()) {
                columnRowList.add(new SystemTableColumnStatistic.Row(schema,
                    logicalTableName.toLowerCase(),
                    columnName,
                    cacheLine.getCardinalityMap().get(columnName),
                    cacheLine.getHistogramMap().get(columnName),
                    cacheLine.getTopN(columnName),
                    cacheLine.getNullCountMap().get(columnName),
                    cacheLine.getSampleRate(),
                    cacheLine.getLastModifyTime()));
            }
        }
        StatisticManager.getInstance().getSds().batchReplace(columnRowList);

        updateMetaDbInformationSchemaTables(schema, logicalTableName);

        long end = System.currentTimeMillis();
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, PROCESS_END,
            new String[] {
                "persist tables statistic:" + schema + "," + logicalTableName + "," + withColumnStatistic,
                "consuming " + (end - start) / 1000.0 + " seconds"
            }, LogLevel.NORMAL
        );
    }

    private static void updateMetaDbInformationSchemaTables(String schemaName, String logicalTableName) {
        ExecutionContext executionContext = new ExecutionContext(schemaName);
        executionContext.setTraceId("statistic");
        executionContext.setParams(new Parameters());
        executionContext.setRuntimeStatistics(RuntimeStatHelper.buildRuntimeStat(executionContext));
        SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, executionContext);
        RelOptCluster relOptCluster = sqlConverter.createRelOptCluster();
        InformationSchemaTables informationSchemaTables =
            new InformationSchemaTables(relOptCluster, relOptCluster.getPlanner().emptyTraitSet());
        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(informationSchemaTables, informationSchemaTables.getTableSchemaIndex()),
            rexBuilder.makeLiteral(schemaName));
        if (logicalTableName != null) {
            RexNode tableNameFilterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(informationSchemaTables, informationSchemaTables.getTableNameIndex()),
                rexBuilder.makeLiteral(logicalTableName));
            filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, filterCondition,
                tableNameFilterCondition);
        }

        informationSchemaTables.pushFilter(filterCondition);
        Cursor cursor = ExecutorHelper.execute(informationSchemaTables, executionContext, false, false);

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(metaDbConn);
            Row row;
            while ((row = cursor.next()) != null) {

                String tableSchema = row.getString(1);
                String tableName = row.getString(2);
                Long tableRows = row.getLong(7);
                Long avgRowLength = row.getLong(8);
                Long dataLength = row.getLong(9);
                Long maxDataLength = row.getLong(10);
                Long indexLength = row.getLong(11);
                Long dataFree = row.getLong(12);

                if (schemaName.equalsIgnoreCase(tableSchema)) {
                    tablesAccessor.updateStatistic(tableSchema, tableName, tableRows, avgRowLength, dataLength,
                        maxDataLength, indexLength, dataFree);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                "Schema `" + schemaName + "` build meta error.");
        } finally {
            try {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            } finally {
                executionContext.clearAllMemoryPool();
            }
        }
    }

    /**
     * hyper loglog process
     */
    public static void sketchTable(String schema, String logicalTableName, boolean needRebuild) {
        // don't sketch archive table
        if (isFileStore(schema, logicalTableName)) {
            return;
        }

        List<ColumnMeta> columnMetaList = getColumnMetas(false, schema, logicalTableName);

        if (columnMetaList == null || columnMetaList.isEmpty()) {
            ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, UNEXPECTED,
                new String[] {
                    "statistic sketch",
                    "column meta is empty :" + schema + "," + logicalTableName
                }, LogLevel.NORMAL);
            return;
        }

        Map<String, Set<String>> colMap = PlanManager.getInstance().columnsInvolvedByPlan().get(schema);

        if (colMap == null) {
            colMap = Maps.newHashMap();
        }

        Set<String> colSet = colMap.get(logicalTableName);
        Set<String> colDoneSet = Sets.newHashSet();

        /**
         * handle columns needed by plan
         */
        for (ColumnMeta columnMeta : columnMetaList) {
            String columnName = columnMeta.getOriginColumnName();
            if (colSet == null || !colSet.contains(columnName)) {
                continue;
            }

            if (needRebuild) {
                // analyze table would rebuild full ndv sketch info
                StatisticManager.getInstance().rebuildShardParts(schema, logicalTableName, columnName);
            } else {
                // schedule job only update ndv sketch info
                StatisticManager.getInstance().updateAllShardParts(schema, logicalTableName, columnName);
            }
            colDoneSet.add(columnName);
        }

        /**
         * handle columns inside index
         */
        TableMeta tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(logicalTableName);
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
                    String colsName = buildColumnsName(cols, i + 1);
                    if (colDoneSet.contains(colsName)) {
                        continue;
                    }
                    if (needRebuild) {
                        // analyze table would rebuild full ndv sketch info
                        StatisticManager.getInstance().rebuildShardParts(schema, logicalTableName, colsName);
                    } else {
                        // schedule job only update ndv sketch info
                        StatisticManager.getInstance().updateAllShardParts(schema, tblName, colsName);
                    }
                    colDoneSet.add(colsName);
                }

                String columnsName = buildColumnsName(cols);
                if (!colDoneSet.contains(columnsName)) {
                    if (needRebuild) {
                        // analyze table would rebuild full ndv sketch info
                        StatisticManager.getInstance().rebuildShardParts(schema, logicalTableName, columnsName);
                    } else {
                        // schedule job only update ndv sketch info
                        StatisticManager.getInstance().updateAllShardParts(schema, tblName, columnsName);
                    }
                    colDoneSet.add(columnsName);
                }

            }
        }

        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, PROCESS_END,
            new String[] {
                "statistic sketch table ",
                schema + "," + logicalTableName + ",is force:" + needRebuild + ",cols:" + String.join(";", colDoneSet)
            }, LogLevel.NORMAL);
        return;
    }

    public static void sampleTable(String schemaName, String logicalTableName) {
        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, PROCESS_START,
            new String[] {
                "statistic sample",
                schemaName + "," + logicalTableName
            }, LogLevel.NORMAL);
        List<ColumnMeta> analyzeColumnList = getColumnMetas(false, schemaName, logicalTableName);
        StatisticManager.CacheLine cacheLine =
            StatisticManager.getInstance().getCacheLine(schemaName, logicalTableName);
        if (analyzeColumnList == null || analyzeColumnList.isEmpty()) {
            ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, UNEXPECTED,
                new String[] {
                    "statistic sample",
                    "column meta is empty :" + schemaName + "," + logicalTableName
                }, LogLevel.NORMAL);
            return;
        }

        /**
         * prepare
         */
        int topNSize = InstConfUtil.getInt(ConnectionParams.TOPN_SIZE);
        int topNMinNum = InstConfUtil.getInt(ConnectionParams.TOPN_MIN_NUM);
        float sampleRate = 1;
        long rowCount = cacheLine.getRowCount();
        if (rowCount > 0) {
            sampleRate = (float) DEFAULT_SAMPLE_SIZE / rowCount;
            if (sampleRate > 1f) {
                sampleRate = 1f;
            } else if (sampleRate < 0.000001f) {
                sampleRate = 0.000001f;
            }
        }

        if (sampleRate * rowCount >= Integer.MAX_VALUE) {
            ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, UNEXPECTED,
                new String[] {
                    "statistic sample",
                    "Size of sampling is too large :" + schemaName + "," + logicalTableName + "," + rowCount
                }, LogLevel.NORMAL);
            return;
        }

        long sampleSize = (int) (sampleRate * rowCount);
        int histogramBucketSize = InstConfUtil.getInt(ConnectionParams.HISTOGRAM_BUCKET_SIZE);

        if (sampleSize == 0) {
            histogramBucketSize = 1;
        } else if (sampleSize <= 10) {
            histogramBucketSize = 4;
        } else if (sampleSize <= 100) {
            histogramBucketSize = Math.min(histogramBucketSize, 8);
        } else if (sampleSize <= 1000) {
            histogramBucketSize = Math.min(histogramBucketSize, 16);
        } else if (sampleSize <= 10000) {
            histogramBucketSize = Math.min(histogramBucketSize, 64);
        }

        /**
         * sample process
         */
        int maxSampleSize = InstConfUtil.getInt(ConnectionParams.HISTOGRAM_MAX_SAMPLE_SIZE);
        List<Row> rows = new ArrayList<>();
        double sampleRateUp =
            scanAnalyze(schemaName, logicalTableName, analyzeColumnList, sampleRate, maxSampleSize, rows);
        for (int i = 0; i < analyzeColumnList.size(); i++) {
            String colName = analyzeColumnList.get(i).getField().getOriginColumnName();

            HyperLogLog hyperLogLog = new HyperLogLog(16);
            GEESample geeSample = new GEESample(rows.size());
            long nullCount = 0L;

            for (Row r : rows) {
                Object columnValue = r.getObject(i);
                hyperLogLog.offer(columnValue);
                if (columnValue == null) {
                    nullCount++;
                } else {
                    geeSample.addElement(columnValue.toString());
                }
            }

            /*
             * Use BC_GEE to estimate cardinality
             */
            double d = hyperLogLog.cardinality();
            double f1 = geeSample.getCountFresh();
            double sumf2tofn = geeSample.getCountDuplicated();
            double lowerBound;
            double n = rows.size();
            double N = rowCount;
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
            double cardinality = Math.sqrt(lbc * ubc) + sumf2tofn;

            cacheLine.setCardinality(colName, (long) cardinality);
            cacheLine.setNullCount(colName, nullCount);
        }

        for (int i = 0; i < analyzeColumnList.size(); i++) {
            int finalI = i;
            List<Object> objs =
                rows.stream().map(r -> r.getObject(finalI)).filter(o -> o != null).collect(Collectors.toList());
            if (!CollectionUtils.isEmpty(objs)) {
                if (objs.get(0) instanceof Slice) {
                    objs = objs.stream().map(o -> ((Slice) o).toStringUtf8()).collect(Collectors.toList());
                } else {
                    if (objs.get(0) instanceof Decimal) {
                        objs = objs.stream().map(o -> ((Decimal) o).toBigDecimal()).collect(Collectors.toList());
                    }
                }
            }
            DataType dataType = analyzeColumnList.get(i).getField().getDataType();
            String colName = analyzeColumnList.get(i).getField().getOriginColumnName();
            TopN topN = new TopN(dataType, sampleRateUp);
            objs.forEach(obj -> topN.offer(obj));

            boolean isReady = topN.build(topNSize, topNMinNum);
            if (isReady) {
                cacheLine.setTopN(colName, topN);
            } else {
                cacheLine.setTopN(colName, null);
            }
            Histogram h = new Histogram(histogramBucketSize, dataType, (float) sampleRateUp);
            h.buildFromData(objs.stream().filter(d -> isReady ? topN.get(d) == 0 : true).toArray());
            cacheLine.setHistogram(colName, h);
        }
        cacheLine.setRowCount((long) (rows.size() / sampleRate / sampleRateUp));
        cacheLine.setSampleRate(sampleRate);
        cacheLine.setLastModifyTime(unixTimeStamp());

        ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, PROCESS_END,
            new String[] {
                "statistic sample",
                schemaName + "," + logicalTableName
            }, LogLevel.NORMAL);
    }

    private static Row purgeRowForHistogram(Row r, int size) {
        Row tmpRow = new ArrayRow(size, r.getParentCursorMeta());
        for (int i = 0; i < size; i++) {
            try {
                Object columnValue = r.getObject(i);
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
                tmpRow.setObject(i, columnValue);
            } catch (Throwable e) {
                // deal with TResultSet getObject error
                continue;
            }
        }
        return tmpRow;
    }

    private static double scanAnalyze(String schema, String logicalTableName, List<ColumnMeta> columnMetaList,
                                      float sampleRate, int maxSampleSize, List<Row> rows) {
        // scan sampling
        ResultCursor rc = null;
        ITransaction trx = null;
        ExecutionContext ec = null;
        try {
            String sql = StatisticUtils.isFileStore(schema, logicalTableName) ?
                constructRandomScanSamplingSql(logicalTableName, columnMetaList, sampleRate)
                : constructScanSamplingSql(logicalTableName, columnMetaList, sampleRate);
            ec = new ExecutionContext(schema);
            ec.setTraceId("statistic");
            ec.setParams(new Parameters());
            ec.setRuntimeStatistics(RuntimeStatHelper.buildRuntimeStat(ec));
            ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();

            // Create new transaction
            trx = tm.createTransaction(ITransactionPolicy.TransactionClass.AUTO_COMMIT, ec);
            ec.setTransaction(trx);
            ec.setStats(new MatrixStatistics());
            ec.setPhysicalRecorder(new SQLRecorder(100));
            ExecutionPlan plan = Planner.getInstance().plan(ByteString.from(sql), ec);
            rc = PlanExecutor.execute(plan, ec);

            Row r = rc.doNext();
            Random rand = new Random();
            int rowcount = 0;
            do {
                if (r != null) {
                    if (rowcount > maxSampleSize) {
                        if (rand.nextInt(rowcount) < maxSampleSize) {
                            rows.set(rand.nextInt(maxSampleSize), purgeRowForHistogram(r, columnMetaList.size()));
                        } else {
                            // ignore
                        }
                    } else {
                        rows.add(purgeRowForHistogram(r, columnMetaList.size()));
                    }
                    rowcount++;
                }
                r = rc.doNext();
            } while (r != null);
            return (double) rows.size() / rowcount;
        } catch (Throwable e) {
            throw e;
        } finally {
            if (rc != null) {
                rc.close(Collections.emptyList());
            }
            if (trx != null) {
                trx.close();
            }
            if (ec != null) {
                ec.clearContextAfterTrans();
            }
        }
    }

    private static String constructScanSamplingSql(String logicalTableName, List<ColumnMeta> columnMetaList,
                                                   float sampleRate) {
        StringBuilder sql = new StringBuilder();

        String cmdExtraSamplePercentage = "";
        cmdExtraSamplePercentage = ",sample_percentage=" + sampleRate * 100;
        sql.append("/*+TDDL:cmd_extra(merge_union=false,ENABLE_DIRECT_PLAN=false" + cmdExtraSamplePercentage + ") */ "
            + "select ");
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
        return sql.toString();
    }

    private static String constructRandomScanSamplingSql(String logicalTableName, List<ColumnMeta> columnMetaList,
                                                         float sampleRate) {
        StringBuilder sql = new StringBuilder();

        sql.append("/*+TDDL:cmd_extra(MERGE_UNION=false,ENABLE_DIRECT_PLAN=false) */ ")
            .append("select ");
        boolean first = true;
        for (ColumnMeta columnMeta : columnMetaList) {
            if (first) {
                first = false;
            } else {
                sql.append(",");
            }
            sql.append("`").append(columnMeta.getName()).append("`");
        }
        sql.append(" from ").append("`").append(logicalTableName).append("`");

        if (sampleRate > 0f && sampleRate < 1f) {
            sql.append(" where rand() < ");
            sql.append(sampleRate);
        }
        return sql.toString();
    }

    /**
     * @return phy schema-> phy table name -> rows num
     */
    public static Map<String, Map<String, Long>> collectRowCountAll(String dnId, Collection<String> tblNames)
        throws SQLException {
        Map<String, Map<String, Long>> rowCountsMap = Maps.newHashMap();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DbTopologyManager.getConnectionForStorage(dnId, InstConfUtil.getInt(STATISTIC_VISIT_DN_TIMEOUT));
            avoidInformationSchemaCache(conn);
            stmt = conn.createStatement();
            String sql = buildCollectRowCountSql(tblNames);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableSchema = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                Long tableRows = rs.getLong("table_rows");

                Map<String, Long> tableRowCountMap;
                if (rowCountsMap.containsKey(tableSchema)) {
                    tableRowCountMap = rowCountsMap.get(tableSchema);
                } else {
                    tableRowCountMap = Maps.newHashMap();
                    rowCountsMap.put(tableSchema, tableRowCountMap);
                }
                tableRowCountMap.put(tableName, tableRows);
            }
            return rowCountsMap;
        } catch (Throwable e) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    UNEXPECTED,
                    new String[] {"collectRowCount table statistic from dn:" + dnId, e.getMessage()},
                    CRITICAL,
                    e
                );
            throw e;
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    /**
     * Implementation of Guaranteed-Error Estimator
     */
    static class GEESample {

        private static final double BLOOM_FILTER_MAX_FP = 0.05;

        private final BloomFilter bloomFilterOnce;
        private final BloomFilter bloomFilterTwice;

        private long countFresh = 0; // f0
        private long countDuplicated = 0; // sum(f1..n)

        public GEESample(int size) {
            this.bloomFilterOnce = new BloomFilter(size, BLOOM_FILTER_MAX_FP);
            this.bloomFilterTwice = new BloomFilter(size, BLOOM_FILTER_MAX_FP);
        }

        public long getCountFresh() {
            return countFresh;
        }

        public long getCountDuplicated() {
            return countDuplicated;
        }

        public void addElement(String e) {
            if (!bloomFilterOnce.isPresent(e)) {
                bloomFilterOnce.add(e);
                countFresh++;
            } else if (!bloomFilterTwice.isPresent(e)) {
                bloomFilterTwice.add(e);
                countDuplicated++;
                countFresh--;
                countFresh = Math.max(countFresh, 0); // just in case
            }
        }
    }

    public static boolean isFileStore(String schema, String logicalTableName) {
        try {
            TableMeta tm = OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(logicalTableName);
            return tm != null && Engine.isFileStore(tm.getEngine());
        } catch (Exception e) {
            return false;
        }
    }

    public static Map<String, Long> getFileStoreStatistic(String schema, String logicalTableName) {

        TableMeta tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(logicalTableName);
        long tableRows = 0L;
        long dataLength = 0L;
        long indexLength = 0;
        long dataFree = 0;
        for (List<FileMeta> fileMetas : tableMeta.getFlatFileMetas().values()) {
            for (FileMeta fileMeta : fileMetas) {
                if (fileMeta instanceof OSSOrcFileMeta) {
                    OSSOrcFileMeta ossOrcFileMeta = (OSSOrcFileMeta) fileMeta;
                    if (ossOrcFileMeta.getRemoveTs() == null) {
                        tableRows += ossOrcFileMeta.getTableRows();
                        dataLength += ossOrcFileMeta.getFileSize();
                    }
                }
            }
        }

        try (Connection connection = MetaDbUtil.getConnection();
            Statement statement = connection.createStatement()) {
            ResultSet results = statement.executeQuery(
                String.format(PRESENT_SQL, schema, logicalTableName));
            if (results.next()) {
                // total length - date length
                indexLength = results.getLong("size") - dataLength;
            }
            results.close();
            results = statement.executeQuery(
                String.format(DATA_FREE_SQL, schema, logicalTableName));
            if (results.next()) {
                dataFree = results.getLong("size");
            }
            results.close();
        } catch (SQLException e) {
            logger.error(e);
            throw GeneralUtil.nestedException(e);
        }

        Map<String, Long> statisticMap = new HashMap<>();
        statisticMap.put("TABLE_ROWS", tableRows);
        statisticMap.put("DATA_LENGTH", dataLength);
        statisticMap.put("INDEX_LENGTH", indexLength);
        statisticMap.put("DATA_FREE", dataFree);
        return statisticMap;
    }

    /**
     * @return phy schema-> phy table name -> rows num
     */
    public static Map<String, Map<String, Long>> collectRowCount(String dnId, Collection<String> tblNames)
        throws SQLException {
        Map<String, Map<String, Long>> rowCountsMap = Maps.newHashMap();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DbTopologyManager.getConnectionForStorage(dnId, InstConfUtil.getInt(STATISTIC_VISIT_DN_TIMEOUT));
            avoidInformationSchemaCache(conn);
            stmt = conn.createStatement();
            String sql = buildCollectRowCountSql(tblNames);

            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableSchema = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                Long tableRows = rs.getLong("table_rows");

                Map<String, Long> tableRowCountMap;
                if (rowCountsMap.containsKey(tableSchema)) {
                    tableRowCountMap = rowCountsMap.get(tableSchema);
                } else {
                    tableRowCountMap = Maps.newHashMap();
                    rowCountsMap.put(tableSchema, tableRowCountMap);
                }
                tableRowCountMap.put(tableName, tableRows);
            }
            return rowCountsMap;
        } catch (Throwable e) {
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.STATISTICS,
                    UNEXPECTED,
                    new String[] {"collectRowCount table statistic from dn:" + dnId, e.getMessage()},
                    CRITICAL,
                    e
                );
            throw e;
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    /**
     * build dn sql to collect rowcount, tbl names is null or empty meaning collect all table info
     *
     * @param tblNames target tables
     * @return collect rowcount sql
     */
    @NotNull
    public static String buildCollectRowCountSql(Collection<String> tblNames) {
        String sql;
        if (tblNames == null || tblNames.isEmpty()) {
            sql = SELECT_TABLE_ROWS_SQL;
        } else {
            sql = SELECT_TABLE_ROWS_SQL + " WHERE TABLE_NAME IN ('" + String.join("','", tblNames) + "')";
        }
        return sql;
    }

    private static void avoidInformationSchemaCache(Connection conn) throws SQLException {
        // avoid mysql 8.0 cache information_schema
        Statement setVarStmt = conn.createStatement();
        try {
            setVarStmt.execute("set information_schema_stats_expiry = 0");
        } catch (Throwable t) {
            // pass
        } finally {
            JdbcUtils.close(setVarStmt);
        }
    }

    /**
     * logicalTableName
     * whereFilter for query informationSchema
     * informationSchemaCache dbName -> {physicalTableName -> RowCount}
     */
    public static Map<String, Set<String>> getTopology(String schema, String tableName) {
        OptimizerContext op = OptimizerContext.getContext(schema);
        if (op == null) {
            return null;
        }
        PartitionInfoManager partitionInfoManager = op.getPartitionInfoManager();

        /*
          build topology for one logical table
         */
        Map<String, Set<String>> topology;
        if (partitionInfoManager.isNewPartDbTable(tableName)) {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
            if (partitionInfo.getSubPartitionBy() != null) {
                return null;
//                throw new AssertionError("do not support subpartition for statistic collector");
            } else {
                List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
                topology = new HashMap<>();
                for (PartitionSpec partitionSpec : partitionSpecs) {
                    String groupKey = partitionSpec.getLocation().getGroupKey();
                    String physicalTableName = partitionSpec.getLocation().getPhyTableName();
                    Set<String> physicalTableNames = topology.computeIfAbsent(groupKey, k -> new HashSet<>());
                    physicalTableNames.add(physicalTableName);
                }
            }
        } else {
            TddlRuleManager tddlRuleManager = op.getRuleManager();
            TableRule tableRule = tddlRuleManager.getTableRule(tableName);
            if (tableRule == null) {
                String dbIndex = tddlRuleManager.getDefaultDbIndex(tableName);
                topology = new HashMap<>();
                topology.put(dbIndex, com.google.common.collect.Sets.newHashSet(tableName));
            } else {
                topology = tableRule.getStaticTopology();
                if (topology == null || topology.size() == 0) {
                    topology = tableRule.getActualTopology();
                }
            }
        }

        /*
          transform db index to phy schema
         */
        List<GroupDetailInfoExRecord> groupDetailInfoExRecords = TableGroupLocation.getOrderedGroupList(schema);
        if (groupDetailInfoExRecords.size() == 0) {
            return null;
        }
        Map<String, String> dbIndexToPhySchema = Maps.newHashMap();
        for (GroupDetailInfoExRecord group : groupDetailInfoExRecords) {
            dbIndexToPhySchema.put(group.getGroupName().toLowerCase(Locale.ROOT),
                group.getPhyDbName().toLowerCase(Locale.ROOT));
        }

        Map<String, Set<String>> rs = Maps.newHashMap();
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            String phySchema = dbIndexToPhySchema.get(entry.getKey().toLowerCase(Locale.ROOT));
            rs.put(phySchema, entry.getValue());
            /*
              broadcast table only keep one phy table
             */
            if (partitionInfoManager.isBroadcastTable(tableName)) {
                break;
            }
        }

        return rs;
    }

    public static long sumRowCount(Map<String, Set<String>> topologyMap, Map<String, Map<String, Long>> rowCountMap) {
        AtomicLong sum = new AtomicLong(0L);
        for (Map.Entry<String, Set<String>> entry : topologyMap.entrySet()) {
            String phySchema = entry.getKey();
            Set<String> phyTables = entry.getValue();
            Map<String, Long> tableRowCountMap = rowCountMap.get(phySchema.toLowerCase(Locale.ROOT));
            if (tableRowCountMap == null) {
                continue;
            }
            phyTables.forEach(t -> sum.addAndGet(
                tableRowCountMap.get(t.toLowerCase(Locale.ROOT)) == null ? 0 : tableRowCountMap.get(t.toLowerCase(
                    Locale.ROOT))));
        }
        return sum.get();
    }

}
