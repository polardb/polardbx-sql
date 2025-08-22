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
import com.alibaba.druid.util.StringUtils;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbVariableConfigManager;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.TopN;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.clearspring.analytics.stream.StreamSummary;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.membership.BloomFilter;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.CollectionUtils;
import org.glassfish.jersey.internal.guava.Sets;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.STATISTIC_VISIT_DN_TIMEOUT;
import static com.alibaba.polardbx.common.utils.GeneralUtil.sampleString;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.NORMAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.INTERRUPTED;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DATA_MAX_LEN;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildColumnsName;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.packDateTypeToLong;

/**
 *
 */
public class StatisticUtils {

    // Statistics logger
    public final static Logger logger = LoggerUtil.statisticsLogger;

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

    /**
     * collect row count from all dn
     * 1. InformationSchemaTables的执行逻辑是遍历所有dn获取information_schema.tables的rowcount，进行聚合(InformationSchemaTablesHandler)
     * 2. 根据聚合的结果更新metadb.tables表
     * 3. 用户从cn上查询information_schema.tables表，会直接去metadb.tables表查询，而不是走InformationSchemaTablesHandler逻辑(具体查看InformationSchemaViewManager#defineView('TABLES'))
     */
    public static void updateMetaDbInformationSchemaTables(String schemaName, String logicalTableName) {
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
            rexBuilder.makeInputRef(informationSchemaTables, InformationSchemaTables.getTableSchemaIndex()),
            rexBuilder.makeLiteral(schemaName));
        if (logicalTableName != null) {
            RexNode tableNameFilterCondition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(informationSchemaTables, InformationSchemaTables.getTableNameIndex()),
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

    public static void updateMetaDbInformationSchemaTables(String schemaName, Collection<String> logicalTables) {
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
            rexBuilder.makeInputRef(informationSchemaTables, InformationSchemaTables.getTableSchemaIndex()),
            rexBuilder.makeLiteral(schemaName));
        List<RexNode> tableNameLiterals = new ArrayList<>(logicalTables.size());
        logicalTables.forEach(table -> tableNameLiterals.add(rexBuilder.makeLiteral(table)));
        RexNode inCondition = rexBuilder.makeCall(SqlStdOperatorTable.IN,
            rexBuilder.makeInputRef(informationSchemaTables, InformationSchemaTables.getTableNameIndex()),
            rexBuilder.makeCall(SqlStdOperatorTable.ROW, tableNameLiterals));
        filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, filterCondition, inCondition);
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
            throw new RuntimeException("Schema `" + schemaName + "` build meta error.");
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

    protected static void getIndexInfoFromGsi(TableMeta tableMeta, Set<String> colDoneSet,
                                              Map<String, Set<String>> indexColsMap) {
        if (tableMeta == null || tableMeta.getGsiPublished() == null) {
            return;
        }
        for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : tableMeta.getGsiPublished().entrySet()) {
            List<GsiMetaManager.GsiIndexColumnMetaBean> cols = entry.getValue().getIndexColumns();
            String indexName = entry.getKey();
            List<String> colNames = cols.stream().map(bean -> bean.columnName).collect(Collectors.toList());
            for (int i = 0; i < cols.size(); i++) {
                if ((!entry.getValue().nonUnique) && i == cols.size() - 1) {
                    continue;
                }
                String colOrder = buildColumnsName(colNames, i + 1);
                if (colDoneSet.contains(colOrder)) {
                    continue;
                }
                if (indexColsMap.containsKey(indexName)) {
                    indexColsMap.get(indexName).add(colOrder);
                } else {
                    Set<String> newSet = Sets.newHashSet();
                    newSet.add(colOrder);
                    indexColsMap.put(indexName, newSet);
                }
                colDoneSet.add(colOrder);
            }
        }
    }

    protected static void getIndexInfoFromLocalIndex(TableMeta tableMeta, Set<String> colDoneSet,
                                                     Map<String, Set<String>> indexColsMap) {
        if (tableMeta == null) {
            return;
        }
        for (IndexMeta localIndex : tableMeta.getAllIndexes()) {
            List<ColumnMeta> colList = localIndex.getKeyColumns();
            List<String> colNames = colList.stream().map(ColumnMeta::getName).collect(Collectors.toList());
            String indexName = localIndex.getPhysicalIndexName();
            for (int i = 0; i < colList.size(); i++) {
                if (localIndex.isUniqueIndex() && i == colList.size() - 1) {
                    continue;
                }

                String colOrder = buildColumnsName(colNames, i + 1);
                if (colDoneSet.contains(colOrder)) {
                    continue;
                }
                if (indexColsMap.containsKey(indexName)) {
                    indexColsMap.get(indexName).add(colOrder);
                } else {
                    Set<String> newSet = Sets.newHashSet();
                    newSet.add(colOrder);
                    indexColsMap.put(indexName, newSet);
                }
                colDoneSet.add(colOrder);
            }
        }
    }

    public static List<List<Integer>> buildColumnBitSet(
        String schemaName,
        String logicalTableName,
        List<ColumnMeta> analyzeColumnList) {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);

        // sample is derived from B+tree leaf of primary key.
        // if primary key is compound, the prefix column is biased,
        // don't check skewness of the column in this case
        ColumnMeta biasedColumn = null;
        IndexMeta primaryKey = tableMeta.getPrimaryIndex();
        if (primaryKey != null && primaryKey.getKeyColumns().size() > 1) {
            biasedColumn = primaryKey.getKeyColumns().get(0);
        }

        // map column to SN in select list
        Map<ColumnMeta, Integer> columnIdMap = Maps.newHashMap();
        for (int i = 0; i < analyzeColumnList.size(); i++) {
            columnIdMap.put(analyzeColumnList.get(i), i);
        }

        // find candidate hot column set
        Set<BitSet> columnBitSet = Sets.newHashSet();
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            BitSet bit = new BitSet();
            for (ColumnMeta column : indexMeta.getKeyColumns()) {
                // skip any index start with biasedColumn
                if (column == biasedColumn) {
                    break;
                }
                if (!supportSkewCheck(column)) {
                    break;
                }
                Integer id = columnIdMap.get(column);
                bit.set(id);
                if (!columnBitSet.contains(bit)) {
                    columnBitSet.add((BitSet) bit.clone());
                }
            }
        }

        // sort according to cardinality
        List<BitSet> orderedColumns = columnBitSet.stream().sorted((x, y) -> {
            if (x.cardinality() > y.cardinality()) {
                return -1;
            }
            if (x.cardinality() < y.cardinality()) {
                return 1;
            }
            return x.toString().compareTo(y.toString());
        }).collect(Collectors.toList());
        List<List<Integer>> columnSns = Lists.newArrayList();
        for (BitSet bitSet : orderedColumns) {

            List<Integer> columnSn = Lists.newArrayList();
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                columnSn.add(i);
            }
            columnSns.add(columnSn);
        }
        return columnSns;
    }

    static void checkFailPoint(String fpKey) {
        if (FailPoint.isKeyEnable(fpKey)) {
            throw new TddlRuntimeException(ErrorCode.ERR_STATISTIC_JOB_INTERRUPTED, "FailPoint : " + fpKey);
        }
    }

    static class ColumnListCounter {
        List<Integer> columnSn;
        Row row;
        List<ColumnMeta> analyzeColumnList;

        public ColumnListCounter(List<Integer> columnSn, Row row, List<ColumnMeta> analyzeColumnList) {
            this.columnSn = columnSn;
            this.row = row;
            this.analyzeColumnList = analyzeColumnList;
        }

        @Override
        public int hashCode() {
            int hashCode = 1;
            for (int sn : columnSn) {
                hashCode = 31 * hashCode + Objects.hashCode(row.getObject(sn));
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ColumnListCounter other = (ColumnListCounter) obj;
            for (int sn : columnSn) {
                if (analyzeColumnList.get(sn).getDataType().compare(row.getObject(sn), other.row.getObject(sn)) != 0) {
                    return false;
                }
            }
            return true;
        }
    }

    private static boolean orderedContain(List<Integer> source, List<Integer> target) {
        int tLoc = 0;
        for (int cur : source) {
            if (cur > target.get(tLoc)) {
                return false;
            }
            if (cur == target.get(tLoc)) {
                tLoc++;
            }
            if (tLoc == target.size()) {
                return true;
            }
        }
        return tLoc == target.size();
    }

    public static void buildSkew(
        String schemaName,
        String logicalTableName,
        List<ColumnMeta> analyzeColumnList,
        List<Row> rows,
        float sampleRate) {
        if (rows.size() == 0) {
            return;
        }
        List<List<Integer>> columnSns = buildColumnBitSet(schemaName, logicalTableName, analyzeColumnList);

        // check skew
        List<Set<String>> skewCols = Lists.newArrayList();
        List<List<Integer>> ansColumnSns = Lists.newArrayList();
        for (List<Integer> columnSn : columnSns) {

            // check where super set is marked as hot
            boolean covered = false;
            for (List<Integer> ansColumnSn : ansColumnSns) {
                if (orderedContain(ansColumnSn, columnSn)) {
                    covered = true;
                    break;
                }
            }
            if (covered) {
                continue;
            }

            StreamSummary<ColumnListCounter> summary = new StreamSummary<>(10000);
            for (Row r : rows) {
                summary.offer(new ColumnListCounter(columnSn, r, analyzeColumnList));
            }
            long count = summary.topK(1).get(0).getCount();
            // The criterion for skewness is temporarily set as having the same quantity of sample values greater than 5
            // and estimated values exceeding 10,000.
            if (count > 5 && (count / sampleRate) > 10000) {
                ansColumnSns.add(columnSn);
            }
        }

        for (List<Integer> ansColumnSn : ansColumnSns) {
            Set<String> columnSet = Sets.newHashSet();
            for (int sn : ansColumnSn) {
                columnSet.add(analyzeColumnList.get(sn).getName().toLowerCase());
            }
            skewCols.add(columnSet);
        }

        StatisticManager.getInstance().getCacheLine(schemaName, logicalTableName).setSkewCols(skewCols);
    }

    public static void buildCardinalityAndNullCount(String schemaName,
                                                    String logicalTableName,
                                                    List<ColumnMeta> analyzeColumnList,
                                                    List<Row> rows,
                                                    ExecutionContext ec,
                                                    long rowCount) {
        StatisticManager.CacheLine cacheLine =
            StatisticManager.getInstance().getCacheLine(schemaName, logicalTableName);

        for (int i = 0; i < analyzeColumnList.size(); i++) {
            if (ec != null && CrossEngineValidator.isJobInterrupted(ec)) {
                long jobId = ec.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }

            String colName = analyzeColumnList.get(i).getField().getOriginColumnName();

            HyperLogLog hyperLogLog = new HyperLogLog(16);
            StatisticUtils.GEESample geeSample = new StatisticUtils.GEESample(rows.size());
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
    }

    public static void buildTopnAndHistogram(String schemaName,
                                             String logicalTableName,
                                             List<ColumnMeta> analyzeColumnList,
                                             List<Row> rows,
                                             ExecutionContext ec,
                                             float sampleRate,
                                             double sampleRateUp,
                                             int histogramBucketSize,
                                             boolean collectCharHistogram) {
        StatisticManager.CacheLine cacheLine =
            StatisticManager.getInstance().getCacheLine(schemaName, logicalTableName);

        for (int i = 0; i < analyzeColumnList.size(); i++) {
            if (ec != null && CrossEngineValidator.isJobInterrupted(ec)) {
                long jobId = ec.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
            int finalI = i;
            List<Object> objs =
                rows.stream().map(r -> r.getObject(finalI)).filter(o -> o != null).collect(Collectors.toList());
            String colName = analyzeColumnList.get(i).getField().getOriginColumnName();
            assert colName != null;
            colName = colName.toLowerCase();
            if (!CollectionUtils.isEmpty(objs)) {
                if (objs.get(0) instanceof Slice) {
                    objs = objs.stream().map(o -> ((Slice) o).toStringUtf8()).collect(Collectors.toList());
                } else {
                    if (objs.get(0) instanceof Decimal) {
                        objs = objs.stream().map(o -> ((Decimal) o).toBigDecimal()).collect(Collectors.toList());
                    }
                }
            } else {
                if (rows.size() > 0) {
                    cacheLine.getAllNullCols().add(colName);
                }
            }
            DataType dataType = analyzeColumnList.get(i).getField().getDataType();
            TopN topN = new TopN(dataType, sampleRateUp);
            objs.forEach(obj -> topN.offer(obj));

            boolean isReady = topN.build(
                StatisticUtils.canUseNewTopN(schemaName, logicalTableName, colName),
                (long) (rows.size() / sampleRate / sampleRateUp),
                sampleRate * sampleRateUp
            );

            if (isReady) {
                cacheLine.setTopN(colName, topN);
            } else {
                cacheLine.setTopN(colName, null);
            }
            if (!DataTypeUtil.isChar(dataType) || collectCharHistogram) {
                Histogram h = new Histogram(histogramBucketSize, dataType, (float) sampleRateUp);
                boolean isBuilt =
                    h.buildFromData(objs.stream().filter(d -> isReady ? topN.get(d) == 0 : true).toArray());
                if (isBuilt) {
                    cacheLine.setHistogram(colName, h);
                }
            } else {
                if (cacheLine.getHistogramMap() != null) {
                    cacheLine.getHistogramMap().remove(colName);
                }
            }
        }
    }

    private static boolean supportSkewCheck(ColumnMeta columnMeta) {
        DataType dataType = columnMeta.getDataType();
        return DataTypeUtil.isStringType(dataType) ||
            DataTypeUtil.isUnderLongType(dataType) ||
            DataTypeUtil.isDateType(dataType);
    }

    /**
     * check whether to use new topN for specific column
     *
     * @return true if can use new topN
     */
    public static boolean canUseNewTopN(String schemaName, String logicalTableName, String columnName) {
        // don't use new
        if (!InstConfUtil.getBool(ConnectionParams.NEW_TOPN)) {
            return false;
        }
        if (StringUtils.isEmpty(columnName)) {
            return false;
        }
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);

        // check prefix of primary key
        IndexMeta primaryKey = tableMeta.getPrimaryIndex();
        if (primaryKey != null) {
            for (ColumnMeta column : primaryKey.getKeyColumns()) {
                if (columnName.equalsIgnoreCase(column.getName())) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Row purgeRowForHistogram(Row r, int size, Boolean purgeMysqlTime) {
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
                DataType dt = r.getParentCursorMeta().getColumnMeta(i).getDataType();
                if (purgeMysqlTime && DataTypeUtil.isMysqlTimeType(dt)) {
                    byte[] bytes = r.getBytes(i);
                    tmpRow.setObject(i, packDateTypeToLong(dt, bytes));
                } else {
                    tmpRow.setObject(i, columnValue);
                }
            } catch (Throwable e) {
                // deal with TResultSet getObject error
                continue;
            }
        }
        return tmpRow;
    }

    public static double scanAnalyzeOnPkRange(String schema, String logicalTableName, List<ColumnMeta> columnMetaList,
                                              float sampleRate, int maxSampleRows, List<Row> rows,
                                              PhyTableOperation phyTableOperation,
                                              Map<Integer, ParameterContext> parameterContextMap) {
        // scan sampling
        try {
            StringBuilder hint = new StringBuilder();
            hint.append(
                "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true,ENABLE_DIRECT_PLAN=false,ENABLE_COLUMNAR_SCAN_EXEC=false) */ ");
            String traceId = "";
            hint.append(String.format(" /* trace_id(%s) */ ", traceId));

            IServerConfigManager serverConfigManager = GsiUtils.getServerConfigManager();
            ResultSet resultSet = null;
            Connection connection = null;
            try {
                connection = (Connection) serverConfigManager.getTransConnection(schema);
                PreparedStatement statement = connection.prepareStatement(hint + phyTableOperation.getNativeSql());
                ParameterMethod.setParameters(statement, parameterContextMap);
                resultSet = statement.executeQuery();
                int rowCount = 0;
                CursorMeta cursorMeta = CursorMeta.build(columnMetaList);
                Random random = new Random();
                while (resultSet.next()) {
                    Object[] objects = new Object[columnMetaList.size()];
                    for (int columnIndex = 1; columnIndex <= columnMetaList.size(); columnIndex++) {
                        objects[columnIndex - 1] = resultSet.getObject(columnIndex);
                    }
                    if (rowCount < maxSampleRows) {
                        rows.add(new ArrayRow(cursorMeta, objects));
                        rowCount++;
                    } else {
                        int rowIndex = random.nextInt(maxSampleRows) - 1;
                        rows.set(rowIndex, new ArrayRow(cursorMeta, objects));
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (resultSet != null) {
                        resultSet.close();
                    }

                } catch (SQLException exception) {
                    throw new RuntimeException(exception);
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        } catch (RuntimeException | SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                " scan sample on pk range failed!, and the cause is "
                    + Optional.ofNullable(e.getMessage()).orElse(" unknown cause"));
        }
        return (double) rows.size() * sampleRate / maxSampleRows;
    }

    // purge is true by default.
    // purge is false when scan analyze on pks driven by backfill, because pk is not nullable, and generally not long.
    public static double scanAnalyze(String schema, String logicalTableName, List<ColumnMeta> columnMetaList,
                                     float sampleRate, int maxSampleSize, List<Row> rows, Boolean purge) {
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
            ec.getParamManager().getProps().put(ConnectionParams.CHOOSE_STREAMING.getName(), "true");
            ExecutionPlan plan = Planner.getInstance().plan(ByteString.from(sql), ec);
            rc = PlanExecutor.execute(plan, ec);

            Row r = rc.doNext();
            Random rand = new Random();
            int rowcount = 0;
            do {
                if (r != null) {
                    if (rowcount > maxSampleSize) {
                        if (rand.nextInt(rowcount) < maxSampleSize) {
                            rows.set(rand.nextInt(maxSampleSize),
                                purgeRowForHistogram(r, columnMetaList.size(), purge));
                        } else {
                            // ignore
                        }
                    } else {
                        rows.add(purgeRowForHistogram(r, columnMetaList.size(), purge));
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
                rc.close(new ArrayList<>());
            }
            if (trx != null) {
                trx.close();
            }
            if (ec != null) {
                ec.clearContextAfterTrans();
            }
        }
    }

    protected static String constructScanSamplingSql(String logicalTableName, List<ColumnMeta> columnMetaList,
                                                     float sampleRate) {
        StringBuilder sql = new StringBuilder();

        String cmdExtraSamplePercentage = "";
        cmdExtraSamplePercentage = ",sample_percentage=" + sampleString(sampleRate * 100);
        sql.append(
            "/*+TDDL:cmd_extra("
                + "enable_post_planner=false,enable_index_selection=false,merge_union=false,enable_direct_plan=false"
                + cmdExtraSamplePercentage + ") */ "
                + "select ");
        boolean first = true;
        for (ColumnMeta columnMeta : columnMetaList) {
            if (first) {
                first = false;
            } else {
                sql.append(",");
            }
            sql.append(SqlIdentifier.surroundWithBacktick(columnMeta.getName()));
        }
        sql.append(" from ");
        sql.append(SqlIdentifier.surroundWithBacktick(logicalTableName));
        return sql.toString();
    }

    protected static String constructScanSamplingSqlOnPkRange(String logicalTableName, List<ColumnMeta> columnMetaList,
                                                              float sampleRate, Map<Integer, ParameterContext> leftRow,
                                                              Map<Integer, ParameterContext> rightRow) {
        StringBuilder sql = new StringBuilder();

        String cmdExtraSamplePercentage = "";
        cmdExtraSamplePercentage = ",sample_percentage=" + sampleString(sampleRate * 100);
        sql.append(
            "/*+TDDL:cmd_extra("
                + "enable_post_planner=false,enable_index_selection=false,merge_union=false,enable_direct_plan=false"
                + cmdExtraSamplePercentage + ") */ "
                + "select ");
        boolean first = true;
        for (ColumnMeta columnMeta : columnMetaList) {
            if (first) {
                first = false;
            } else {
                sql.append(",");
            }
            sql.append(SqlIdentifier.surroundWithBacktick(columnMeta.getName()));
        }
        sql.append(" from ");
        sql.append(SqlIdentifier.surroundWithBacktick(logicalTableName));
        return sql.toString();
    }

    private static String constructRandomScanSamplingSql(String logicalTableName, List<ColumnMeta> columnMetaList,
                                                         float sampleRate) {
        StringBuilder sql = new StringBuilder();

        sql.append(
                "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true,ENABLE_DIRECT_PLAN=false,ENABLE_COLUMNAR_SCAN_EXEC=false) */ ")
            .append("select ");
        boolean first = true;
        for (ColumnMeta columnMeta : columnMetaList) {
            if (first) {
                first = false;
            } else {
                sql.append(",");
            }
            sql.append(SqlIdentifier.surroundWithBacktick(columnMeta.getName()));
        }
        sql.append(" from ").append(SqlIdentifier.surroundWithBacktick(logicalTableName));

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
            if (conn != null && !conn.isClosed()) {
                StatisticUtils.resetInformationSchemaCache(conn);
            }
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

    public static void avoidInformationSchemaCache(Connection conn) throws SQLException {
        if (InstanceVersion.isMYSQL80()) {
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
    }

    public static void resetInformationSchemaCache(Connection conn) throws SQLException {
        final String INFORMATION_SCHEMA_STATS_EXPIRY = "information_schema_stats_expiry";
        final String SHOW_VARIABLES = "show global variables like 'information_schema_stats_expiry'";
        Map<String, Object> globalServerVariables = MetaDbVariableConfigManager.getInstance().getDnVariableConfigMap();
        Object val;
        if (InstanceVersion.isMYSQL80()) {
            try (Statement showVar = conn.createStatement(); ResultSet rs = showVar.executeQuery(SHOW_VARIABLES)) {
                if (rs.next()) {
                    val = rs.getObject(2);
                } else {
                    val = (globalServerVariables.containsKey(INFORMATION_SCHEMA_STATS_EXPIRY) ?
                        globalServerVariables.get(INFORMATION_SCHEMA_STATS_EXPIRY) :
                        TddlConstants.INFORMATION_SCHEMA_STATS_EXPIRY_TIME);
                }
            }
            // avoid mysql 8.0 cache information_schema
            Statement setVarStmt = conn.createStatement();
            try {
                setVarStmt.execute("set information_schema_stats_expiry = " + val);
            } catch (Throwable t) {
                // pass
            } finally {
                JdbcUtils.close(setVarStmt);
            }
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
            List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
            topology = new HashMap<>();
            for (PartitionSpec partitionSpec : partitionSpecs) {
                String groupKey = partitionSpec.getLocation().getGroupKey();
                String physicalTableName = partitionSpec.getLocation().getPhyTableName();
                Set<String> physicalTableNames = topology.computeIfAbsent(groupKey, k -> new HashSet<>());
                physicalTableNames.add(physicalTableName);
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


    public static float getSampleRate(long rowCount){
        float sampleRate = 1;
        if (rowCount > 0) {
            sampleRate = (float) DEFAULT_SAMPLE_SIZE / rowCount;
            if (sampleRate > 1f) {
                sampleRate = 1f;
            } else if (sampleRate < 0.000001f) {
                sampleRate = 0.000001f;
            }
        }
        return sampleRate;
    }

    public static int getHistogramBucketSize(long sampleSize) {
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
        return histogramBucketSize;
    }

    public static Set<String> getMasterStorageAllDnIds() {
        return StorageHaManager.getInstance().getMasterStorageList()
            .stream()
            .filter(s -> !s.isMetaDb())
            .map(StorageInstHaContext::getStorageInstId)
            .collect(Collectors.toSet());
    }
}
