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
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateRowCountSyncAction;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.executor.utils.SchemaMetaUtil;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.NDVSketchService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticCollector;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.metadata.InfoSchemaCommon;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.clearspring.analytics.stream.membership.BloomFilter;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MysqlStatisticCollector extends StatisticCollector {

    private static final Logger logger = LoggerFactory.getLogger("statistics");

    static final boolean USE_GEE_CARDINALITY = false;
    static final boolean USE_HLL_CARDINALITY = false;
    static final boolean USE_BC_GEE_CARDINALITY = true;

    /**
     * initial tick delay time 30 mins , in (ms)
     */
    public static final int INITIAL_DELAY = 30 * 60 * 1000;

    /**
     * tick period time 30 mins , in (ms)
     */
    public static final int PERIOD = 30 * 60 * 1000;

    /**
     * tick time unit milliseconds
     */
    public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * select table rows sql, need to concat with where filter
     */
    private static final String SELECT_TABLE_ROWS_SQL =
            "SELECT table_schema, table_name, table_rows FROM information_schema.tables ";

    /**
     * show tables
     */
    private static final String SHOW_TABLES = "SHOW TABLES ";

    /**
     * select column cardinality sql, need to concat with where filter
     */
    private static final String SELECT_COLUMN_CARDINALITY_SQL =
            "SELECT table_schema, table_name, column_name, cardinality FROM information_schema.statistics ";

    /**
     * for remember the init state
     */
    private boolean hasInit = false;

    /**
     * variable to record actual sql run per collection
     */
    private int actualSqlCount = 0;

    /**
     * the last collection Time of schema
     */
    private long schemaLastCollectTime = 0L;

    /**
     * AppName
     */
    private final String schemaName;

    /**
     * AppName co-related statistic manager
     */
    private final StatisticManager statisticManager;

    /**
     * TDataSource connection properties manager
     */
    private final ParamManager paramManager;

    private final SystemTableTableStatistic systemTableTableStatistic;

    /**
     * AppName co-related system table COLUMN_STATISTICS
     */
    private final SystemTableColumnStatistic systemTableColumnStatistic;

    private final SystemTableNDVSketchStatistic systemTableNDVSketchStatistic;

    private final NDVSketchService ndvSketch;

    private DataSource tDataSource;

    private Set<String> needCollectorTables = new HashSet<>();

    public MysqlStatisticCollector(String schemaName,
                                   Map<String, Object> connectionProperties,
                                   StatisticManager statisticManager,
                                   SystemTableTableStatistic systemTableTableStatistic,
                                   SystemTableColumnStatistic systemTableColumnStatistic,
                                   SystemTableNDVSketchStatistic systemTableNDVSketchStatistic,
                                   NDVSketchService ndvSketch,
                                   DataSource tdataSource) {
        this.schemaName = schemaName;
        this.paramManager = new ParamManager(connectionProperties);
        this.statisticManager = statisticManager;
        this.systemTableTableStatistic = systemTableTableStatistic;
        this.systemTableColumnStatistic = systemTableColumnStatistic;
        this.systemTableNDVSketchStatistic = systemTableNDVSketchStatistic;
        this.ndvSketch = ndvSketch;
        this.tDataSource = tdataSource;
    }

    @Override
    public synchronized void run() {
        MDC.put(MDC.MDC_KEY_APP, schemaName.toLowerCase());
        long start = System.currentTimeMillis();
        Map<String, Map<String, Long>> tablesRowCountCache = new HashMap<>();
        actualSqlCount = 0;
        int count = 0;
        if (disableByUser()) {
            return;
        }
        if (!shouldRunAtThatTime(schemaLastCollectTime, start)) {
            return;
        }

        if (SystemDbHelper.isDBBuildIn(schemaName)) {
            return;
        }

        List<String> dbNameList = getActualDbNameList();
        String whereFilter = constructInformationSchemaTablesWhereFilter(dbNameList);

        for (Map.Entry<String, StatisticManager.CacheLine> entry : statisticManager.getStatisticCache().entrySet()) {
            String logicalTableName = entry.getKey();
            StatisticManager.CacheLine cacheLine = entry.getValue();
            if (hasExpire(cacheLine)) {
                collectRowCount(logicalTableName, whereFilter, tablesRowCountCache);
                count++;
            } else {
                continue;
            }
        }

        long end = System.currentTimeMillis();
        schemaLastCollectTime = end;
        logger.info("collectRowCount " + count + " tables statistics with sql count " + actualSqlCount + " consuming "
                + (end - start) / 1000.0 + " seconds");
        persistStatistic(false);
    }

    @Override
    public synchronized void collectTables() {
        getTables();
        collectRowCount();
        collectCardinality();
        getNeedCollectorTables().clear();
        persistStatistic(false);
    }

    private void updateMetaDbInformationSchemaTables() {
        updateMetaDbInformationSchemaTables(null);
    }

    private void updateMetaDbInformationSchemaTables(String logicalTableName) {
        if (ConfigDataMode.isMasterMode()) {
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
    }

    @Override
    public synchronized void collectRowCount() {
        long start = System.currentTimeMillis();
        int count = 0;
        Map<String, Map<String, Long>> tablesRowCountCache = new HashMap<>();
        actualSqlCount = 0;
        List<String> dbNameList = getActualDbNameList();
        String informationSchemaTablesWhereFilter = constructInformationSchemaTablesWhereFilter(dbNameList);
        for (String tableName : getNeedCollectorTables()) {
            collectRowCount(tableName, informationSchemaTablesWhereFilter, tablesRowCountCache);
            count++;
        }
        long end = System.currentTimeMillis();
        schemaLastCollectTime = end;
        logger.info("collectRowCount " + count + " tables statistics with sql count " + actualSqlCount + " consuming "
                + (end - start) / 1000.0 + " seconds");
    }

    private List<String> getAllTables(String groupName) {
        List<String> tables = new ArrayList<>();
        if (InfoSchemaCommon.MYSQL_SYS_SCHEMAS.contains(groupName.toUpperCase())) {
            return tables;
        }

        MyDataSourceGetter myDataSourceGetter = new MyDataSourceGetter(schemaName);
        TGroupDataSource ds = myDataSourceGetter.getDataSource(schemaName, groupName);
        if (ds == null) {
            logger.error("getAllTables fail, datasource is null, group name is " + groupName);
            return tables;
        }
        String dbName = getDbName(ds);

        if (dbName == null) {
            return tables;
        }
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            avoidInformationSchemaCache(conn);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(SHOW_TABLES);

            while (rs.next()) {
                String tableName = rs.getString(1);
                // Exclude table name of secondary index and system table name
                if (TStringUtil.contains(tableName, InfoSchemaCommon.KEYWORD_SECONDARY_INDEX)
                        || SystemTables.contains(tableName)) {
                    continue;
                }
                tables.add(tableName);
            }
            return tables;
        } catch (Throwable e) {
            logger.error("error arise when execute getAllTables", e);
            return tables;
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    private Set<String> getTables() {
        getNeedCollectorTables().clear();
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        TddlRule tddlRule = tddlRuleManager.getTddlRule();
        Collection<TableRule> tableRules = tddlRule.getTables();
        Map<String, String> dbIndexMap = tddlRule.getDbIndexMap();
        for (TableRule tableRule : tableRules) {
            String table = tableRule.getVirtualTbName();
            getNeedCollectorTables().add(table);
        }
        if (!onlyCollectorTablesFromRule()) {
            //这里主要是统计未配路由规则的单表
            String defaultGroup = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
            List<String> defaultDbTables = getAllTables(defaultGroup);
            tddlRuleManager.mergeTableRule(getNeedCollectorTables(), tableRules, dbIndexMap, defaultDbTables, true);
        }
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        getNeedCollectorTables().addAll(partitionInfoManager.getPartitionTables());
        return getNeedCollectorTables();
    }

    @Override
    public synchronized void collectCardinality() {
        long start = System.currentTimeMillis();
        int count = 0;
        /**
         * dbName -> {physicalTableName -> { columnName -> RowCount}}
         */
        Map<String, Map<String, Map<String, Long>>> columnCardinalityCache = new HashMap<>();
        actualSqlCount = 0;

        List<String> dbNameList = getActualDbNameList();
        String informationSchemaStatisticsWhereFilter = constructInformationSchemaStatisticsWhereFilter(dbNameList);
        for (String tableName : getNeedCollectorTables()) {
            collectCardinality(tableName, informationSchemaStatisticsWhereFilter, columnCardinalityCache);
            count++;
        }
        long end = System.currentTimeMillis();
        schemaLastCollectTime = end;
        logger.info(
                "collectCardinality " + count + " tables statistics with sql count " + actualSqlCount + " consuming "
                        + (end - start) / 1000.0 + " seconds");
    }

    public synchronized void collectCardinality(String logicalTableName, String whereFilter,
                                                Map<String, Map<String, Map<String, Long>>> columnCardinalityCache) {
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);
        if (!partitionInfoManager.isNewPartDbTable(logicalTableName)) {
            if (tableRule == null && onlyCollectorTablesFromRule()) {
                logger
                        .error("no table rule for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
                return;
            }
        }

        TableMeta tableMeta = null;
        try {
            tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        } catch (Throwable throwable) {
            logger.error("no table meta for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            return;
        }
        if (tableMeta == null) {
            return;
        }

        Map<String, Set<String>> topology;
        if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(logicalTableName);
            // FIXME if support subPartition
            if (partitionInfo.getSubPartitionBy() != null) {
                throw new AssertionError("do not support subpartition for statistic collector");
            } else {
                List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
                topology = new HashMap<>();
                for (PartitionSpec partitionSpec : partitionSpecs) {
                    String groupKey = partitionSpec.getLocation().getGroupKey();
                    String physicalTableName = partitionSpec.getLocation().getPhyTableName();
                    Set<String> physicalTableNames = topology.get(groupKey);
                    if (physicalTableNames == null) {
                        physicalTableNames = new HashSet<>();
                        topology.put(groupKey, physicalTableNames);
                    }
                    physicalTableNames.add(physicalTableName);
                }
            }
        } else {
            if (tableRule == null) {
                String dbIndex = tddlRuleManager.getDefaultDbIndex(logicalTableName);
                topology = new HashMap<>();
                topology.put(dbIndex, Sets.newHashSet(logicalTableName));
            } else {
                topology = tableRule.getStaticTopology();
                if (topology == null || topology.size() == 0) {
                    topology = tableRule.getActualTopology();
                }
            }
        }

        List<ColumnMeta> columnMetaList = tableMeta.getAllColumns();
        LOOP:
        for (ColumnMeta columnMeta : columnMetaList) {
            String columnName = columnMeta.getName().toLowerCase();
            StatisticManager.CacheLine cacheLine = statisticManager.getCacheLine(logicalTableName);
            // already has analyzed table, skip
            if (cacheLine.getCardinalityMap() != null
                    && cacheLine.getCountMinSketchMap() != null
                    && cacheLine.getHistogramMap() != null
                    && cacheLine.getNullCountMap() != null) {
                continue;
            }
            long physicalCardinalitySum = 0;
            int physicalTableCount = 0;
            for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
                String groupName = entry.getKey();
                Set<String> physicalTables = entry.getValue();
                if (groupName == null || physicalTables == null) {
                    continue LOOP;
                }
                Long physicalCardinality =
                        queryCardinality(groupName, physicalTables, columnName, whereFilter, columnCardinalityCache);
                if (physicalCardinality == null) {
                    continue LOOP; // no index stats
                } else {
                    physicalCardinalitySum += physicalCardinality;
                    physicalTableCount += physicalTables.size();
                }
            }
            if (physicalTableCount <= 0) {
                continue;
            }
            long logicalCardinality;

            if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
                // partition table
                PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(logicalTableName);
                TreeSet<String> partitionKeys = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                TreeSet<String> subPartitionKeys = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                if (partitionInfo.getPartitionBy() != null) {
                    partitionKeys.addAll(partitionInfo.getPartitionBy().getPartitionColumnNameList());
                }
                if (partitionInfo.getSubPartitionBy() != null) {
                    subPartitionKeys.addAll(partitionInfo.getSubPartitionBy().getSubPartitionColumnNameList());
                }

                if (partitionKeys.size() == 1 && partitionKeys.contains(columnName)
                        && subPartitionKeys.size() == 1 && subPartitionKeys.contains(columnName)) {
                    logicalCardinality = physicalCardinalitySum;
                } else if (partitionKeys.size() == 1 && partitionKeys.contains(columnName)
                        && subPartitionKeys.isEmpty()) {
                    logicalCardinality = physicalCardinalitySum;
                } else if (partitionKeys.isEmpty()
                        && subPartitionKeys.size() == 1 && subPartitionKeys.contains(columnName)) {
                    logicalCardinality = physicalCardinalitySum;
                } else {
                    logicalCardinality = (long) (physicalCardinalitySum / Math.sqrt(physicalTableCount));
                }

            } else if (tableRule == null) {
                //The table maybe a single table.
                logicalCardinality = physicalCardinalitySum;
            } else {
                // sharding table
                TreeSet<String> dbPartitionKeys = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                dbPartitionKeys.addAll(tableRule.getDbPartitionKeys());

                TreeSet<String> tbPartitionKeys = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                tbPartitionKeys.addAll(tableRule.getTbPartitionKeys());

                if (dbPartitionKeys.size() == 1 && dbPartitionKeys.contains(columnName)
                        && tbPartitionKeys.size() == 1 && tbPartitionKeys.contains(columnName)) {
                    logicalCardinality = physicalCardinalitySum;
                } else if (dbPartitionKeys.size() == 1 && dbPartitionKeys.contains(columnName)
                        && tbPartitionKeys.isEmpty()) {
                    logicalCardinality = physicalCardinalitySum;
                } else if (dbPartitionKeys.isEmpty()
                        && tbPartitionKeys.size() == 1 && tbPartitionKeys.contains(columnName)) {
                    logicalCardinality = physicalCardinalitySum;
                } else {
                    logicalCardinality = (long) (physicalCardinalitySum / Math.sqrt(physicalTableCount));
                }
            }
            statisticManager.getCacheLine(logicalTableName).setCardinality(columnName, logicalCardinality);
        }
    }

    private DataSource getGroupDataSource(String schemaName, String groupName) {
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (executorContext == null) {
            throw new IllegalArgumentException(schemaName + ":" + groupName);
        }
        TopologyHandler topologyHandler = executorContext.getTopologyHandler();
        if (topologyHandler == null) {
            throw new IllegalArgumentException(schemaName + ":" + groupName);
        }
        IGroupExecutor groupExecutor = topologyHandler.get(groupName);
        if (groupExecutor == null) {
            throw new IllegalArgumentException(schemaName + ":" + groupName);
        }
        DataSource ds = groupExecutor.getDataSource();
        return ds;
    }

    private Long queryCardinality(String groupName,
                                  Set<String> physicalTableNames,
                                  String columnName,
                                  String whereFilter,
                                  Map<String, Map<String, Map<String, Long>>> columnCardinalityCache) {

        MyDataSourceGetter myDataSourceGetter = new MyDataSourceGetter(schemaName);
        TGroupDataSource ds = myDataSourceGetter.getDataSource(schemaName, groupName);
        if (ds == null) {
            logger.error("queryCardinality fail, datasource is null, group name is " + groupName);
            return null;
        }
        String dbName = getDbName(ds);
        if (dbName == null) {
            return null;
        }
        Map<String, Map<String, Long>> groupCache = columnCardinalityCache.get(dbName);
        if (groupCache != null) {
            return getCardinalityFromGroupCache(groupCache, physicalTableNames, columnName);
        }
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            actualSqlCount++;
            conn = ds.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(SELECT_COLUMN_CARDINALITY_SQL + whereFilter);
            while (rs.next()) {
                String tableSchema = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                String col = rs.getString("column_name");
                Long cardinality = rs.getLong("cardinality");
                groupCache = columnCardinalityCache.get(tableSchema);
                if (groupCache == null) {
                    groupCache = new HashMap<>();
                    columnCardinalityCache.put(tableSchema, groupCache);
                }
                Map<String, Long> columnMap = groupCache.get(tableName);
                if (columnMap == null) {
                    columnMap = new HashMap<>();
                    groupCache.put(tableName.toLowerCase(), columnMap);
                }
                // use max cardinality
                if (columnMap.get(col.toLowerCase()) != null) {
                    columnMap.put(col.toLowerCase(), Math.max(columnMap.get(col.toLowerCase()), cardinality));
                } else {
                    columnMap.put(col.toLowerCase(), cardinality);
                }
            }
            groupCache = columnCardinalityCache.get(dbName);
            if (groupCache == null) {
                logger.error("getCardinalityFromGroupCache groupCache is null, groupName = " + groupName);
                columnCardinalityCache.put(dbName, new HashMap<>());
                return null;
            }
            return getCardinalityFromGroupCache(groupCache, physicalTableNames, columnName);
        } catch (Throwable e) {
            logger.error("error arise when execute collectRowCount table statistic sql", e);
            return null;
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    private Long getCardinalityFromGroupCache(Map<String, Map<String, Long>> groupCache, Set<String> physicalTableNames,
                                              String columnName) {
        long physicalCardinalitySum = 0;
        for (String physicalTableName : physicalTableNames) {
            Map<String, Long> columnMap = groupCache.get(physicalTableName.toLowerCase());
            if (columnMap == null) {
                return null;
            }
            Long physicalCardinality = columnMap.get(columnName.toLowerCase());
            if (physicalCardinality == null) {
                return null;
            }
            physicalCardinalitySum += physicalCardinality;
        }
        return physicalCardinalitySum;
    }

    @Override
    public synchronized void collectRowCount(String logicalTableName) {
        long start = System.currentTimeMillis();
        Map<String, Map<String, Long>> tablesRowCountCache = new HashMap<>();
        actualSqlCount = 0;

        List<String> dbNameList = getActualDbNameList();
        List<String> physicalNameList = getPhysicalTableNameList(logicalTableName);
        if (physicalNameList == null) {
            return;
        }
        String informationSchemaTablesWhereFilter =
                constructInformationSchemaTablesWhereFilter(dbNameList, physicalNameList);
        collectRowCount(logicalTableName, informationSchemaTablesWhereFilter, tablesRowCountCache);
        long end = System.currentTimeMillis();
        schemaLastCollectTime = end;
        logger.info("collectRowCount 1 tables statistics with sql count " + actualSqlCount + " consuming "
                + (end - start) / 1000.0 + " seconds");
        persistStatistic(logicalTableName, false); // persistStatistic
        SyncManagerHelper.sync(new UpdateRowCountSyncAction(schemaName, logicalTableName,
                statisticManager.getCacheLine(logicalTableName).getRowCount()), schemaName);
    }

    private List<String> getPhysicalTableNameList(String logicalTableName) {

        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(logicalTableName);
            // FIXME if support subPartition

            List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
            return partitionSpecs.stream().map(x -> x.getLocation().getPhyTableName()).collect(Collectors.toList());
        }

        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);
        if (tableRule == null && onlyCollectorTablesFromRule()) {
            logger.error("no table rule for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            return null;
        }
        Map<String, Set<String>> topology;
        if (tableRule == null) {
            String dbIndex = tddlRuleManager.getDefaultDbIndex(logicalTableName);
            topology = new HashMap<>();
            topology.put(dbIndex, Sets.newHashSet(logicalTableName));
        } else {
            topology = tableRule.getStaticTopology();
            if (topology == null || topology.size() == 0) {
                topology = tableRule.getActualTopology();
            }
        }

        List<String> physicalNameList = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            physicalNameList.addAll(entry.getValue());
        }
        return physicalNameList;
    }

    @Override
    public boolean analyzeColumns(String logicalTableName, List<ColumnMeta> columnMetaList,
                                  ParamManager callerParamManager) {
        try {
            if (columnMetaList.isEmpty()) {
                return true;
            }
            Engine engine =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).getEngine();
            logger.debug("start collectRowCount with scan");
            StatisticBuilder statisticBuilder = new StatisticBuilder(statisticManager, tDataSource,
                    callerParamManager, logicalTableName,
                    columnMetaList, SchemaMetaUtil.checkSupportHll(statisticManager.getSchemaName()), false, engine);

            statisticBuilder.prepare();
            statisticBuilder.analyze();
            StatisticManager.CacheLine cacheLine = statisticBuilder.build();

            statisticManager.setCacheLine(logicalTableName, cacheLine);
            /** persist */
            persistStatistic(logicalTableName, true);
            /** sync other nodes */
            SyncManagerHelper.sync(
                    new UpdateStatisticSyncAction(
                            schemaName,
                            logicalTableName,
                            cacheLine),
                    schemaName);
        } catch (Throwable e) {
            logger.error(
                    "analyzeColumns error for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            logger.error(e);
            return false;
        }
        return true;
    }

    @Override
    public boolean sampleColumns(String logicalTableName, List<ColumnMeta> columnMetaList,
                                  ParamManager callerParamManager) {
        try {
            if (columnMetaList.isEmpty()) {
                return true;
            }
            logger.debug("start sample table:" + logicalTableName);
            StatisticBuilder statisticBuilder = new StatisticBuilder(statisticManager, tDataSource,
                    callerParamManager, logicalTableName,
                    columnMetaList, false, false, Engine.INNODB);

            statisticBuilder.prepare();
            statisticBuilder.analyze();
            StatisticManager.CacheLine cacheLine = statisticBuilder.build();

            statisticManager.setCacheLine(logicalTableName, cacheLine);
            /** persist */
            persistStatistic(logicalTableName, true);
            /** sync other nodes */
            SyncManagerHelper.sync(
                    new UpdateStatisticSyncAction(
                            schemaName,
                            logicalTableName,
                            cacheLine),
                    schemaName);
        } catch (Throwable e) {
            logger.error(
                    "sample table error for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            logger.error(e);
            return false;
        }
        return true;
    }

    @Override
    public boolean forceAnalyzeColumns(String logicalTableName, List<ColumnMeta> columnMetaList,
                                       ParamManager callerParamManager) {
        try {
            if (columnMetaList.isEmpty()) {
                return true;
            }
            logger.debug("start collectRowCount with scan");
            Engine engine =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).getEngine();
            StatisticBuilder statisticBuilder = new StatisticBuilder(statisticManager, tDataSource,
                    callerParamManager, logicalTableName,
                    columnMetaList, SchemaMetaUtil.checkSupportHll(statisticManager.getSchemaName(), callerParamManager.getBoolean(ConnectionParams.ENABLE_HLL)), true, engine);

            statisticBuilder.prepare();
            statisticBuilder.analyze();
            StatisticManager.CacheLine cacheLine = statisticBuilder.build();

            statisticManager.setCacheLine(logicalTableName, cacheLine);
            /** persist */
            persistStatistic(logicalTableName, true);
            /** sync other nodes */
            SyncManagerHelper.sync(
                    new UpdateStatisticSyncAction(
                            schemaName,
                            logicalTableName,
                            cacheLine),
                    schemaName);
        } catch (Throwable e) {
            logger.error(
                    "analyzeColumns error for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            logger.error(e);
            return false;
        }
        return true;
    }

    private void persistStatistic(String logicalTableName, boolean withColumnStatistic) {
        long start = System.currentTimeMillis();
        ArrayList<SystemTableTableStatistic.Row> rowList = new ArrayList<>();
        StatisticManager.CacheLine cacheLine = statisticManager.getCacheLine(logicalTableName);
        rowList.add(new SystemTableTableStatistic.Row(logicalTableName.toLowerCase(), cacheLine.getRowCount(),
                cacheLine.getLastModifyTime()));
        systemTableTableStatistic.batchReplace(rowList);

        ArrayList<SystemTableColumnStatistic.Row> columnRowList = new ArrayList<>();
        if (withColumnStatistic && cacheLine.getCardinalityMap() != null && cacheLine.getCountMinSketchMap() != null
                && cacheLine.getHistogramMap() != null && cacheLine.getNullCountMap() != null) {
            for (String columnName : cacheLine.getCardinalityMap().keySet()) {
                columnRowList.add(new SystemTableColumnStatistic.Row(
                        logicalTableName.toLowerCase(),
                        columnName,
                        cacheLine.getCardinalityMap().get(columnName),
                        cacheLine.getCountMinSketchMap().get(columnName),
                        cacheLine.getHistogramMap().get(columnName),
                        cacheLine.getTopNMap().get(columnName),
                        cacheLine.getNullCountMap().get(columnName),
                        cacheLine.getSampleRate(),
                        cacheLine.getLastModifyTime()));
            }
        }
        systemTableColumnStatistic.batchReplace(columnRowList);
        updateMetaDbInformationSchemaTables(logicalTableName);

//        if (ConfigDataMode.isPolarDbX() && systemTableNDVSketchStatistic != null && ndvSketch != null) {
//            systemTableNDVSketchStatistic.batchReplace(ndvSketch.serialize());
//        }
        long end = System.currentTimeMillis();
        logger.debug("persist tables statistics consuming " + (end - start) / 1000.0 + " seconds");
    }

    private void persistStatistic(boolean withColumnStatistic) {
        long start = System.currentTimeMillis();
        ArrayList<SystemTableTableStatistic.Row> rowList = new ArrayList<>();
        ArrayList<SystemTableColumnStatistic.Row> columnRowList = new ArrayList<>();
        for (Map.Entry<String, StatisticManager.CacheLine> entry : statisticManager.getStatisticCache().entrySet()) {
            String logicalTableName = entry.getKey();
            StatisticManager.CacheLine cacheLine = entry.getValue();
            if (cacheLine.getRowCount() > 0) {
                rowList.add(new SystemTableTableStatistic.Row(logicalTableName.toLowerCase(), cacheLine.getRowCount(),
                        cacheLine.getLastModifyTime()));
            }
            if (withColumnStatistic
                    && cacheLine.getCardinalityMap() != null
                    && cacheLine.getCountMinSketchMap() != null
                    && cacheLine.getHistogramMap() != null
                    && cacheLine.getNullCountMap() != null) {
                for (String columnName : cacheLine.getCardinalityMap().keySet()) {
                    columnRowList.add(new SystemTableColumnStatistic.Row(
                            logicalTableName.toLowerCase(),
                            columnName,
                            cacheLine.getCardinalityMap().get(columnName),
                            cacheLine.getCountMinSketchMap().get(columnName),
                            cacheLine.getHistogramMap().get(columnName),
                            cacheLine.getTopNMap().get(columnName),
                            cacheLine.getNullCountMap().get(columnName),
                            cacheLine.getSampleRate(),
                            cacheLine.getLastModifyTime()));
                }
            }
        }
        systemTableTableStatistic.batchReplace(rowList);
        systemTableColumnStatistic.batchReplace(columnRowList);
        updateMetaDbInformationSchemaTables();
//
//        if (ConfigDataMode.isPolarDbX() && systemTableNDVSketchStatistic != null && ndvSketch != null) {
//            systemTableNDVSketchStatistic.batchReplace(ndvSketch.serialize());
//        }

        long end = System.currentTimeMillis();
        logger.debug("persist tables statistics consuming " + (end - start) / 1000.0 + " seconds");
    }

    /**
     * logicalTableName
     * whereFilter for query informationSchema
     * informationSchemaCache dbName -> {physicalTableName -> RowCount}
     */
    private void collectRowCount(String logicalTableName, String whereFilter,
                                 Map<String, Map<String, Long>> tablesRowCountCache) {
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        Map<String, Set<String>> topology;
        try {
            TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
            if (Engine.isFileStore(tableMeta.getEngine())) {
                long tableCacheRowCount;
                try (Connection conn = tDataSource.getConnection()) {
                    try (Statement stmt = conn.createStatement()) {
                        try (ResultSet resultSet = stmt.executeQuery("/*+TDDL:cmd_extra(MERGE_CONCURRENT=true)*/ select count(*) from " + logicalTableName)) {
                            resultSet.next();
                            tableCacheRowCount = resultSet.getLong(1);
                        }
                    }
                } catch (Throwable t) {
                    return;
                }
                statisticManager.setRowCount(logicalTableName, tableCacheRowCount);
                return;
            }
        } catch (Throwable t) {
            return;
        }

        if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(logicalTableName);
            // FIXME if support subPartition
            if (partitionInfo.getSubPartitionBy() != null) {
                throw new AssertionError("do not support subpartition for statistic collector");
            } else {
                List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
                topology = new HashMap<>();
                for (PartitionSpec partitionSpec : partitionSpecs) {
                    String groupKey = partitionSpec.getLocation().getGroupKey();
                    String physicalTableName = partitionSpec.getLocation().getPhyTableName();
                    Set<String> physicalTableNames = topology.get(groupKey);
                    if (physicalTableNames == null) {
                        physicalTableNames = new HashSet<>();
                        topology.put(groupKey, physicalTableNames);
                    }
                    physicalTableNames.add(physicalTableName);
                }
            }
        } else {
            TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
            TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);
            if (tableRule == null && onlyCollectorTablesFromRule()) {
                logger
                        .error("no table rule for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            }
            if (tableRule == null) {
                String dbIndex = tddlRuleManager.getDefaultDbIndex(logicalTableName);
                topology = new HashMap<>();
                topology.put(dbIndex, Sets.newHashSet(logicalTableName));
            } else {
                topology = tableRule.getStaticTopology();
                if (topology == null || topology.size() == 0) {
                    topology = tableRule.getActualTopology();
                }
            }
        }

        long rowCountSum = 0;
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            rowCountSum += queryRowCount(entry.getKey(), entry.getValue(), whereFilter, tablesRowCountCache);
        }
        statisticManager.setRowCount(logicalTableName, rowCountSum);
    }

    /**
     * groupName
     * physicalTableNames
     * whereFilter for query informationSchema
     * informationSchemaCache dbName -> {physicalTableName -> RowCount}
     */
    private long queryRowCount(String groupName, Set<String> physicalTableNames, String whereFilter,
                               Map<String, Map<String, Long>> tablesRowCountCache) {

        MyDataSourceGetter myDataSourceGetter = new MyDataSourceGetter(schemaName);
        TGroupDataSource ds = myDataSourceGetter.getDataSource(groupName);
        if (ds == null) {
            logger.error("queryRowCount fail, datasource is null, group name is " + groupName);
            return 0;
        }
        String dbName = getDbName(ds);
        if (dbName == null) {
            return 0;
        }
        Map<String, Long> groupCache = tablesRowCountCache.get(dbName);
        if (groupCache != null) {
            return getRowCountFromGroupCache(groupCache, physicalTableNames);
        }
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            actualSqlCount++;
            conn = ds.getConnection();
            avoidInformationSchemaCache(conn);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(SELECT_TABLE_ROWS_SQL + whereFilter);
            while (rs.next()) {
                String tableSchema = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                Long tableRows = rs.getLong("table_rows");
                groupCache = tablesRowCountCache.get(tableSchema);
                if (groupCache == null) {
                    groupCache = new HashMap<>();
                }
                groupCache.put(tableName.toLowerCase(), tableRows);
                tablesRowCountCache.put(tableSchema, groupCache);
            }
            groupCache = tablesRowCountCache.get(dbName);
            if (groupCache == null) {
                logger.error("getFromGroupCache groupCache is null, groupName = " + groupName);
                tablesRowCountCache.put(dbName, new HashMap<>());
                return 0;
            }
            return getRowCountFromGroupCache(groupCache, physicalTableNames);
        } catch (Throwable e) {
            logger.error("error arise when execute collectRowCount table statistic sql", e);
            return 0;
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
            JdbcUtils.close(conn);
        }
    }

    private long getRowCountFromGroupCache(Map<String, Long> groupCache, Set<String> physicalTableNames) {
        long rowCountSum = 0;
        for (String physicalTableName : physicalTableNames) {
            Long c = groupCache.get(physicalTableName.toLowerCase());
            if (c != null) {
                rowCountSum += c;
            }
        }
        return rowCountSum;
    }

    public boolean shouldRunAtThatTime(long lastCollectTime, long now) {
        try {
            int period = getCollectPeriodInMs();
            String startTime = getCollectStartTime();
            String endTime = getCollectEndTime();
            String[] startTimeSplit = startTime.split(":");
            String[] endTimeSplit = endTime.split(":");
            assert startTimeSplit.length == 2;
            assert endTimeSplit.length == 2;

            int startTimeHour = Integer.valueOf(startTimeSplit[0]);
            int startTimeMin = Integer.valueOf(startTimeSplit[1]);
            int endTimeHour = Integer.valueOf(endTimeSplit[0]);
            int endTimeMin = Integer.valueOf(endTimeSplit[1]);

            assert startTimeHour >= 0 && startTimeHour < 24;
            assert startTimeMin >= 0 && startTimeMin < 60;
            assert endTimeHour >= 0 && endTimeHour < 24;
            assert endTimeMin >= 0 && endTimeMin < 60;

            long startTimeInMs = (startTimeHour * 60 + startTimeMin) * 60 * 1000;
            long endTimeInMs = (endTimeHour * 60 + endTimeMin) * 60 * 1000;
            if (now - lastCollectTime < period) {
                return false;
            }
            long todayMs = now - GeneralUtil.startOfToday(now);
            if (endTimeInMs >= startTimeInMs) {
                return todayMs > startTimeInMs && todayMs < endTimeInMs;
            } else {
                return todayMs > startTimeInMs || todayMs < endTimeInMs;
            }
        } catch (Throwable e) {
            logger.error("shouldRunAtThatTime error ", e);
            return false;
        }
    }

    private String constructInformationSchemaStatisticsWhereFilter(List<String> dbNameList) {
        return constructInformationSchemaStatisticsWhereFilter(dbNameList, null);
    }

    private String constructWhereFilter(String dbColumnName, List<String> dbNameList, String physicalTableColumnName,
                                        List<String> physicalTableNameList) {
        if (dbNameList == null || dbNameList.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" where ");
        sb.append(dbColumnName);
        sb.append(" in (");
        boolean first = true;
        for (String dbName : dbNameList) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append("'");
            sb.append(dbName);
            sb.append("'");
        }
        sb.append(")");
        if (physicalTableNameList != null) {
            sb.append(" and ");
            sb.append(physicalTableColumnName);
            sb.append(" in (");
            first = true;
            for (String physicalTableNae : physicalTableNameList) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                sb.append("'");
                sb.append(physicalTableNae.replace("'", "\\'"));
                sb.append("'");
            }
            sb.append(")");
        }
        return sb.toString();
    }

    private String constructInformationSchemaStatisticsWhereFilter(List<String> dbNameList,
                                                                   List<String> physicalTableNameList) {
        String filter = constructWhereFilter("table_schema", dbNameList, "table_name", physicalTableNameList);
        if (filter != null && !filter.isEmpty()) { // only use single column
            filter += " and SEQ_IN_INDEX = 1";
        } else {
            filter = " where SEQ_IN_INDEX = 1";
        }
        return filter;
    }

    private String constructInformationSchemaTablesWhereFilter(List<String> dbNameList,
                                                               List<String> physicalTableNameList) {
        return constructWhereFilter("table_schema", dbNameList, "table_name", physicalTableNameList);
    }

    private String constructInformationSchemaTablesWhereFilter(List<String> dbNameList) {
        return constructInformationSchemaTablesWhereFilter(dbNameList, null);
    }

    private List<String> getActualDbNameList() {
        List<String> result = new ArrayList<>();
        List<String> groupNameList = ExecutorContext.getContext(schemaName).getTopologyHandler().getAllTransGroupList();
        for (String groupName : groupNameList) {
            MyDataSourceGetter myDataSourceGetter = new MyDataSourceGetter(schemaName);
            TGroupDataSource ds = myDataSourceGetter.getDataSource(groupName);
            if (ds == null) {
                logger.error("queryRowCount fail, datasource is null, group name is " + groupName);
                continue;
            }
            String dbName = getDbName(ds);
            if (dbName == null) {
                continue;
            }

            result.add(dbName);
        }
        return result;
    }

    private boolean hasExpire(StatisticManager.CacheLine cacheLine) {
        return (cacheLine.getLastAccessTime() - cacheLine.getLastModifyTime()) > getCollectExpireTimeInMs();
    }

    private boolean disableByUser() {
        return !paramManager.getBoolean(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION);
    }

    private boolean onlyCollectorTablesFromRule() {
        return paramManager.getBoolean(ConnectionParams.STATISTIC_COLLECTOR_FROM_RULE);
    }

    private String getCollectStartTime() {
        return paramManager.getString(ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_START_TIME);
    }

    private String getCollectEndTime() {
        return paramManager.getString(ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_END_TIME);
    }

    private Integer getCollectPeriodInMs() {
        return paramManager.getInt(ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_PERIOD);
    }

    private Integer getCollectExpireTimeInMs() {
        return paramManager.getInt(ConnectionParams.BACKGROUND_STATISTIC_COLLECTION_EXPIRE_TIME);
    }

    private String getDbName(TGroupDataSource dataSource) {
        try {
            return dataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY)
                    .getDsConfHandle().getRunTimeConf().getDbName();
        } catch (Throwable e) {
            logger.error("getDbName fail ", e);
            return null;
        }
    }

    public int getInitialDelay() {
        return INITIAL_DELAY;
    }

    public int getPeriod() {
        return PERIOD;
    }

    public TimeUnit getTimeUnit() {
        return TIME_UNIT;
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

    private void avoidInformationSchemaCache(Connection conn) throws SQLException {
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

    public Set<String> getNeedCollectorTables() {
        return needCollectorTables;
    }

}

