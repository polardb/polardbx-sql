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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import org.apache.commons.lang.StringUtils;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author shengyu
 */
public class InformationSchemaTablesHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaTablesHandler.class);
    private static final String QUERY_INFO_SCHEMA_TABLES = "select * from information_schema.tables where "
        + " (table_schema, table_name) in (%s)";
    private static final String DISABLE_INFO_SCHEMA_CACHE_80 = "set information_schema_stats_expiry = 0";

    public InformationSchemaTablesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTables;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        if (ConfigDataMode.isFastMock()) {
            return cursor;
        }
        InformationSchemaTables informationSchemaTables = (InformationSchemaTables) virtualView;

        final int schemaIndex = InformationSchemaTables.getTableSchemaIndex();
        final int tableIndex = InformationSchemaTables.getTableNameIndex();
        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        Set<String> schemaNames =
            virtualView.applyFilters(schemaIndex, params, OptimizerContext.getActiveSchemaNames());

        // tableIndex
        Set<String> indexTableNames = virtualView.getEqualsFilterValues(tableIndex, params);
        // tableLike
        String tableLike = virtualView.getLikeString(tableIndex, params);

        // Fetch accurate info from each DN.
        if (DynamicConfig.getInstance().isEnableAccurateInfoSchemaTables()) {
            fetchAccurateInfoSchemaTables(schemaNames, indexTableNames, tableLike, cursor);
            return cursor;
        }

        boolean once = true;
        for (String schemaName : schemaNames) {
            SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();

            // groupName -> {(logicalTableName, physicalTableName)}
            Map<String, Set<Pair<String, String>>> groupToPair =
                virtualViewHandler.getGroupToPair(schemaName, indexTableNames, tableLike,
                    executionContext.isTestMode());

            for (String groupName : groupToPair.keySet()) {

                TGroupDataSource groupDataSource =
                    (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                        .getGroupExecutor(groupName).getDataSource();

                String actualDbName = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY)
                    .getDsConfHandle().getRunTimeConf().getDbName();

                Set<Pair<String, String>> collection = groupToPair.get(groupName);

                if (collection.isEmpty()) {
                    continue;
                }

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("select * from information_schema.tables where table_schema = ");
                stringBuilder.append("'");
                stringBuilder.append(actualDbName.replace("'", "\\'"));
                stringBuilder.append("' and table_name in (");

                boolean first = true;

                // physicalTableName -> logicalTableName
                Map<String, String> physicalTableToLogicalTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (Pair<String, String> pair : collection) {
                    String logicalTableName = pair.getKey();
                    String physicalTableName = pair.getValue();
                    physicalTableToLogicalTable.put(physicalTableName, logicalTableName);
                    if (!first) {
                        stringBuilder.append(", ");
                    }
                    first = false;
                    stringBuilder.append("'");
                    stringBuilder.append(physicalTableName.replace("'", "\\'"));
                    stringBuilder.append("'");
                }
                stringBuilder.append(")");

                // FIXME: NEED UNION INFORMATION TABLES?
                if (once) {
                    stringBuilder.append(
                        " union select * from information_schema.tables where table_schema = 'information_schema'");
                    once = false;
                }

                Connection conn = null;
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    conn = groupDataSource.getConnection();
                    stmt = conn.createStatement();
                    rs = stmt.executeQuery(stringBuilder.toString());
                    while (rs.next()) {

                        String logicalTableName;
                        String tableSchema;
                        long tableRows;
                        String autoPartition = "NO";
                        if (rs.getString("TABLE_SCHEMA").equalsIgnoreCase("information_schema")) {
                            tableSchema = rs.getString("TABLE_SCHEMA");
                            logicalTableName = rs.getString("TABLE_NAME");
                            tableRows = rs.getLong("TABLE_ROWS");
                        } else {
                            logicalTableName =
                                physicalTableToLogicalTable.get(rs.getString("TABLE_NAME"));
                            tableSchema = schemaName;
                            StatisticResult statisticResult =
                                StatisticManager.getInstance().getRowCount(schemaName, logicalTableName, false);
                            tableRows = statisticResult.getLongValue();
                            if (!CanAccessTable.verifyPrivileges(schemaName, logicalTableName, executionContext)) {
                                continue;
                            }

                            // do not skip GSI when getting statistics for GSI
                            if (!informationSchemaTables.includeGsi()) {
                                try {
                                    TableMeta tableMeta = schemaManager.getTable(logicalTableName);
                                    if (tableMeta.isAutoPartition()) {
                                        autoPartition = "YES";
                                    }
                                    if (tableMeta.isGsi()) {
                                        continue;
                                    }
                                } catch (Throwable t) {
                                    // ignore table not exists
                                }
                            }
                        }

                        long single_data_length = rs.getLong("DATA_LENGTH");
                        double scale = 1;
                        if (single_data_length != 0 && rs.getLong("AVG_ROW_LENGTH") > 0) {
                            scale = ((double) (tableRows * rs.getLong("AVG_ROW_LENGTH"))) / single_data_length;
                        }
                        long dataLength = (long) (single_data_length * scale);
                        long indexLength = (long) (rs.getLong("INDEX_LENGTH") * scale);
                        long dataFree = (long) (rs.getLong("DATA_FREE") * scale);
                        BigInteger avgRowLength = (BigInteger) rs.getObject("AVG_ROW_LENGTH");
                        // build info for file storage table
                        if (!rs.getString("TABLE_SCHEMA").equalsIgnoreCase("information_schema")) {
                            if (StatisticUtils.isFileStore(schemaName, logicalTableName)) {
                                Map<String, Long> statisticMap =
                                    StatisticUtils.getFileStoreStatistic(schemaName, logicalTableName);
                                tableRows = statisticMap.get("TABLE_ROWS");
                                dataLength = statisticMap.get("DATA_LENGTH");
                                indexLength = statisticMap.get("INDEX_LENGTH");
                                dataFree = statisticMap.get("DATA_FREE");
                                if (tableRows != 0) {
                                    avgRowLength = BigInteger.valueOf(dataLength / tableRows);
                                }
                            }
                        }

                        cursor.addRow(new Object[] {
                            rs.getObject("TABLE_CATALOG"),
                            StringUtils.lowerCase(tableSchema),
                            StringUtils.lowerCase(logicalTableName),
                            rs.getObject("TABLE_TYPE"),
                            rs.getObject("ENGINE"),
                            rs.getObject("VERSION"),
                            rs.getObject("ROW_FORMAT"),
                            tableRows,
                            avgRowLength,
                            dataLength,
                            rs.getObject("MAX_DATA_LENGTH"),
                            indexLength,
                            dataFree,
                            rs.getObject("AUTO_INCREMENT"),
                            rs.getObject("CREATE_TIME"),
                            rs.getObject("UPDATE_TIME"),
                            rs.getObject("CHECK_TIME"),
                            rs.getObject("TABLE_COLLATION"),
                            rs.getObject("CHECKSUM"),
                            rs.getObject("CREATE_OPTIONS"),
                            rs.getObject("TABLE_COMMENT"),
                            autoPartition
                        });
                    }
                } catch (Throwable t) {
                    logger.error(t);
                } finally {
                    GeneralUtil.close(rs);
                    GeneralUtil.close(stmt);
                    GeneralUtil.close(conn);
                }
            }
        }

        return cursor;
    }

    protected static void fetchAccurateInfoSchemaTables(Set<String> schemaNames, Set<String> indexTableNames,
                                                        String tableLike, ArrayResultCursor cursor) {
        for (String schemaName : schemaNames) {
            // logical table -> table statistics
            Map<String, Statistics> tableStatistics = new HashMap<>();
            // gsi table name -> logical table name, all in lower case
            Map<String, String> gsiToLogicalTb = new HashMap<>();
            // dn host:port -> information of physical tables in this dn
            Map<String, DnInfo> dnInfos = generateMetadataForSchema(schemaName, indexTableNames, tableLike,
                gsiToLogicalTb);

            for (DnInfo dnInfo : dnInfos.values()) {
                try (Connection connection = dnInfo.getConnection();
                    Statement stmt = connection.createStatement()) {
                    if (InstanceVersion.isMYSQL80()) {
                        try {
                            stmt.execute(DISABLE_INFO_SCHEMA_CACHE_80);
                        } catch (SQLException e) {
                            // ignore
                        }
                    }
                    String dbAndTb = dnInfo.phyDbPhyTbToLogicalTb.keySet()
                        .stream()
                        .map(o -> "('" + o.getKey() + "', '" + o.getValue() + "')")
                        .collect(Collectors.joining(","));
                    String sql = String.format(QUERY_INFO_SCHEMA_TABLES, dbAndTb);
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        String physicalDb = rs.getString("TABLE_SCHEMA");
                        String physicalTb = rs.getString("TABLE_NAME");
                        // find logical table
                        String logicalTable = dnInfo.phyDbPhyTbToLogicalTb.get(new Pair<>(physicalDb, physicalTb));
                        if (null == logicalTable) {
                            logger.warn("Cant find logical table for " + physicalDb + "." + physicalTb + " in "
                                + dnInfo.dnAddress);
                            continue;
                        }

                        long version = rs.getLong("VERSION");
                        String rowFormat = rs.getString("ROW_FORMAT");
                        long tableRows = rs.getLong("TABLE_ROWS");
                        long avgRowLength = rs.getLong("AVG_ROW_LENGTH");
                        long dataLength = rs.getLong("DATA_LENGTH");
                        long maxDataLength = rs.getLong("MAX_DATA_LENGTH");
                        long indexLength = rs.getLong("INDEX_LENGTH");
                        long dataFree = rs.getLong("DATA_FREE");
                        String autoIncrement = rs.getString("AUTO_INCREMENT");
                        String createTime = rs.getString("CREATE_TIME");
                        String updateTime = rs.getString("UPDATE_TIME");
                        String checkTime = rs.getString("CHECK_TIME");
                        String tableCollation = rs.getString("TABLE_COLLATION");
                        String checksum = rs.getString("CHECKSUM");
                        String createOptions = rs.getString("CREATE_OPTIONS");
                        String tableComment = rs.getString("TABLE_COMMENT");

                        Statistics stats = tableStatistics.computeIfAbsent(logicalTable, k -> {
                            Statistics statistics = new Statistics();
                            statistics.version = version;
                            statistics.rowFormat = rowFormat;
                            statistics.autoIncrement = autoIncrement;
                            statistics.createTime = createTime;
                            statistics.updateTime = updateTime;
                            statistics.checkTime = checkTime;
                            statistics.tableCollation = tableCollation;
                            statistics.checksum = checksum;
                            statistics.createOptions = createOptions;
                            statistics.tableComment = tableComment;
                            return statistics;
                        });
                        stats.tableRows += tableRows;
                        stats.avgRowLength += avgRowLength;
                        stats.dataLength += dataLength;
                        stats.maxDataLength += maxDataLength;
                        stats.indexLength += indexLength;
                        stats.dataFree += dataFree;
                        stats.cnt++;
                    }
                } catch (SQLException ex) {
                    logger.error("error when querying information_schema.tables", ex);
                }
            }

            // Add index_length of gsi into index_length of primary table
            for (Map.Entry<String, String> gsiAndLogicalTb : gsiToLogicalTb.entrySet()) {
                Statistics gsiStat = tableStatistics.get(gsiAndLogicalTb.getKey());
                Statistics primaryStat = tableStatistics.get(gsiAndLogicalTb.getValue());
                if (null != gsiStat && null != primaryStat) {
                    primaryStat.indexLength += gsiStat.indexLength;
                    gsiStat.indexLength = 0;
                }
            }

            for (Map.Entry<String, Statistics> statistics : tableStatistics.entrySet()) {
                if (DynamicConfig.getInstance().isEnableTrxDebugMode()) {
                    logger.warn("accurate info schema tables: " + statistics.getKey());
                }
                String logicalTableName = statistics.getKey();
                if (StatisticUtils.isFileStore(schemaName, logicalTableName)) {
                    Map<String, Long> statisticMap =
                        StatisticUtils.getFileStoreStatistic(schemaName, logicalTableName);
                    statistics.getValue().tableRows = statisticMap.get("TABLE_ROWS");
                    statistics.getValue().dataLength = statisticMap.get("DATA_LENGTH");
                    statistics.getValue().indexLength = statisticMap.get("INDEX_LENGTH");
                    statistics.getValue().dataFree = statisticMap.get("DATA_FREE");
                    if (statistics.getValue().tableRows != 0) {
                        statistics.getValue().avgRowLength
                            = statistics.getValue().dataLength / statistics.getValue().tableRows;
                    }
                }
                cursor.addRow(new Object[] {
                    "def",
                    schemaName,
                    statistics.getKey(),
                    "BASE TABLE",
                    "InnoDB",
                    statistics.getValue().version,
                    statistics.getValue().rowFormat,
                    statistics.getValue().tableRows,
                    statistics.getValue().avgRowLength / statistics.getValue().cnt,
                    statistics.getValue().dataLength,
                    statistics.getValue().maxDataLength,
                    statistics.getValue().indexLength,
                    statistics.getValue().dataFree,
                    statistics.getValue().autoIncrement,
                    statistics.getValue().createTime,
                    statistics.getValue().updateTime,
                    statistics.getValue().checkTime,
                    statistics.getValue().tableCollation,
                    statistics.getValue().checksum,
                    statistics.getValue().createOptions,
                    statistics.getValue().tableComment
                });
            }
        }
    }

    public static Map<String, DnInfo> generateMetadataForSchema(String schemaName, Set<String> indexTableNames,
                                                                String tableLike, Map<String, String> gsiToLogicalTb) {
        Map<String, DnInfo> dnInfos = new HashMap<>();
        List<String> logicalTableNameSet = new ArrayList<>();
        ITopologyExecutor executor = ExecutorContext.getContext(schemaName).getTopologyExecutor();

        // Drds mode.
        TddlRuleManager tddlRuleManager =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        TddlRule tddlRule = tddlRuleManager.getTddlRule();
        Collection<TableRule> tableRules = tddlRule.getTables();
        for (TableRule tableRule : tableRules) {
            String logicalTableName = tableRule.getVirtualTbName();
            if (SystemTables.contains(logicalTableName)) {
                continue;
            }
            logicalTableNameSet.add(logicalTableName);
        }

        // Auto mode.
        PartitionInfoManager partitionInfoManager =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getPartitionInfoManager();
        for (PartitionInfo partitionInfo : partitionInfoManager.getPartitionInfos()) {
            String logicalTableName = partitionInfo.getTableName();
            if (SystemTables.contains(logicalTableName)) {
                continue;
            }
            logicalTableNameSet.add(logicalTableName);
        }

        Like likeFunc = new Like();

        for (String logicalTableName : logicalTableNameSet) {
            if (indexTableNames != null && !indexTableNames.isEmpty()) {
                if (!indexTableNames.contains(logicalTableName.toLowerCase())) {
                    continue;
                }
            }

            if (tableLike != null) {
                if (!likeFunc.like(logicalTableName, tableLike)) {
                    continue;
                }
            }

            // Process this logical table
            if (partitionInfoManager.isNewPartDbTable(logicalTableName)) {
                processAutoTable(dnInfos, executor, partitionInfoManager, logicalTableName);

                // For gsi
                TableMeta tableMeta = Objects.requireNonNull(OptimizerContext.getContext(schemaName))
                    .getLatestSchemaManager().getTable(logicalTableName);
                Map<String, GsiMetaManager.GsiIndexMetaBean> publishedGsi = tableMeta.getGsiPublished();
                if (null != publishedGsi) {
                    List<String> gsiList = publishedGsi.keySet()
                        .stream().map(String::toLowerCase).collect(Collectors.toList());
                    for (String gsi : gsiList) {
                        processAutoTable(dnInfos, executor, partitionInfoManager, gsi);
                        gsiToLogicalTb.put(gsi.toLowerCase(), logicalTableName.toLowerCase());
                    }
                }
            } else {
                processDrdsTable(dnInfos, executor, tddlRuleManager, logicalTableName);

                // For gsi
                TableMeta tableMeta = Objects.requireNonNull(OptimizerContext.getContext(schemaName))
                    .getLatestSchemaManager().getTable(logicalTableName);
                Map<String, GsiMetaManager.GsiIndexMetaBean> publishedGsi = tableMeta.getGsiPublished();
                if (null != publishedGsi) {
                    List<String> gsiList = publishedGsi.keySet()
                        .stream().map(String::toLowerCase).collect(Collectors.toList());
                    for (String gsi : gsiList) {
                        processDrdsTable(dnInfos, executor, tddlRuleManager, gsi);
                        gsiToLogicalTb.put(gsi.toLowerCase(), logicalTableName.toLowerCase());
                    }
                }
            }
        }

        return dnInfos;
    }

    private static void processDrdsTable(Map<String, DnInfo> dnInfos, ITopologyExecutor executor,
                                         TddlRuleManager tddlRuleManager, String tableName) {
        TableRule tableRule = tddlRuleManager.getTableRule(tableName);
        if (null != tableRule) {
            Map<String, Set<String>> topologys = tableRule.getStaticTopology();
            if (topologys == null || topologys.size() == 0) {
                topologys = tableRule.getActualTopology();
            }

            for (Map.Entry<String, Set<String>> topology : topologys.entrySet()) {
                String group = topology.getKey();
                Set<String> tableNames = topology.getValue();
                if (tableNames == null || tableNames.isEmpty()) {
                    continue;
                }
                TGroupDataSource dataSource = (TGroupDataSource) executor.getGroupExecutor(group).getDataSource();
                String address = dataSource.getMasterSourceAddress().toLowerCase();
                String phyDb = dataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY)
                    .getDsConfHandle().getRunTimeConf().getDbName();
                for (String physicalTable : tableNames) {
                    DnInfo dnInfo = dnInfos.computeIfAbsent(address, k -> new DnInfo(k, dataSource));
                    dnInfo.phyDbPhyTbToLogicalTb.put(new Pair<>(phyDb.toLowerCase(), physicalTable.toLowerCase()),
                        tableName.toLowerCase());
                }
            }
        }
    }

    private static void processAutoTable(Map<String, DnInfo> dnInfos, ITopologyExecutor executor,
                                         PartitionInfoManager partitionInfoManager, String tableName) {
        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableName);
        List<PartitionSpec> partitions = partitionInfo.getPartitionBy().getPhysicalPartitions();
        for (PartitionSpec partition : partitions) {
            PartitionLocation location = partition.getLocation();
            String groupName = location.getGroupKey();
            String phyDb = GroupInfoUtil.buildPhysicalDbNameFromGroupName(groupName).toLowerCase();
            String phyTb = location.getPhyTableName().toLowerCase();
            TGroupDataSource dataSource = (TGroupDataSource) executor.getGroupExecutor(groupName).getDataSource();
            String address = dataSource.getMasterSourceAddress().toLowerCase();
            DnInfo dnInfo = dnInfos.computeIfAbsent(address, k -> new DnInfo(k, dataSource));
            dnInfo.phyDbPhyTbToLogicalTb.put(new Pair<>(phyDb, phyTb), tableName.toLowerCase());
        }
    }

    public static class DnInfo {
        // DN host:port
        public String dnAddress;
        // (physical_db, physical_tb) -> logical_tb, all in lower case
        public Map<Pair<String, String>, String> phyDbPhyTbToLogicalTb = new HashMap<>();
        public TGroupDataSource dataSource;

        public DnInfo(String address, TGroupDataSource dataSource) {
            this.dnAddress = address;
            this.dataSource = dataSource;
        }

        public Connection getConnection() {
            String masterDnId = dataSource.getMasterDNId();
            return DbTopologyManager.getConnectionForStorage(masterDnId);
        }
    }

    private static class Statistics {
        public long version;
        public String rowFormat;
        public long tableRows = 0;
        public long avgRowLength = 0;
        public long dataLength = 0;
        public long maxDataLength = 0;
        public long indexLength = 0;
        public long dataFree = 0;
        public String autoIncrement;
        public String createTime;
        public String updateTime;
        public String checkTime;
        public String tableCollation;
        public String checksum;
        public String createOptions;
        public String tableComment;
        public long cnt = 0;
    }
}
