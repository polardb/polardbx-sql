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

package com.alibaba.polardbx.executor.balancer.stats;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Maintain statistics of table-group
 *
 * @author moyi
 * @since 2021/03
 */
public class StatsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StatsUtils.class);

    public static List<TableGroupConfig> getTableGroupConfigs() {
        List<TableGroupConfig> res = new ArrayList<>();

        try (Connection connection = MetaDbUtil.getConnection()) {
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            tableGroupAccessor.setConnection(connection);

            List<String> schemaNames = tableGroupAccessor.getDistinctSchemaNames();
            for (String schemaName : schemaNames) {
                TableGroupInfoManager tableGroupInfoManager =
                    OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
                Map<Long, TableGroupConfig> tableGroupConfigMap =
                    tableGroupInfoManager.getTableGroupConfigInfoCache();

                res.addAll(tableGroupConfigMap.values());
            }
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
        return res;
    }

    /**
     * Get all schema name of partitioning mode
     */
    public static Set<String> getSchemaNames(List<TableGroupConfig> tableGroupConfigs) {
        Set<String> schemaNames = new HashSet<>();
        for (TableGroupConfig tableGroupConfig : tableGroupConfigs) {
            if (tableGroupConfig.getTableCount() == 0) {
                continue;
            }
            String schemaName = tableGroupConfig.getTableGroupRecord().schema;
            schemaNames.add(schemaName);
        }
        return schemaNames;
    }

    /**
     * Query table-groups with table_schema filter
     *
     * @return TableGroupConfig List
     */
    public static List<TableGroupConfig> getTableGroupConfigsWithFilter(List<TableGroupConfig> tableGroupConfigs,
                                                                        Set<String> schemaNamesFilter) {
        List<TableGroupConfig> res = new ArrayList<>();
        if (schemaNamesFilter == null || schemaNamesFilter.isEmpty()) {
            return res;
        }

        for (TableGroupConfig tableGroupConfig : tableGroupConfigs) {
            if (tableGroupConfig.getTableCount() == 0) {
                continue;
            }
            String schemaName = tableGroupConfig.getTableGroupRecord().schema;
            if (schemaNamesFilter.contains(schemaName)) {
                res.add(tableGroupConfig);
            }
        }
        return res;
    }

    /**
     * Query stats of all table-groups
     *
     * @return stats
     */
    public static List<TableGroupStat> getTableGroupsStats(String targetSchema, String targetTable) {
        List<TableGroupConfig> tableGroupConfigs = StatsUtils.getTableGroupConfigs();
        List<TableGroupStat> res = new ArrayList<>();

        // iterate all table-groups
        for (TableGroupConfig tableGroupConfig : tableGroupConfigs) {
            String schema = tableGroupConfig.getTableGroupRecord().schema;
            if (targetSchema != null && !targetSchema.equalsIgnoreCase(schema)) {
                continue;
            }
            PartitionInfoManager pm = OptimizerContext.getContext(schema).getPartitionInfoManager();
            TableGroupStat tableGroupStat = new TableGroupStat(tableGroupConfig);
            Map<String, Map<String, List<Object>>> tablesStatInfo = queryTableGroupStats(tableGroupConfig, null, null);

            // iterate all tables in a table-group
            for (TablePartRecordInfoContext tableContext : tableGroupConfig.getAllTables()) {
                String table = tableContext.getTableName().toLowerCase(Locale.ROOT);
                if (targetTable != null && !targetTable.equalsIgnoreCase(table)) {
                    continue;
                }

                List<TablePartitionRecord> tablePartitionRecords =
                    tableContext
                        .filterPartitions(x -> x.partLevel != TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
                Map<String, List<Object>> tableStatInfo = tablesStatInfo.get(table);

                // iterate all partitions in a table
                for (TablePartitionRecord record : tablePartitionRecords) {
                    PartitionInfo info = pm.getPartitionInfo(table);
                    PartitionStat pgStat = new PartitionStat(tableGroupConfig, record, info);

                    String phyTable = record.phyTable.toLowerCase();
                    pgStat.setDataLength(DataTypes.LongType.convertFrom(tableStatInfo.get(phyTable).get(4)));
                    pgStat.setIndexLength(DataTypes.LongType.convertFrom(tableStatInfo.get(phyTable).get(5)));
                    pgStat.setDataRows(DataTypes.LongType.convertFrom(tableStatInfo.get(phyTable).get(3)));

                    tableGroupStat.addPartition(pgStat);
                }
            }

            res.add(tableGroupStat);
        }

        return res;
    }

    private static RelDataTypeFieldImpl convertRelField(int index, ColumnMeta columnMeta) {
        return new RelDataTypeFieldImpl(columnMeta.getName(), index, columnMeta.getField().getRelType());
    }

    public static RelDataType partitionKeyRowType(List<ColumnMeta> columnsMeta) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        List<RelDataTypeFieldImpl> fields =
            IntStream.range(0, columnsMeta.size())
                .mapToObj(x -> convertRelField(x, columnsMeta.get(x)))
                .collect(Collectors.toList());
        return typeFactory.createStructType(fields);
    }

    /**
     * Query a physical group, and case type
     */
    public static List<SearchDatumInfo> queryGroupTyped(String schema, String physicalDb,
                                                        List<DataType> resultTypes,
                                                        String sql) {

        ExecutorContext ec = ExecutorContext.getContext(schema);
        String groupName = GroupInfoUtil.buildGroupNameFromPhysicalDb(physicalDb);
        IGroupExecutor ge = ec.getTopologyExecutor().getGroupExecutor(groupName);
        List<SearchDatumInfo> result = new ArrayList<>();

        try (Connection conn = ge.getDataSource().getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {

            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<PartitionField> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    DataType objType = resultTypes.get(i - 1);
                    PartitionField field = PartitionFieldBuilder.createField(objType);
                    field.store(rs, i);

                    row.add(field);
                }

                result.add(SearchDatumInfo.createFromFields(row));
            }

            return result;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Query a physical group
     * NOTE: it's not safe to use ResultSet::getObject when type casting
     */
    public static List<List<Object>> queryGroupByGroupName(String schema, String groupName, String sql) {
        final int queryTimeout = 600;
        ExecutorContext ec = ExecutorContext.getContext(schema);
        IGroupExecutor ge = ec.getTopologyExecutor().getGroupExecutor(groupName);
        List<List<Object>> result = new ArrayList<>();

        try (Connection conn = ge.getDataSource().getConnection();
            Statement stmt = conn.createStatement()) {

            stmt.setQueryTimeout(queryTimeout);

            try (ResultSet rs = stmt.executeQuery(sql)) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= columns; i++) {
                        row.add(rs.getObject(i));
                    }
                    result.add(row);
                }
            }

            return result;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                String.format("query group %s with sql %s failed: %s", groupName, sql, e.getMessage()), e);
        }
    }

    /**
     * Query a physical group
     * NOTE: it's not safe to use ResultSet::getObject when type casting
     */
    public static List<List<Object>> queryGroupByPhyDb(String schema, String physicalDb, String sql) {
        String groupName = GroupInfoUtil.buildGroupNameFromPhysicalDb(physicalDb);
        return queryGroupByGroupName(schema, groupName, sql);
    }

    /**
     * Query statistics of a table-group
     */
    public static Map<String, Map<String, List<Object>>> queryTableGroupStats(TableGroupConfig tableGroupConfig,
                                                                              Set<String> indexTableNames,
                                                                              String tableLike) {
        String schema = tableGroupConfig.getTableGroupRecord().schema;
        Map<String, Map<String, List<Object>>> result = new HashMap<>();

        for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
            String physicalDb = partitionGroupRecord.phy_db;
            String sql = generateQueryPartitionGroupStatsSQL(tableGroupConfig, partitionGroupRecord, indexTableNames,
                tableLike);
            // TODO(moyi) it should not happen
            if (TStringUtil.isBlank(sql)) {
                continue;
            }

            List<List<Object>> rows = queryGroupByPhyDb(schema, physicalDb, sql);
            for (List<Object> row : rows) {
                String logicalTable = (String) row.get(1);
                String physicalTable = (String) row.get(2);
                Map<String, List<Object>> table =
                    result.computeIfAbsent(logicalTable.toLowerCase(), x -> new HashMap<>());
                table.put(physicalTable.toLowerCase(), row);
            }

            String statisticSql =
                genQueryPartitionStatisticsSQL(tableGroupConfig, partitionGroupRecord, indexTableNames, tableLike);
            List<List<Object>> statisticRows = queryGroupByPhyDb(schema, physicalDb, statisticSql);
            for (List<Object> row : statisticRows) {
                String logicalTable = (String) row.get(0);
                String physicalTable = (String) row.get(1);

                // append to existed row
                Map<String, List<Object>> table =
                    result.computeIfAbsent(logicalTable.toLowerCase(), x -> new HashMap<>());
                List<Object> existedRow = table.get(physicalTable.toLowerCase());
                for (int i = 2; i < row.size(); i++) {
                    existedRow.add(row.get(i));
                }
            }

        }
        return result;
    }

    /**
     * Query phyDbNames of a logical db
     */
    public static void queryPhyDbNames(Set<String> schemaNames, Map<String, Set<String>> dbPhyDbNames,
                                       Map<String, Set<String>> dbAllPhyDbNames) {
        Map<String, Set<String>> dbStorageInstIds = new HashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> completedGroupInfos =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());
            Collections.sort(completedGroupInfos);

            for (int i = 0; i < completedGroupInfos.size(); i++) {
                GroupDetailInfoExRecord groupDetailInfoExRecord = completedGroupInfos.get(i);
                String instId = groupDetailInfoExRecord.storageInstId;
                String dbName = groupDetailInfoExRecord.dbName;
                String phyDbName = groupDetailInfoExRecord.phyDbName;
                if (schemaNames.contains(dbName)) {
                    Set<String> storageInstIds =
                        dbStorageInstIds.computeIfAbsent(dbName.toLowerCase(), x -> new HashSet<>());
                    // add group Name whose storageInstId is first visited
                    if (!storageInstIds.contains(instId)) {
                        storageInstIds.add(instId);
                        Set<String> phyDbNames =
                            dbPhyDbNames.computeIfAbsent(dbName.toLowerCase(), x -> new HashSet<>());
                        phyDbNames.add(phyDbName);
                    }
                    Set<String> phyDbNames =
                        dbAllPhyDbNames.computeIfAbsent(dbName.toLowerCase(), x -> new HashSet<>());
                    phyDbNames.add(phyDbName);
                }
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException("Failed to get storage and phy db info", ex);
        }
    }

    /**
     * Query statistics of all filtered schema
     */
    public static Map<String, Map<String, List<Object>>> queryTableSchemaStats(Set<String> schemaNames,
                                                                               Set<String> indexTableNames,
                                                                               String tableLike) {
        Map<String, Map<String, List<Object>>> phyDbTablesInfo = new HashMap<>();
        if (schemaNames == null || schemaNames.isEmpty()) {
            return phyDbTablesInfo;
        }

        Map<String, Set<String>> dbPhyDbNames = new HashMap<>();
        Map<String, Set<String>> dbAllPhyDbNames = new HashMap<>();
        queryPhyDbNames(schemaNames, dbPhyDbNames, dbAllPhyDbNames);

        // build sql and exec
        for (String schemaName : schemaNames) {
            // get phy tables info of each logical db (character may be different)
            Set<String> phyDbNames = dbPhyDbNames.get(schemaName);
            Set<String> allPhyDbNames = dbAllPhyDbNames.get(schemaName);
            if (phyDbNames == null || phyDbNames.isEmpty()) {
                continue;
            }
            String sql = generateQueryPhyTablesStatsSQL(allPhyDbNames, indexTableNames, tableLike);

            // get phy tables info from each data node
            List<List<Object>> rows = new ArrayList<>();
            for (String phyDbName : phyDbNames) {
                rows.addAll(queryGroupByPhyDb(schemaName, phyDbName, sql));
            }

            // add phyDbTablesInfo
            for (List<Object> row : rows) {
                String phyTbName = (String) row.get(2);
                String phyDbName = (String) row.get(3);

                Map<String, List<Object>> phyDb =
                    phyDbTablesInfo.computeIfAbsent(phyDbName.toLowerCase(), x -> new HashMap<>());
                phyDb.put(phyTbName.toLowerCase(), row);
            }

            // get phy tables statistic
            String statisticSql = generateQueryPhyTablesStatisticsSQL(allPhyDbNames, indexTableNames, tableLike);
            List<List<Object>> statisticRows = new ArrayList<>();
            for (String phyDbName : phyDbNames) {
                statisticRows.addAll(queryGroupByPhyDb(schemaName, phyDbName, statisticSql));
            }

            for (List<Object> row : statisticRows) {
                String phyTbName = (String) row.get(0);
                String phyDbName = (String) row.get(1);

                // append to existed row
                Map<String, List<Object>> phyDb =
                    phyDbTablesInfo.computeIfAbsent(phyDbName.toLowerCase(), x -> new HashMap<>());
                List<Object> existedRow = phyDb.get(phyTbName.toLowerCase());
                if (existedRow != null) {
                    for (int i = 2; i < row.size(); i++) {
                        existedRow.add(row.get(i));
                    }
                }
            }
        }

        return phyDbTablesInfo;
    }

    /**
     * Query statistics of a table-group
     */
    public static Map<String, Map<String, List<Object>>> queryTableGroupStats(TableGroupConfig tableGroupConfig,
                                                                              Set<String> indexTableNames,
                                                                              String tableLike,
                                                                              Map<String, Map<String, List<Object>>> phyDbTablesInfo) {
        Map<String, Map<String, List<Object>>> result = new HashMap<>();

        for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
            for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
                // table name filter
                String logicalTableName = tablePartRecordInfoContext.getTableName().toLowerCase();
                if (!isFilterTable(indexTableNames, tableLike, logicalTableName)) {
                    continue;
                }

                long partitionGroupId = partitionGroupRecord.id;
                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartRecordInfoContext.getPartitionRecListByGroupId(partitionGroupId);
                assert GeneralUtil.isNotEmpty(tablePartitionRecords);
                for (TablePartitionRecord tablePartitionRecord : tablePartitionRecords) {
                    String phyDbName = partitionGroupRecord.phy_db.toLowerCase();
                    String phyTbName = tablePartitionRecord.phyTable.toLowerCase();
                    List<Object> row;
                    try {
                        row = phyDbTablesInfo.get(phyDbName).get(phyTbName);
                    } catch (Exception ex) {
                        throw GeneralUtil.nestedException("Failed to get physical table info ", ex);
                    }

                    StringBuilder partDesc = new StringBuilder();
                    if (tablePartitionRecord.partPosition > 1) {
                        TablePartitionRecord lastPart =
                            tablePartRecordInfoContext
                                .getPartitionByPosition((int) (tablePartitionRecord.partPosition - 1));
                        partDesc.append("[");
                        if (lastPart != null) {
                            partDesc.append(lastPart.partDesc.replaceAll("'", ""));
                            partDesc.append(", ");
                        }
                    } else {
                        partDesc.append("[MINVALUE, ");
                    }
                    partDesc.append(tablePartitionRecord.partDesc.replaceAll("'", ""));
                    partDesc.append(")");
                    row.set(0, partDesc.toString());
                    row.set(1, logicalTableName);
                    row.remove(3);

                    Map<String, List<Object>> table =
                        result.computeIfAbsent(logicalTableName.toLowerCase(), x -> new HashMap<>());
                    table.put(phyTbName.toLowerCase(), row);
                }
            }
        }

        return result;
    }

    public static boolean isFilterTable(Set<String> indexTableNames, String tableLike, String logicalTableName) {
        Like likeFunc = new Like(null, null);
        if (indexTableNames != null && !indexTableNames.isEmpty()) {
            if (!indexTableNames.contains(logicalTableName)) {
                return false;
            }
        }
        if (tableLike != null) {
            return likeFunc.like(logicalTableName, tableLike);
        }
        return true;
    }

    /**
     * Build a SQL to query information_schema.table_statistics
     * <p>
     * SELECT LOGICAL_TABLENAME, PHYSICAL_TABLEANME, ROWS_READ, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED
     * FROM xxx
     */
    private static String genQueryPartitionStatisticsSQL(TableGroupConfig tableGroupConfig,
                                                         PartitionGroupRecord partitionGroupRecord,
                                                         Set<String> indexTableNames,
                                                         String tableLike) {
        StringBuilder sb = new StringBuilder();
        int tableIndex = 0;

        for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
            if (tablePartRecordInfoContext == null) {
                continue;
            }

            String logicalTableName = tablePartRecordInfoContext.getTableName().toLowerCase();
            if (!isFilterTable(indexTableNames, tableLike, logicalTableName)) {
                continue;
            }

            long partitionGroupId = partitionGroupRecord.id;
            List<TablePartitionRecord> tablePartitionRecords =
                tablePartRecordInfoContext.getPartitionRecListByGroupId(partitionGroupId);
            assert GeneralUtil.isNotEmpty(tablePartitionRecords);

            for (TablePartitionRecord tablePartitionRecord : tablePartitionRecords) {
                // select
                sb.append(tableIndex == 0 ? "SELECT " : " UNION ALL SELECT ");
                sb.append(TStringUtil.quoteString(tablePartitionRecord.tableName)).append(" LOGICAL_TABLE_NAME, ");
                sb.append(" TABLE_NAME as PHYSICAL_TABLE, ");
                sb.append(" ROWS_READ, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED ");

                // from
                sb.append(" FROM ");
                sb.append(" information_schema.table_statistics ");

                // where
                sb.append(" WHERE ");
                sb.append(String.format(" table_schema = '%s' AND table_name = '%s' ",
                    partitionGroupRecord.phy_db, tablePartitionRecord.phyTable));

                tableIndex++;
            }
        }

        return sb.toString();
    }

    public static Map<String, Long> queryDbGroupDataSize(String schema, List<GroupDetailInfoExRecord> groupRecords) {
        Map<String, Long> groupDataSizeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        groupRecords.stream().collect(Collectors.groupingBy(x -> x.storageInstId))
            .forEach((storageInstId, groups) -> {
                Map<String, String> phyDbToGroup = groups.stream()
                    .map(x -> Pair.of(x.getPhyDbName().toLowerCase(), x.groupName))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                List<String> phyDbList = groups.stream().map(x -> x.getPhyDbName()).collect(Collectors.toList());
                String anchorPhyDb = phyDbList.get(0);
                String sql = genDbGroupSQL(phyDbList);
                // PhyDbName, DataSizeKB
                List<List<Object>> rows = queryGroupByPhyDb(schema, anchorPhyDb, sql);
                for (List<Object> row : rows) {
                    String phyDbName = (String) row.get(0);
                    long dataSizeKB = ((BigDecimal) row.get(1)).longValue();
                    String groupName = phyDbToGroup.get(phyDbName);

                    groupDataSizeMap.merge(groupName, dataSizeKB * 1024, Long::sum);
                }
            });
        return groupDataSizeMap;
    }

    private static String genDbGroupSQL(List<String> phyDbList) {
        String phyDbStr = phyDbList.stream().map(TStringUtil::quoteString).collect(Collectors.joining(","));
        String sql = String.format("SELECT table_schema \"PhyDbName\",  " +
            "sum( data_length + index_length ) / 1024 \"DataSizeKB\" " +
            "FROM information_schema.TABLES " +
            "WHERE table_schema in (%s) " +
            "GROUP BY table_schema ", phyDbStr);
        return sql;
    }

    /**
     * Build a SQL to collect stats of mysql table
     */
    private static String generateQueryPartitionGroupStatsSQL(TableGroupConfig tableGroupConfig,
                                                              PartitionGroupRecord partitionGroupRecord,
                                                              Set<String> indexTableNames,
                                                              String tableLike) {
        int tableCount = 0;
        StringBuilder sb = new StringBuilder();

        for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
            if (tablePartRecordInfoContext == null) {
                continue;
            }

            String logicalTableName = tablePartRecordInfoContext.getTableName().toLowerCase();
            if (!isFilterTable(indexTableNames, tableLike, logicalTableName)) {
                continue;
            }

            long partitionGroupId = partitionGroupRecord.id;
            List<TablePartitionRecord> tablePartitionRecords =
                tablePartRecordInfoContext.getPartitionRecListByGroupId(partitionGroupId);
            assert GeneralUtil.isNotEmpty(tablePartitionRecords);

            for (TablePartitionRecord tablePartitionRecord : tablePartitionRecords) {
                if (tableCount > 0) {
                    sb.append(" union all select ");
                } else {
                    sb.append(" select ");
                }
                if (tablePartitionRecord.partPosition > 1) {
                    TablePartitionRecord lastPart =
                        tablePartRecordInfoContext
                            .getPartitionByPosition((int) (tablePartitionRecord.partPosition - 1));
                    sb.append("'[");
                    if (lastPart != null) {
                        sb.append(lastPart.partDesc.replaceAll("'", ""));
                        sb.append(", ");
                    }
                } else {
                    sb.append("'[MINVALUE, ");
                }

                sb.append(tablePartitionRecord.partDesc.replaceAll("'", ""));
                sb.append(")' as PART_DESC, ");
                sb.append("'");
                sb.append(tablePartitionRecord.tableName);
                sb.append("'");
                sb.append(" LOGICAL_TABLE_NAME,");
                sb.append(
                    " TABLE_NAME as PHYSICAL_TABLE" +
                        ", IFNULL(TABLE_ROWS,0) as TABLE_ROWS" +
                        ", IFNULL(DATA_LENGTH,0) as DATA_LENGTH" +
                        ", IFNULL(INDEX_LENGTH,0) as INDEX_LENGTH  " +
                        "FROM information_schema.TABLES where ");
                sb.append(String.format("table_schema = '%s'", partitionGroupRecord.phy_db));
                sb.append(String.format(" and table_name = '%s'", tablePartitionRecord.phyTable));
                tableCount++;
            }
        }
        return sb.toString();
    }

    /**
     * Build a SQL to collect stats of mysql table
     */
    private static String generateQueryPhyTablesStatsSQL(Set<String> schemaNames, Set<String> indexTableNames,
                                                         String tableLike) {
        StringBuilder sb = new StringBuilder();
        int schemaIndex = 0;
        for (String schemaName : schemaNames) {
            if (schemaIndex != 0) {
                sb.append(" union all ");
            }
            sb.append(
                "select null as PART_DESC, null as LOGICAL_TABLE_NAME, " +
                    " TABLE_NAME as PHYSICAL_TABLE" +
                    ", TABLE_SCHEMA as PHYSICAL_SCHEMA" +
                    ", IFNULL(TABLE_ROWS,0) as TABLE_ROWS" +
                    ", IFNULL(DATA_LENGTH,0) as DATA_LENGTH" +
                    ", IFNULL(INDEX_LENGTH,0) as INDEX_LENGTH  " +
                    " FROM information_schema.TABLES " +
                    " where table_schema = ");
            // handle schemaName filter
            sb.append("'");
            sb.append(schemaName);
            sb.append("'");

            // handle tableName filter
            if (indexTableNames != null && !indexTableNames.isEmpty()) {
                sb.append(" and (");
                schemaIndex = 0;
                for (String tableName : indexTableNames) {
                    String filter = "table_name like '" + tableName + "%'";
                    if (schemaIndex != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    schemaIndex++;
                }
                sb.append(")");
            }

            if (tableLike != null) {
                String filter = "and table_name like '" + tableLike + "%'";
                sb.append(filter);
            }
            schemaIndex++;
        }
        return sb.toString();
    }

    /**
     * Build a SQL to query information_schema.table_statistics
     */
    private static String generateQueryPhyTablesStatisticsSQL(Set<String> schemaNames, Set<String> indexTableNames,
                                                              String tableLike) {
        StringBuilder sb = new StringBuilder();
        int schemaIndex = 0;
        for (String schemaName : schemaNames) {
            // select
            sb.append(schemaIndex == 0 ? "SELECT " : " UNION ALL SELECT ");
            sb.append(" TABLE_NAME as PHYSICAL_TABLE, ");
            sb.append(" TABLE_SCHEMA as PHYSICAL_SCHEMA, ");
            sb.append(" ROWS_READ, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED ");

            // from
            sb.append(" FROM ");
            sb.append(" information_schema.table_statistics ");
            sb.append(" where table_schema = ");
            // handle schemaName filter
            sb.append("'");
            sb.append(schemaName);
            sb.append("'");

            // handle tableName filter
            if (indexTableNames != null && !indexTableNames.isEmpty()) {
                sb.append(" and (");
                schemaIndex = 0;
                for (String tableName : indexTableNames) {
                    String filter = "table_name like '" + tableName + "%'";
                    if (schemaIndex != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    schemaIndex++;
                }
                sb.append(")");
            }

            if (tableLike != null) {
                String filter = "and table_name like '" + tableLike + "%'";
                sb.append(filter);
            }
            schemaIndex++;
        }
        return sb.toString();
    }
}

