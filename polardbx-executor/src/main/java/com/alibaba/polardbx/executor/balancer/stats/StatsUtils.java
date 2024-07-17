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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
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
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Data;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.commons.collections.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.gms.tablegroup.TableGroupRecord.TG_TYPE_COLUMNAR_TBL_TG;

/**
 * Maintain statistics of table-group
 *
 * @author moyi
 * @since 2021/03
 */
public class StatsUtils {

    private static final Logger logger = LoggerFactory.getLogger(StatsUtils.class);

    public static List<TableGroupConfig> getTableGroupConfigs(Set<String> schemaNames) {
        List<TableGroupConfig> res = new ArrayList<>();

        try (Connection connection = MetaDbUtil.getConnection()) {
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            tableGroupAccessor.setConnection(connection);

            for (String schemaName : schemaNames) {
                res.addAll(TableGroupUtils.getAllTableGroupInfoByDb(schemaName));
            }
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
        return res;
    }

    public static List<String> getDistinctSchemaNames() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            tableGroupAccessor.setConnection(connection);

            return tableGroupAccessor.getDistinctSchemaNames();

        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
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

    public static List<TableGroupStat> getTableGroupsStats(String targetSchema, String targetTableGroup, Boolean idle) {
        List<TableGroupDetailConfig> tableGroupConfigs = TableGroupUtils.getAllTableGroupDetailInfoByDb(targetSchema);
        tableGroupConfigs = tableGroupConfigs.stream()
            .filter(tgConfig -> tgConfig.getTableGroupRecord().getTg_name().equals(targetTableGroup))
            .collect(Collectors.toList());
        List<TableGroupStat> res = new ArrayList<>();
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(targetSchema), targetSchema + " not exists");
        PartitionInfoManager pm = oc.getPartitionInfoManager();

        // execute physical sdl
        long startMilli = System.currentTimeMillis();
        Map<String, Map<String, MySQLTablesRowVO>> tablesStatInfo =
            queryTableGroupStats(targetSchema, tableGroupConfigs);
        long elapsed = System.currentTimeMillis() - startMilli;
        SQLRecorderLogger.ddlLogger.info(
            String.format("got table-group stats for schema(%s) cost %dms: %s", targetSchema, elapsed, tablesStatInfo));

        // iterate all table-groups
        for (TableGroupDetailConfig tableGroupConfig : tableGroupConfigs) {
            String schema = tableGroupConfig.getTableGroupRecord().schema;
            if (targetSchema != null && !targetSchema.equalsIgnoreCase(schema)) {
                continue;
            }
            if (tableGroupConfig.getTableGroupRecord().tg_type == TG_TYPE_COLUMNAR_TBL_TG) {
                continue;
            }
            TableGroupStat tableGroupStat = new TableGroupStat(tableGroupConfig);

            // iterate all tables in a table-group
            for (TablePartRecordInfoContext tableContext : tableGroupConfig.getTablesPartRecordInfoContext()) {
                String table = tableContext.getTableName().toLowerCase(Locale.ROOT);

                List<TablePartitionRecord> tablePartitionRecords = null;
                if (tableContext.getSubPartitionRecList().isEmpty()) {
                    tablePartitionRecords =
                        tableContext
                            .filterPartitions(x -> x.partLevel != TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
                } else {
                    tablePartitionRecords =
                        tableContext
                            .filterSubPartitions(
                                x -> x.partLevel != TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
                }
                Map<String, MySQLTablesRowVO> tableStatInfo = tablesStatInfo.get(table);

                // TODO: use lock to avoid meta too old exception
                if (tableStatInfo == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, targetSchema, table);
                }

                // iterate all partitions in a table
                for (TablePartitionRecord record : tablePartitionRecords) {
                    PartitionInfo info = pm.getPartitionInfo(table);
                    PartitionStat pgStat = new PartitionStat(tableGroupConfig, record, info);
                    MySQLTablesRowVO phyTableInfo = tableStatInfo.get(record.phyTable.toLowerCase());

                    // TODO: use lock to avoid meta too old exception
                    if (phyTableInfo == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, targetSchema, table);
                    }

                    pgStat.setDataLength(phyTableInfo.dataLength);
                    pgStat.setIndexLength(phyTableInfo.indexLength);
                    pgStat.setDataRows(phyTableInfo.dataRows);

                    tableGroupStat.addPartition(pgStat);
                }
            }

            res.add(tableGroupStat);
        }
        return res;
    }

    /**
     * Query stats of all table-groups in the `targetSchema`
     *
     * @param targetTable query stats of this table if it's not null
     * @return stats
     */
    public static List<TableGroupStat> getTableGroupsStats(String targetSchema, @Nullable String targetTable) {
        List<TableGroupDetailConfig> tableGroupConfigs = TableGroupUtils.getAllTableGroupDetailInfoByDb(targetSchema);
        List<TableGroupStat> res = new ArrayList<>();
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(targetSchema), targetSchema + " not exists");
        PartitionInfoManager pm = oc.getPartitionInfoManager();

        // execute physical sdl
        long startMilli = System.currentTimeMillis();
        Map<String, Map<String, MySQLTablesRowVO>> tablesStatInfo =
            queryTableGroupStats(targetSchema, tableGroupConfigs);
        long elapsed = System.currentTimeMillis() - startMilli;
        SQLRecorderLogger.ddlLogger.info(
            String.format("got table-group stats for schema(%s) cost %dms: %s", targetSchema, elapsed,
                JSON.toJSONString(tablesStatInfo)));

        // iterate all table-groups
        for (TableGroupDetailConfig tableGroupConfig : tableGroupConfigs) {
            String schema = tableGroupConfig.getTableGroupRecord().schema;
            if (targetSchema != null && !targetSchema.equalsIgnoreCase(schema)) {
                continue;
            }
            if (tableGroupConfig.getTableGroupRecord().isColumnarTableGroup()) {
                continue;
            }
            TableGroupStat tableGroupStat = new TableGroupStat(tableGroupConfig);

            // iterate all tables in a table-group
            for (TablePartRecordInfoContext tableContext : tableGroupConfig.getTablesPartRecordInfoContext()) {
                String table = tableContext.getTableName().toLowerCase(Locale.ROOT);
                if (targetTable != null && !targetTable.equalsIgnoreCase(table)) {
                    continue;
                }

                List<TablePartitionRecord> tablePartitionRecords = null;
                if (tableContext.getSubPartitionRecList().isEmpty()) {
                    tablePartitionRecords =
                        tableContext
                            .filterPartitions(x -> x.partLevel != TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
                } else {
                    tablePartitionRecords =
                        tableContext
                            .filterSubPartitions(
                                x -> x.partLevel != TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE);
                }
                Map<String, MySQLTablesRowVO> tableStatInfo = tablesStatInfo.get(table);

                // TODO: use lock to avoid meta too old exception
                if (tableStatInfo == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, targetSchema, table);
                }

                // iterate all partitions in a table
                for (TablePartitionRecord record : tablePartitionRecords) {
                    PartitionInfo info = pm.getPartitionInfo(table);
                    PartitionStat pgStat = new PartitionStat(tableGroupConfig, record, info);
                    MySQLTablesRowVO phyTableInfo = tableStatInfo.get(record.phyTable.toLowerCase());

                    // TODO: use lock to avoid meta too old exception
                    if (phyTableInfo == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, targetSchema, table);
                    }

                    pgStat.setDataLength(phyTableInfo.dataLength);
                    pgStat.setIndexLength(phyTableInfo.indexLength);
                    pgStat.setDataRows(phyTableInfo.dataRows);

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
        List<List<Object>> result = new ArrayList<>();
        IGroupExecutor ge = null;
        try {
            ExecutorContext ec = ExecutorContext.getContext(schema);
            ge = ec.getTopologyExecutor().getGroupExecutor(groupName);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(
                String.format("query group %s with sql %s failed: %s", groupName, sql, e.getMessage()), e);
        }

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
     * <p>
     * Hierarchy:
     * TableGroup
     * | PartitionGroup pg1
     * | PhysicalTable pt1
     * | PhysicalTable pt2
     * | PartitionGroup pg2
     * | PartitionGroup pg...
     *
     * @return <LogicalTable, <PhysicalTable, MySQLTablesRow>>
     */
    public static Map<String, Map<String, MySQLTablesRowVO>> queryTableGroupStats(String schema,
                                                                                  List<TableGroupDetailConfig> tableGroups) {
        Map<String, Map<String, MySQLTablesRowVO>> result = new HashMap<>();

        Map<String, String> phyTable2LogicalTableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (TableGroupDetailConfig tg : tableGroups) {
            phyTable2LogicalTableMap.putAll(tg.phyToLogicalTables());
        }

        List<PartitionGroupRecord> allPgList =
            tableGroups.stream().flatMap(x -> x.getPartitionGroupRecords().stream())
                .collect(Collectors.toList());

        // Group all partition-groups by physical database, to avoid iterate all partitions
        allPgList.stream()
            .collect(Collectors.groupingBy(x -> x.phy_db))
            .forEach((physicalDb, pgList) -> {
                String sql = genQueryPartitionGroupStatsSQL(physicalDb);

                List<List<Object>> rows = queryGroupByPhyDb(schema, physicalDb, sql);
//                SQLRecorderLogger.ddlLogger.info(
//                    String.format("got group for phydb stats for schema(%s): %s", schema, JSON.toJSONString(rows)));

                for (List<Object> row : rows) {
                    MySQLTablesRowVO rowVO = MySQLTablesRowVO.fromRow(row);
                    String physicalTable = rowVO.getPhyTable();
                    String logicalTable = phyTable2LogicalTableMap.get(physicalTable);
                    // table not in the target table-group
                    if (logicalTable == null) {
                        continue;
                    }

                    result.computeIfAbsent(logicalTable.toLowerCase(), x -> new HashMap<>())
                        .put(physicalTable.toLowerCase(), rowVO);
                }
            });

        return result;
    }

    /**
     * Build a SQL to query information_schema.table_statistics
     */
    private static String genQueryPartitionStatisticsSQL(String phyDb) {

        return MySQLTableStatisticRowVO.genSelectClause()
            + String.format(" WHERE table_schema = '%s' ", phyDb);
    }

    /**
     * Build a SQL to collect stats of mysql information_schema.tables
     */
    private static String genQueryPartitionGroupStatsSQL(String phyDb) {

        return MySQLTablesRowVO.genSelectClause()
            + String.format(" WHERE table_schema = '%s'", phyDb);
    }

    /**
     * Query phyDbNames of a logical db
     */
    public static void queryPhyDbNames(Set<String> schemaNames, Map<String, Set<String>> dbPhyDbNames,
                                       Map<String, Set<String>> dbAllPhyDbNames,
                                       Map<String, Pair<String, String>> storageInstIdGroupNames) {
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

                String groupName = groupDetailInfoExRecord.groupName;
                storageInstIdGroupNames.put(phyDbName, new Pair<>(instId, groupName));

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
     * Query GroupName and InstId of schema
     */
    public static Map<String, String> queryGroupNameAndInstId(String schemaName) {
        Map<String, String> storageInstIdGroupNames = new HashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> completedGroupInfos =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());
            Collections.sort(completedGroupInfos);

            for (GroupDetailInfoExRecord groupDetailInfoExRecord : completedGroupInfos) {
                if (schemaName == null || !schemaName.equalsIgnoreCase(groupDetailInfoExRecord.getDbName())) {
                    continue;
                }
                String instId = groupDetailInfoExRecord.storageInstId;
                String groupName = groupDetailInfoExRecord.groupName;
                storageInstIdGroupNames.put(instId, groupName);
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException("Failed to get storage and phy db info", ex);
        }
        return storageInstIdGroupNames;
    }

    public static Map<String, List<String>> queryAllGroupNameAndInstId(String schemaName) {
        Map<String, List<String>> storageInstIdGroupNames = new HashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> completedGroupInfos =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());
            Collections.sort(completedGroupInfos);

            for (GroupDetailInfoExRecord groupDetailInfoExRecord : completedGroupInfos) {
                if (schemaName == null || !schemaName.equalsIgnoreCase(groupDetailInfoExRecord.getDbName())) {
                    continue;
                }
                String instId = groupDetailInfoExRecord.storageInstId;
                String groupName = groupDetailInfoExRecord.groupName;
                storageInstIdGroupNames.putIfAbsent(instId, new ArrayList<>());
                storageInstIdGroupNames.get(instId).add(groupName);
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException("Failed to get storage and phy db info", ex);
        }
        return storageInstIdGroupNames;
    }

    /**
     * <pre>
     *
     * 0: PART_DESC
     * 1: LOGICAL_TABLE_NAME
     * 2: PHYSICAL_TABLE
     * 3: PHYSICAL_SCHEMA
     * 4: TABLE_ROWS
     * 5: DATA_LENGTH
     * 6: INDEX_LENGTH
     * 7: ROWS_READ
     * 8: ROWS_INSERTED
     * 9: ROWS_UPDATED
     * 10: ROWS_DELETED
     * </pre>
     * Query statistics of all filtered schema
     */
    public static Map<String, Map<String, List<Object>>> queryTableSchemaStats(Set<String> schemaNames,
                                                                               Set<String> indexTableNames,
                                                                               String tableLike,
                                                                               Map<String, Pair<String, String>> storageInstIdGroupNames,
                                                                               Integer maxScanTablesNum) {
        Map<String, Map<String, List<Object>>> phyDbTablesInfo = new HashMap<>();
        if (schemaNames == null || schemaNames.isEmpty()) {
            return phyDbTablesInfo;
        }

        Map<String, Set<String>> dbPhyDbNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Set<String>> dbAllPhyDbNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        queryPhyDbNames(schemaNames, dbPhyDbNames, dbAllPhyDbNames, storageInstIdGroupNames);

        boolean isMeetMax = false;
        int scanTablesNum = 0;

        // build sql and exec
        for (String schemaName : schemaNames) {
            if (isMeetMax) {
                break;
            }
            // get phy tables info of each logical db (character may be different)
            Set<String> phyDbNames = dbPhyDbNames.get(schemaName);
            Set<String> allPhyDbNames = dbAllPhyDbNames.get(schemaName);
            if (phyDbNames == null || phyDbNames.isEmpty()) {
                continue;
            }

            // get phy tables info from each data node
            String sql = generateQueryPhyTablesStatsSQL(schemaName, allPhyDbNames, indexTableNames, tableLike);

            // get phy tables statistic
            String statisticSql =
                generateQueryPhyTablesStatisticsSQL(schemaName, allPhyDbNames, indexTableNames, tableLike);

            List<List<Object>> rows = new ArrayList<>();
            List<List<Object>> statisticRows = new ArrayList<>();
            for (String phyDbName : phyDbNames) {
                List<List<Object>> phyDbs = queryGroupByPhyDb(schemaName, phyDbName, sql);
                scanTablesNum += phyDbs.size();
                if (maxScanTablesNum != null && maxScanTablesNum > 0 && scanTablesNum > maxScanTablesNum) {
                    isMeetMax = true;
                    break;
                }
                rows.addAll(phyDbs);
                statisticRows.addAll(queryGroupByPhyDb(schemaName, phyDbName, statisticSql));
            }

            // add phyDbTablesInfo
            for (List<Object> row : rows) {
                String phyTbName = (String) row.get(2);
                String phyDbName = (String) row.get(3);

                Map<String, List<Object>> phyDb =
                    phyDbTablesInfo.computeIfAbsent(phyDbName.toLowerCase(), x -> new HashMap<>());
                phyDb.put(phyTbName.toLowerCase(), row);
            }

            // add phyDbTablesInfo statistic
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

    public static Map<String, Map<String, List<Object>>> queryTableSchemaStatsForHeatmap(Set<String> schemaNames,
                                                                                         Set<String> indexTableNames,
                                                                                         Map<String, Pair<String, String>> storageInstIdGroupNames,
                                                                                         Integer maxScanTablesNum,
                                                                                         Integer maxSingleLogicSchemaCount,
                                                                                         Map<String, Long> phyTableRows) {
        Map<String, Map<String, List<Object>>> phyDbTablesInfo = new HashMap<>();
        if (schemaNames == null || schemaNames.isEmpty()) {
            return phyDbTablesInfo;
        }

        Map<String, Set<String>> dbPhyDbNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Set<String>> dbAllPhyDbNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        queryPhyDbNames(schemaNames, dbPhyDbNames, dbAllPhyDbNames, storageInstIdGroupNames);

        boolean isMeetMax = false;
        int scanTablesNum = 0;

        // build sql and exec
        for (String schemaName : schemaNames) {
            if (isMeetMax) {
                break;
            }
            // get phy tables info of each logical db (character may be different)
            Set<String> phyDbNames = dbPhyDbNames.get(schemaName);
            Set<String> allPhyDbNames = dbAllPhyDbNames.get(schemaName);
            if (phyDbNames == null || phyDbNames.isEmpty()) {
                continue;
            }

            String sql =
                generateQueryPhyTablesStatsSQLForHeatmap(schemaName, allPhyDbNames, indexTableNames,
                    maxSingleLogicSchemaCount);

            String countSql = generateQueryPhyTablesCountSQLForHeatmap(schemaName, allPhyDbNames, indexTableNames);

            List<List<Object>> rows = new ArrayList<>();
            for (String phyDbName : phyDbNames) {
                Long count = queryCountByPhyDb(schemaName, phyDbName, countSql);
                if (count > maxSingleLogicSchemaCount) {
                    continue;
                }
                scanTablesNum += count;
                if (maxScanTablesNum != null && maxScanTablesNum > 0 && scanTablesNum > maxScanTablesNum) {
                    isMeetMax = true;
                    break;
                }

                List<List<Object>> phyDbs = queryGroupByPhyDb(schemaName, phyDbName, sql);
                rows.addAll(phyDbs);
            }

            // add phyDbTablesInfo
            for (List<Object> row : rows) {
                String phyTbName = ((String) row.get(0)).toLowerCase();
                String phyDbName = ((String) row.get(1)).toLowerCase();

                phyTableRows.put(getPhyTableRowsKey(phyDbName, phyTbName), DataTypes.LongType.convertFrom(row.get(2)));

                Map<String, List<Object>> phyDb =
                    phyDbTablesInfo.computeIfAbsent(phyDbName, x -> new HashMap<>());
                phyDb.put(phyTbName, row);
            }
        }
        return phyDbTablesInfo;
    }

    public static Long queryCountByPhyDb(String schemaName, String phyDbName, String countSql) {
        List<List<Object>> phyDbs = queryGroupByPhyDb(schemaName, phyDbName, countSql);
        if (phyDbs == null) {
            return 0L;
        }
        Long count = 0L;
        for (List<Object> row : phyDbs) {
            count += DataTypes.LongType.convertFrom(row.get(0));
        }
        return count;
    }

    public static String getPhyTableRowsKey(String phyDbName, String phyTbName) {
        return String.format("%s,%s", phyDbName, phyTbName);
    }

    public static Map<String, Map<String, List<Object>>> queryTableSchemaStaticsWithoutRowsForHeatmap(
        Set<String> schemaNames,
        Set<String> indexTableNames,
        Map<String, Pair<String, String>> storageInstIdGroupNames,
        Integer maxScanTablesNum,
        Integer maxSingleLogicSchemaCount,
        Map<String, Long> phyTableRows) {
        Map<String, Map<String, List<Object>>> phyDbTablesInfo = new HashMap<>();
        if (schemaNames == null || schemaNames.isEmpty()) {
            return phyDbTablesInfo;
        }

        Map<String, Set<String>> dbPhyDbNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Set<String>> dbAllPhyDbNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        queryPhyDbNames(schemaNames, dbPhyDbNames, dbAllPhyDbNames, storageInstIdGroupNames);

        boolean isMeetMax = false;
        int scanTablesNum = 0;

        // build sql and exec
        for (String schemaName : schemaNames) {
            if (isMeetMax) {
                break;
            }
            // get phy tables info of each logical db (character may be different)
            Set<String> phyDbNames = dbPhyDbNames.get(schemaName);
            Set<String> allPhyDbNames = dbAllPhyDbNames.get(schemaName);
            if (phyDbNames == null || phyDbNames.isEmpty()) {
                continue;
            }

            String sql =
                generateQueryPhyStaticsSQLForHeatmap(schemaName, allPhyDbNames, indexTableNames,
                    maxSingleLogicSchemaCount);

            String countSql =
                generateQueryPhyTableStatisticsCountSQLForHeatmap(schemaName, allPhyDbNames, indexTableNames);

            List<List<Object>> rows = new ArrayList<>();
            for (String phyDbName : phyDbNames) {
                Long count = queryCountByPhyDb(schemaName, phyDbName, countSql);
                if (count > maxSingleLogicSchemaCount) {
                    continue;
                }
                scanTablesNum += count;
                if (maxScanTablesNum != null && maxScanTablesNum > 0 && scanTablesNum > maxScanTablesNum) {
                    isMeetMax = true;
                    break;
                }

                List<List<Object>> phyDbs = queryGroupByPhyDb(schemaName, phyDbName, sql);
                rows.addAll(phyDbs);
            }

            // add phyDbTablesInfo
            for (List<Object> row : rows) {
                String phyTbName = ((String) row.get(0)).toLowerCase();
                String phyDbName = ((String) row.get(1)).toLowerCase();

                Long tableRows = phyTableRows.get(getPhyTableRowsKey(phyDbName, phyTbName));
                if (tableRows != null) {
                    row.set(2, tableRows);
                }

                Map<String, List<Object>> phyDb =
                    phyDbTablesInfo.computeIfAbsent(phyDbName, x -> new HashMap<>());
                phyDb.put(phyTbName, row);
            }
        }
        return phyDbTablesInfo;
    }

    public static Map<String, Map<String, List<Object>>> queryTableGroupStatsForHeatmap(
        TableGroupConfig tableGroupConfig,
        Set<String> indexTableNames,
        String tableLike,
        Map<String, Map<String, List<Object>>> phyDbTablesInfoForHeatmap) {
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        String targetSchema = tableGroupRecord.getSchema();
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(targetSchema), targetSchema + " not exists");
        PartitionInfoManager pm = oc.getPartitionInfoManager();
        Map<String, Map<String, List<Object>>> result = new HashMap<>();
        for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
            for (String logicalTableName : tableGroupConfig.getAllTables()) {
                // table name filter
                logicalTableName = logicalTableName.toLowerCase();
                if (!isFilterTable(indexTableNames, tableLike, logicalTableName)) {
                    continue;
                }
                PartitionInfo partitionInfo = pm.getPartitionInfo(logicalTableName);
                if (partitionInfo == null) {
                    logger.warn(String.format(
                        "queryTableGroupStatsForHeatmap partitionInfo is null. logicalTableName=%s",
                        logicalTableName));
                    continue;
                }
                long partitionGroupId = partitionGroupRecord.id;
                PartitionSpec partitionSpec =
                    partitionInfo.getPartitionBy().getPhysicalPartitions().stream()
                        .filter(o -> o.getLocation().getPartitionGroupId().longValue() == partitionGroupId).findFirst()
                        .orElse(null);
                if (partitionSpec == null) {
                    logger.warn(String.format(
                        "queryTableGroupStatsForHeatmap PartitionSpec is null. logicalTableName=%s, partitionGroupId=%s",
                        logicalTableName, partitionGroupId));
                    continue;
                }
                String phyDbName = partitionGroupRecord.phy_db.toLowerCase();
                String phyTbName = partitionSpec.getLocation().getPhyTableName().toLowerCase();
                List<Object> row;
                try {
                    Map<String, List<Object>> phyTablesMap = phyDbTablesInfoForHeatmap.get(phyDbName);
                    if (phyTablesMap == null) {
                        row = getDefaultRowList(phyTbName, phyDbName);
                    } else {
                        row = phyTablesMap.get(phyTbName);
                    }
                    if (row == null) {
                        //row is null when phy table numbers is over max number. or not be accessed.
                        row = getDefaultRowList(phyTbName, phyDbName);
                    }
                } catch (Exception ex) {
                    throw GeneralUtil.nestedException("Failed to get physical table info ", ex);
                }

                Map<String, List<Object>> table =
                    result.computeIfAbsent(logicalTableName, x -> new HashMap<>());
                table.put(phyTbName, row);
            }
        }

        return result;
    }

    public static List<Object> getDefaultRowList(String phyTbName, String phyDbName) {
        return Arrays.asList(phyTbName, phyDbName, 0, 0, 0, 0);
    }

    /**
     * <pre>
     *  key: logTbName
     *  val:
     *      key: phyTb
     *      val: Map:
     *          key: boundValue
     *          key: subBoundValue
     *          key: phyPartPosition
     *          key: logicalTable
     *          key: partitionName
     *          key: subPartitionName
     *          key: subPartitionTemplateName
     *          key: phyPartPosition
     *          key: physicalTable
     *          key: physicalSchema
     *          key: physicalTableRows
     *          key: physicalDataLength
     *          key: physicalIndexLength
     *          key: physicalRowsRead
     *          key: physicalRowsInserted
     *          key: physicalRowsUpdated
     *          key: physicalRowsDeleted
     * </pre>
     * <p>
     * Query statistics of a table-group
     */
    public static Map<String, Map<String, Map<String, Object>>> queryTableGroupStatInfos(
        TableGroupConfig tableGroupConfig,
        Set<String> indexTableNames,
        String tableLike,
        Map<String, Map<String, List<Object>>> phyDbTablesInfo) {
        Map<String, Map<String, Map<String, Object>>> result = new HashMap<>();
        int tableGroupType =
            tableGroupConfig.getTableGroupRecord() != null ? tableGroupConfig.getTableGroupRecord().tg_type :
                TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;
        boolean isBroadCastTg = (tableGroupType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG);

        /**
         * scan each partition group
         */
        String dbName = tableGroupConfig.getTableGroupRecord().getSchema();
        SchemaManager latestSchemaManager = OptimizerContext.getContext(dbName).getLatestSchemaManager();
        for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {

            /**
             * scan each logical table of table group
             */
            for (String logicalTableName : tableGroupConfig.getAllTables()) {

                /**
                 * Filter unused tableName by tableLike, logicalTableName may be gsi or logTb
                 */
                logicalTableName = logicalTableName.toLowerCase();
                if (!isFilterTable(indexTableNames, tableLike, logicalTableName)) {
                    continue;
                }

                String pgName = partitionGroupRecord.partition_name;

                /**
                 * Fetch partInfo by dbName and logicalTableName
                 */
                TableMeta tableMeta = latestSchemaManager.getTable(logicalTableName);
                PartitionInfo partInfo = tableMeta.getPartitionInfo();
                boolean useSubPart = partInfo.getPartitionBy().getSubPartitionBy() != null;

                PartitionSpec phySpec = partInfo.getPartSpecSearcher().getPartSpecByPartName(pgName);
                PartitionSpec parentPartSpec = null;
                PartitionSpec lastPhySpec = null;
                PartitionSpec lastParentPartSpec = null;
                Long parentPartPosi = phySpec.getParentPartPosi();
                Long partPosiOfPhyPart = phySpec.getPosition();
                List<PartitionSpec> topLevelPartSpecs = partInfo.getPartitionBy().getPartitions();
                if (useSubPart) {
                    parentPartSpec = topLevelPartSpecs.get(parentPartPosi.intValue() - 1);
                    if (partPosiOfPhyPart.intValue() > 1) {
                        Long partPosiOfLastPhyPart = partPosiOfPhyPart - 1;
                        lastPhySpec = parentPartSpec.getSubPartitions().get(partPosiOfLastPhyPart.intValue() - 1);
                    }
                    if (parentPartPosi > 1) {
                        Long partPosiOfLastParentPart = parentPartPosi - 1;
                        lastParentPartSpec = topLevelPartSpecs.get(partPosiOfLastParentPart.intValue() - 1);
                    }
                } else {
                    Long partPosiOfLastPhyPart = partPosiOfPhyPart - 1;
                    if (partPosiOfPhyPart.intValue() > 1) {
                        lastPhySpec = topLevelPartSpecs.get(partPosiOfLastPhyPart.intValue() - 1);
                    }
                }

                String phyDbName = partitionGroupRecord.phy_db.toLowerCase();
                String phyTbName = phySpec.getLocation().getPhyTableName().toLowerCase();
                List<Object> row;
                try {
                    Map<String, List<Object>> phyTablesMap = phyDbTablesInfo.get(phyDbName);
                    if (phyTablesMap == null) {
                        continue;
                    }
                    row = phyTablesMap.get(phyTbName);
                    if (row == null) {
                        //row is null when phy table numbers is over max number.
                        continue;
                    }

                    String pName = "";
                    String spName = "";
                    String spTempName = "";
                    Long globalPhyPartSpecPosi = null;

                    String boundValue = "";
                    String subBoundValue = "";
                    if (useSubPart) {
                        PartitionByDefinition subPartBy = partInfo.getPartitionBy().getSubPartitionBy();
                        Long subPartPosi = partPosiOfPhyPart;
                        String subPartDescVal = phySpec.getBoundSpec().toString();
                        String lastSubPartDescVal = null;
                        if (lastPhySpec != null) {
                            lastSubPartDescVal = lastPhySpec.getBoundSpec().toString();
                        }
                        int subpartCols = subPartBy.getPartitionColumnNameList().size();
                        boolean multiDatumOfSubPart = (subpartCols > 1) && !subPartBy.getStrategy().isHashed();
                        boolean isSubPartList = subPartBy.getStrategy().isList();
                        subBoundValue =
                            buildCompleteBoundVal(isBroadCastTg, subPartPosi, subPartDescVal, lastSubPartDescVal,
                                subpartCols, multiDatumOfSubPart, isSubPartList);

                        PartitionByDefinition partBy = partInfo.getPartitionBy();
                        Long partPosi = parentPartPosi;
                        String partDescVal = parentPartSpec.getBoundSpec().toString();
                        String lastPartDescVal = null;
                        if (lastParentPartSpec != null) {
                            lastPartDescVal = lastParentPartSpec.getBoundSpec().toString();
                        }
                        int partCols = partBy.getPartitionColumnNameList().size();
                        boolean multiDatumOfPart = (partCols > 1) && !partBy.getStrategy().isHashed();
                        boolean isPartList = partBy.getStrategy().isList();
                        boundValue = buildCompleteBoundVal(isBroadCastTg, partPosi, partDescVal, lastPartDescVal,
                            partCols, multiDatumOfPart, isPartList);

                        spName = phySpec.getName();
                        spTempName = phySpec.getTemplateName();
                        pName = parentPartSpec.getName();
                    } else {
                        PartitionByDefinition partBy = partInfo.getPartitionBy();
                        Long partPosi = phySpec.getPosition();
                        String partDescVal = phySpec.getBoundSpec().toString();
                        String lastPartDescVal = null;
                        if (lastPhySpec != null) {
                            lastPartDescVal = lastPhySpec.getBoundSpec().toString();
                        }
                        int partCols = partBy.getPartitionColumnNameList().size();
                        boolean multiDatumOfPart = (partCols > 1) && !partBy.getStrategy().isHashed();
                        boolean isPartList = partBy.getStrategy().isList();
                        boundValue = buildCompleteBoundVal(isBroadCastTg, partPosi, partDescVal, lastPartDescVal,
                            partCols, multiDatumOfPart, isPartList);
                        pName = phySpec.getName();
                    }
                    globalPhyPartSpecPosi = phySpec.getPhyPartPosition();

                    Map<String, Object> phyTbStatInfo = new HashMap<>();

                    phyTbStatInfo.put("boundValue", boundValue);
                    phyTbStatInfo.put("subBoundValue", subBoundValue);
                    phyTbStatInfo.put("logicalTable", logicalTableName.toLowerCase());
                    phyTbStatInfo.put("partName", pName);
                    phyTbStatInfo.put("subpartName", spName);
                    phyTbStatInfo.put("subpartTemplateName", spTempName);
                    phyTbStatInfo.put("phyPartPosition", globalPhyPartSpecPosi);

                    Object physicalTable = row.get(2);
                    phyTbStatInfo.put("physicalTable", physicalTable);

                    Object physicalSchema = row.get(3);
                    phyTbStatInfo.put("physicalSchema", physicalSchema);

                    Object physicalTableRows = row.get(4);
                    phyTbStatInfo.put("physicalTableRows", physicalTableRows);

                    Object physicalDataLength = row.get(5);
                    phyTbStatInfo.put("physicalDataLength", physicalDataLength);

                    Object physicalIndexLength = row.get(6);
                    phyTbStatInfo.put("physicalIndexLength", physicalIndexLength);

                    if (row.size() > 7) {
                        // ROWS_READ
                        Object physicalRowsRead = row.get(7);
                        phyTbStatInfo.put("physicalRowsRead", physicalRowsRead);

                        // ROWS_INSERTED
                        Object physicalRowsInserted = row.get(8);
                        phyTbStatInfo.put("physicalRowsInserted", physicalRowsInserted);

                        // ROWS_UPDATED
                        Object physicalRowsUpdated = row.get(9);
                        phyTbStatInfo.put("physicalRowsUpdated", physicalRowsUpdated);

                        // ROWS_DELETED
                        Object physicalRowsDeleted = row.get(10);
                        phyTbStatInfo.put("physicalRowsDeleted", physicalRowsDeleted);
                    }

                    Map<String, Map<String, Object>> table =
                        result.computeIfAbsent(logicalTableName.toLowerCase(), x -> new HashMap<>());
                    table.put(phyTbName.toLowerCase(), phyTbStatInfo);

                } catch (Exception ex) {
                    throw GeneralUtil.nestedException("Failed to get physical table info ", ex);
                }
            }
        }

        return result;
    }

    @NotNull
    private static String buildCompleteBoundVal(boolean isBroadCastTg,
                                                Long partPosi,
                                                String partDescVal,
                                                String lastPartDescVal,
                                                int partCols,
                                                boolean multiDatum,
                                                boolean isList) {
        StringBuilder partDesc = new StringBuilder();
        if (!isBroadCastTg) {
            if (partPosi > 1) {
                if (isList) {
                    partDesc.append("(");
                } else {
                    partDesc.append("[");
                }
                if (lastPartDescVal != null && !isList) {
                    if (multiDatum) {
                        partDesc.append("(");
                    }
                    partDesc.append(lastPartDescVal.replaceAll("'", ""));
                    if (multiDatum) {
                        partDesc.append(")");
                    }
                    partDesc.append(", ");
                }
            } else {
                if (!isList) {
                    if (multiDatum) {
                        for (int i = 0; i < partCols; i++) {
                            if (i == 0) {
                                partDesc.append("[(MINVALUE");
                            } else {
                                partDesc.append(",MINVALUE");
                            }
                        }
                        partDesc.append("),");
                    } else {
                        partDesc.append("[MINVALUE, ");
                    }
                } else {
                    partDesc.append("(");
                }
            }
            if (multiDatum) {
                partDesc.append("(");
            }
            partDesc.append(partDescVal.replaceAll("'", ""));
            if (multiDatum) {
                partDesc.append(")");
            }
            partDesc.append(")");
        } else {
            partDesc.append("[MINVALUE, MAXVALUE)");
        }
        return partDesc.toString();
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

    @Data
    static class MySQLTablesRowVO {
        private String phyTable;
        private long dataLength;
        private long indexLength;
        private long dataRows;

        public static String genSelectClause() {
            StringBuilder sb = new StringBuilder();
            sb.append(" SELECT ");
            sb.append(" TABLE_NAME as PHYSICAL_TABLE");
            sb.append(", TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH ");
            sb.append("FROM information_schema.tables ");
            return sb.toString();
        }

        public static MySQLTablesRowVO fromRow(List<Object> row) {
            if (CollectionUtils.isEmpty(row)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Empty row");
            }
            if (row.size() != 4) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Corrupted row: " + row);
            }
            MySQLTablesRowVO res = new MySQLTablesRowVO();
            res.setPhyTable(DataTypes.StringType.convertFrom(row.get(0)).toLowerCase());
            res.setDataRows(DataTypes.LongType.convertFrom(row.get(1)));
            res.setDataLength(DataTypes.LongType.convertFrom(row.get(2)));
            res.setIndexLength(DataTypes.LongType.convertFrom(row.get(3)));
            return res;
        }
    }

    @Data
    static class MySQLTableStatisticRowVO {
        private String phyTable;
        private long rowsRead;
        private long rowsInsert;
        private long rowsUpdate;
        private long rowsDelete;

        public static String genSelectClause() {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            sb.append(" TABLE_NAME as PHYSICAL_TABLE, ");
            sb.append(" ROWS_READ, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED ");

            sb.append(" FROM information_schema.table_statistics ");
            return sb.toString();
        }

        public static MySQLTableStatisticRowVO fromRow(List<Object> row) {
            if (CollectionUtils.isEmpty(row)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Empty row");
            }
            if (row.size() != 5) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Corrupted row: " + row);
            }
            MySQLTableStatisticRowVO res = new MySQLTableStatisticRowVO();
            res.setPhyTable(DataTypes.StringType.convertFrom(row.get(0)));
            res.setRowsRead(DataTypes.LongType.convertFrom(row.get(1)));
            res.setRowsInsert(DataTypes.LongType.convertFrom(row.get(2)));
            res.setRowsUpdate(DataTypes.LongType.convertFrom(row.get(3)));
            res.setRowsDelete(DataTypes.LongType.convertFrom(row.get(3)));
            return res;
        }
    }

    public static Map<String, Pair<Long, Long>> queryDbGroupDataSize(String schema,
                                                                     List<GroupDetailInfoExRecord> groupRecords) {
        Map<String, Pair<Long, Long>> groupDataSizeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
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
                    long tableRows = ((BigDecimal) row.get(2)).longValue();
                    String groupName = phyDbToGroup.get(phyDbName);

                    groupDataSizeMap.merge(groupName, Pair.of(tableRows, dataSizeKB * 1024),
                        new BiFunction<Pair<Long, Long>, Pair<Long, Long>, Pair<Long, Long>>() {
                            @Override
                            public Pair<Long, Long> apply(Pair<Long, Long> a, Pair<Long, Long> b) {
                                return Pair.of(a.getKey() + b.getKey(), a.getValue() + b.getValue());
                            }
                        });
                }
            });
        return groupDataSizeMap;
    }

    private static String genDbGroupSQL(List<String> phyDbList) {
        String phyDbStr = phyDbList.stream().map(TStringUtil::quoteString).collect(Collectors.joining(","));
        String sql = String.format("SELECT table_schema \"PhyDbName\",  " +
            "sum( data_length + index_length ) / 1024 \"DataSizeKB\", " +
            "SUM(TABLE_ROWS) \"TABLE_ROWS\" " +
            "FROM information_schema.TABLES " +
            "WHERE table_schema in (%s) " +
            "GROUP BY table_schema ", phyDbStr);
        return sql;
    }

    public static String genTableRowsCountSQL(String phyDb, String phyTableName) {
        return String.format(
            "SELECT IFNULL(TABLE_ROWS,0) as TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"%s\" AND TABLE_NAME = \"%s\"",
            phyDb, phyTableName);
    }

    public static String genAvgTableRowLengthSQL(String phyDb, String phyTableName) {
        return String.format(
            "SELECT IFNULL(`AVG_ROW_LENGTH`, 0) from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \"%s\" AND TABLE_NAME = \"%s\"",
            phyDb, phyTableName
        );
    }

    /**
     * Build a SQL to collect stats of mysql table
     */
    private static String generateQueryPhyTablesStatsSQL(String logicalSchema, Set<String> schemaNames,
                                                         Set<String> indexTableNames,
                                                         String tableLike) {
        StringBuilder sb = new StringBuilder();
        int schemaIndex = 0;
        SchemaManager sm = OptimizerContext.getContext(logicalSchema).getLatestSchemaManager();
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
                    String pattern;
                    try {
                        PartitionInfo partitionInfo = sm.getTable(tableName).getPartitionInfo();
                        pattern = partitionInfo.getTableNamePattern();
                    } catch (Exception ex) {
                        pattern = tableName;
                    }

                    String filter = "table_name like '" + pattern + "%'";
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

    private static String generateQueryPhyTablesCountSQLForHeatmap(String logicalSchema,
                                                                   Set<String> schemaNames,
                                                                   Set<String> indexTableNames) {
        SchemaManager sm = OptimizerContext.getContext(logicalSchema).getLatestSchemaManager();
        StringBuilder sb = new StringBuilder();
        int schemaIndex = 0;
        for (String schemaName : schemaNames) {
            if (schemaIndex != 0) {
                sb.append(" union all ");
            }
            sb.append(
                "select count(1) " +
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
                    String pattern;
                    try {
                        PartitionInfo partitionInfo = sm.getTable(tableName).getPartitionInfo();
                        pattern = partitionInfo.getTableNamePattern();
                    } catch (Exception ex) {
                        pattern = tableName;
                    }

                    String filter = "table_name like '" + pattern + "%'";
                    if (schemaIndex != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    schemaIndex++;
                }
                sb.append(")");
            }
            schemaIndex++;
        }
        return sb.toString();
    }

    private static String generateQueryPhyTableStatisticsCountSQLForHeatmap(String logicalSchema,
                                                                            Set<String> schemaNames,
                                                                            Set<String> indexTableNames) {
        StringBuilder sb = new StringBuilder();
        SchemaManager sm = OptimizerContext.getContext(logicalSchema).getLatestSchemaManager();
        int schemaIndex = 0;
        for (String schemaName : schemaNames) {
            if (schemaIndex != 0) {
                sb.append(" union all ");
            }
            sb.append(
                "select count(1) " +
                    " FROM information_schema.table_statistics " +
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
                    String pattern;
                    try {
                        PartitionInfo partitionInfo = sm.getTable(tableName).getPartitionInfo();
                        pattern = partitionInfo.getTableNamePattern();
                    } catch (Exception ex) {
                        pattern = tableName;
                    }

                    String filter = "table_name like '" + pattern + "%'";
                    if (schemaIndex != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    schemaIndex++;
                }
                sb.append(")");
            }
            schemaIndex++;
        }
        return sb.toString();
    }

    private static String generateQueryPhyTablesStatsSQLForHeatmap(String logicalSchema, Set<String> schemaNames,
                                                                   Set<String> indexTableNames,
                                                                   Integer maxSingleLogicSchemaCount) {
        StringBuilder sb = new StringBuilder();
        int schemaIndex = 0;
        SchemaManager sm = OptimizerContext.getContext(logicalSchema).getLatestSchemaManager();
        int limit = maxSingleLogicSchemaCount / schemaNames.size();
        for (String schemaName : schemaNames) {
            if (schemaIndex != 0) {
                sb.append(" union all ");
            }
            sb.append("(");
            sb.append(
                "select " +
                    " t.TABLE_NAME as PHYSICAL_TABLE" +
                    ", t.TABLE_SCHEMA as PHYSICAL_SCHEMA" +
                    ", IFNULL(t.TABLE_ROWS,0) as TABLE_ROWS" +
                    ", IFNULL(s.ROWS_READ,0) as ROWS_READ  " +
                    ", IFNULL(s.ROWS_INSERTED,0) as ROWS_INSERTED  " +
                    ", IFNULL(s.ROWS_UPDATED,0) as ROWS_UPDATED  " +
                    " FROM information_schema.TABLES t LEFT JOIN information_schema.table_statistics s "
                    + "ON t.TABLE_NAME = s.TABLE_NAME AND t.TABLE_SCHEMA = s.TABLE_SCHEMA " +
                    " where t.table_schema = ");
            // handle schemaName filter
            sb.append("'");
            sb.append(schemaName);
            sb.append("'");

            // handle tableName filter
            if (indexTableNames != null && !indexTableNames.isEmpty()) {
                sb.append(" and (");
                schemaIndex = 0;
                for (String tableName : indexTableNames) {
                    String pattern;
                    try {
                        PartitionInfo partitionInfo = sm.getTable(tableName).getPartitionInfo();
                        pattern = partitionInfo.getTableNamePattern();
                    } catch (Exception ex) {
                        pattern = tableName;
                    }

                    String filter = "table_name like '" + pattern + "%'";
                    if (schemaIndex != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    schemaIndex++;
                }
                sb.append(")");
            }

            sb.append(" limit ");
            sb.append(limit);
            sb.append(")");
            schemaIndex++;
        }
        return sb.toString();
    }

    private static String generateQueryPhyStaticsSQLForHeatmap(String logicalSchema, Set<String> schemaNames,
                                                               Set<String> indexTableNames,
                                                               Integer maxSingleLogicSchemaCount) {
        StringBuilder sb = new StringBuilder();
        int schemaIndex = 0;
        SchemaManager sm = OptimizerContext.getContext(logicalSchema).getLatestSchemaManager();
        int limit = maxSingleLogicSchemaCount / schemaNames.size();
        for (String schemaName : schemaNames) {
            if (schemaIndex != 0) {
                sb.append(" union all ");
            }
            sb.append("(");
            sb.append(
                "select " +
                    " s.TABLE_NAME as PHYSICAL_TABLE" +
                    ", s.TABLE_SCHEMA as PHYSICAL_SCHEMA" +
                    ", 0 as TABLE_ROWS" +
                    ", IFNULL(s.ROWS_READ,0) as ROWS_READ  " +
                    ", IFNULL(s.ROWS_INSERTED,0) as ROWS_INSERTED  " +
                    ", IFNULL(s.ROWS_UPDATED,0) as ROWS_UPDATED  " +
                    " FROM information_schema.table_statistics s " +
                    " where s.table_schema = ");
            // handle schemaName filter
            sb.append("'");
            sb.append(schemaName);
            sb.append("'");

            // handle tableName filter
            if (indexTableNames != null && !indexTableNames.isEmpty()) {
                sb.append(" and (");
                schemaIndex = 0;
                for (String tableName : indexTableNames) {
                    String pattern;
                    try {
                        PartitionInfo partitionInfo = sm.getTable(tableName).getPartitionInfo();
                        pattern = partitionInfo.getTableNamePattern();
                    } catch (Exception ex) {
                        pattern = tableName;
                    }

                    String filter = "table_name like '" + pattern + "%'";
                    if (schemaIndex != 0) {
                        sb.append(" or ");
                    }
                    sb.append(filter);
                    schemaIndex++;
                }
                sb.append(")");
            }

            sb.append(" limit ");
            sb.append(limit);
            sb.append(")");
            schemaIndex++;
        }
        return sb.toString();
    }

    /**
     * Build a SQL to query information_schema.table_statistics
     */
    private static String generateQueryPhyTablesStatisticsSQL(String logicalSchema, Set<String> schemaNames,
                                                              Set<String> indexTableNames,
                                                              String tableLike) {
        StringBuilder sb = new StringBuilder();
        int schemaIndex = 0;
        SchemaManager sm = OptimizerContext.getContext(logicalSchema).getLatestSchemaManager();
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
                    String pattern;
                    try {
                        PartitionInfo partitionInfo = sm.getTable(tableName).getPartitionInfo();
                        pattern = partitionInfo.getTableNamePattern();
                    } catch (Exception ex) {
                        pattern = tableName;
                    }

                    String filter = "table_name like '" + pattern + "%'";
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

    private static int countPartitionColumns(TablePartitionRecord tablePartitionRecord) {
        assert tablePartitionRecord.partLevel > 0;
        int partCols = tablePartitionRecord.partExpr.split(",").length;

        return partCols;
    }

    private static boolean isListPartitionStrategy(TablePartitionRecord tablePartitionRecord) {

        String method = tablePartitionRecord.partMethod;
        PartitionStrategy strategy = PartitionStrategy.valueOf(method);

        return strategy.isList();
    }

    private static boolean isHashPartitionStrategy(TablePartitionRecord tablePartitionRecord) {

        String method = tablePartitionRecord.partMethod;
        PartitionStrategy strategy = PartitionStrategy.valueOf(method);

        return strategy.isHashed();
    }
}

