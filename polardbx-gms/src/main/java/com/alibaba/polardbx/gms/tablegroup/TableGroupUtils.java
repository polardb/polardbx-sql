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

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * utils to get the tablegroupInfo from metadb.
 *
 * @author luoyanxin
 */
public class TableGroupUtils {

    public static List<TableGroupConfig> getAllTableGroupInfoByDb(String dbName) {

        return MetaDbUtil.queryMetaDbWrapper(null, (conn) -> {
            List<TableGroupConfig> result = new ArrayList<>();
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tableGroupAccessor.setConnection(conn);
            partitionGroupAccessor.setConnection(conn);
            tablePartitionAccessor.setConnection(conn);

            List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getAllTableGroups(dbName);
            Map<Long, List<PartitionGroupRecord>> allPartitionGroupRecordsMap =
                partitionGroupAccessor.getPartitionGroupsBySchema(dbName).stream()
                    .collect(Collectors.groupingBy(x -> x.tg_id));

            Map<Long, List<TablePartitionConfig>> tablePartitionConfigsMap =
                tablePartitionAccessor.getAllTablePartitionConfigs(dbName).stream()
                    .collect(Collectors.groupingBy(x -> x.getTableConfig().groupId));

            for (TableGroupRecord tableGroupRecord : tableGroupRecords) {
                List<PartitionGroupRecord> partitionGroupRecords = allPartitionGroupRecordsMap.get(tableGroupRecord.id);

                List<TablePartitionConfig> tablePartitionConfigsForTableGroup =
                    tablePartitionConfigsMap.get(tableGroupRecord.id);

                List<String> tablesName = new ArrayList<>();
                for (TablePartitionConfig config : GeneralUtil.emptyIfNull(tablePartitionConfigsForTableGroup)) {
                    TablePartitionRecord tablePartitionRecord = config.getTableConfig();
                    if (tablePartitionRecord.partStatus != TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC) {
                        continue;
                    }
                    List<TablePartitionSpecConfig> partitionSpecConfigs = config.getPartitionSpecConfigs();
                    List<TablePartitionRecord> partitionRecList = new ArrayList<>();
                    List<TablePartitionRecord> subPartitionRecList = new ArrayList<>();
                    if (GeneralUtil.isNotEmpty(partitionSpecConfigs)) {
                        for (TablePartitionSpecConfig partitionSpecConfig : partitionSpecConfigs) {
                            partitionRecList.add(partitionSpecConfig.getSpecConfigInfo());
                            if (GeneralUtil.isNotEmpty(partitionSpecConfig.getSubPartitionSpecConfigs())) {
                                for (TablePartitionSpecConfig subPartSpecConfig : partitionSpecConfig.getSubPartitionSpecConfigs()) {
                                    subPartitionRecList.add(subPartSpecConfig.getSpecConfigInfo());
                                }
                            }
                        }
                    }
                    tablesName.add(tablePartitionRecord.getTableName());
                }
                TableGroupConfig tableGroupConfig =
                    new TableGroupConfig(tableGroupRecord,
                        partitionGroupRecords == null ? new ArrayList<>() : partitionGroupRecords,
                        tablesName,
                        tableGroupRecord.getLocality());
                result.add(tableGroupConfig);
            }
            return result;
        });
    }

    public static List<TableGroupDetailConfig> getAllTableGroupDetailInfoByDb(String dbName) {

        return MetaDbUtil.queryMetaDbWrapper(null, (conn) -> {
            List<TableGroupDetailConfig> result = new ArrayList<>();
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tableGroupAccessor.setConnection(conn);
            partitionGroupAccessor.setConnection(conn);
            tablePartitionAccessor.setConnection(conn);

            List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getAllTableGroups(dbName);
            Map<Long, List<PartitionGroupRecord>> allPartitionGroupRecordsMap =
                partitionGroupAccessor.getPartitionGroupsBySchema(dbName).stream()
                    .collect(Collectors.groupingBy(x -> x.tg_id));

            Map<Long, List<TablePartitionConfig>> tablePartitionConfigsMap =
                tablePartitionAccessor.getAllTablePartitionConfigs(dbName).stream()
                    .collect(Collectors.groupingBy(x -> x.getTableConfig().groupId));

            for (TableGroupRecord tableGroupRecord : tableGroupRecords) {
                List<PartitionGroupRecord> partitionGroupRecords = allPartitionGroupRecordsMap.get(tableGroupRecord.id);

                List<TablePartitionConfig> tablePartitionConfigsForTableGroup =
                    tablePartitionConfigsMap.get(tableGroupRecord.id);

                List<TablePartRecordInfoContext> tablePartRecordInfoContexts = new ArrayList<>();
                for (TablePartitionConfig config : GeneralUtil.emptyIfNull(tablePartitionConfigsForTableGroup)) {
                    TablePartitionRecord tablePartitionRecord = config.getTableConfig();
                    if (tablePartitionRecord.partStatus != TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_PUBLIC) {
                        continue;
                    }
                    List<TablePartitionSpecConfig> partitionSpecConfigs = config.getPartitionSpecConfigs();
                    List<TablePartitionRecord> partitionRecList = new ArrayList<>();
                    List<TablePartitionRecord> subPartitionRecList = new ArrayList<>();
                    if (GeneralUtil.isNotEmpty(partitionSpecConfigs)) {
                        for (TablePartitionSpecConfig partitionSpecConfig : partitionSpecConfigs) {
                            partitionRecList.add(partitionSpecConfig.getSpecConfigInfo());
                            if (GeneralUtil.isNotEmpty(partitionSpecConfig.getSubPartitionSpecConfigs())) {
                                for (TablePartitionSpecConfig subPartSpecConfig : partitionSpecConfig.getSubPartitionSpecConfigs()) {
                                    subPartitionRecList.add(subPartSpecConfig.getSpecConfigInfo());
                                }
                            }
                        }
                    }
                    TablePartRecordInfoContext tablePartRecordInfoContext = new TablePartRecordInfoContext();
                    tablePartRecordInfoContext.setLogTbRec(tablePartitionRecord);
                    Collections.sort(partitionRecList,
                        (o1, o2) -> o1.getPartPosition().compareTo(o2.getPartPosition()));
                    tablePartRecordInfoContext.setPartitionRecList(partitionRecList);
                    Collections.sort(subPartitionRecList,
                        (o1, o2) -> o1.getPartPosition().compareTo(o2.getPartPosition()));
                    tablePartRecordInfoContext.setSubPartitionRecList(subPartitionRecList);

                    tablePartRecordInfoContexts.add(tablePartRecordInfoContext);
                }
                TableGroupDetailConfig tableGroupConfig =
                    new TableGroupDetailConfig(tableGroupRecord,
                        partitionGroupRecords == null ? new ArrayList<>() : partitionGroupRecords,
                        tablePartRecordInfoContexts,
                        tableGroupRecord.getLocality());
                result.add(tableGroupConfig);
            }
            return result;
        });
    }

    public static TableGroupConfig getTableGroupInfoByGroupId(Long tableGroupId) {
        TableGroupConfig tableGroupConfig = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            tableGroupConfig = getTableGroupInfoByGroupId(conn, tableGroupId);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return tableGroupConfig;
    }

    public static TableGroupDetailConfig getTableGroupDetailInfoByGroupId(Connection metaDbConn, Long tableGroupId) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            TableGroupDetailConfig tableGroupConfig = null;
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tableGroupAccessor.setConnection(conn);
            partitionGroupAccessor.setConnection(conn);
            tablePartitionAccessor.setConnection(conn);

            List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getTableGroupsByID(tableGroupId);
            if (tableGroupRecords != null && tableGroupRecords.size() > 0) {
                List<PartitionGroupRecord> partitionGroupRecords =
                    partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupId, false);
                List<TablePartRecordInfoContext> tablePartRecordInfoContexts =
                    tablePartitionAccessor
                        .getAllTablePartRecordInfoContextsByGroupId(tableGroupRecords.get(0).schema, tableGroupId,
                            null);

                tableGroupConfig = new TableGroupDetailConfig(tableGroupRecords.get(0),
                    partitionGroupRecords,
                    tablePartRecordInfoContexts,
                    tableGroupRecords.get(0).getLocality());
            }
            return tableGroupConfig;
        });
    }

    public static TableGroupRecord getTableGroupByGroupId(Long tableGroupId) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            tableGroupAccessor.setConnection(conn);
            List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getTableGroupsByID(tableGroupId);
            if (tableGroupRecords.size() > 0) {
                return tableGroupRecords.get(0);
            } else {
                return null;
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static TableGroupConfig getTableGroupInfoByGroupId(Connection metaDbConn, Long tableGroupId) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            TableGroupConfig tableGroupConfig = null;
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tableGroupAccessor.setConnection(conn);
            partitionGroupAccessor.setConnection(conn);
            tablePartitionAccessor.setConnection(conn);

            List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getTableGroupsByID(tableGroupId);
            if (tableGroupRecords != null && tableGroupRecords.size() > 0) {
                List<PartitionGroupRecord> partitionGroupRecords =
                    partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupId, false);
                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartitionAccessor
                        .getTablePartitionsByDbNameGroupId(tableGroupRecords.get(0).schema,
                            tableGroupId);

                tableGroupConfig = new TableGroupConfig(tableGroupRecords.get(0),
                    partitionGroupRecords,
                    tablePartitionRecords.stream().map(o -> o.getTableName()).collect(Collectors.toList()),
                    tableGroupRecords.get(0).getLocality());
            }
            return tableGroupConfig;
        });
    }

    public static TablePartRecordInfoContext getTablePartRecordInfoContextsByDbNameAndTableName(Connection metaDbConn,
                                                                                                String dbName,
                                                                                                String tbName) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            tablePartitionAccessor.setConnection(conn);

            return tablePartitionAccessor.getTablePartRecordInfoContextsByDbNameAndTableName(dbName, tbName);
        });
    }

    public static TableGroupConfig getTableGroupInfoByGroupName(String schemaName, String tableGroupName) {
        return MetaDbUtil.queryMetaDbWrapper(null, (conn) -> {
            TableGroupConfig tableGroupConfig = null;

            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tableGroupAccessor.setConnection(conn);
            partitionGroupAccessor.setConnection(conn);
            tablePartitionAccessor.setConnection(conn);

            List<TableGroupRecord> tableGroupRecords =
                tableGroupAccessor.getTableGroupsBySchemaAndName(schemaName, tableGroupName, false);
            if (tableGroupRecords != null && tableGroupRecords.size() > 0) {
                List<PartitionGroupRecord> partitionGroupRecords =
                    partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupRecords.get(0).id, false);
                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartitionAccessor
                        .getTablePartitionsByDbNameGroupId(tableGroupRecords.get(0).schema,
                            tableGroupRecords.get(0).id);

                tableGroupConfig = new TableGroupConfig(tableGroupRecords.get(0),
                    partitionGroupRecords,
                    tablePartitionRecords.stream().map(o -> o.getTableName()).collect(Collectors.toList()),
                    tableGroupRecords.get(0).getLocality());
            }
            return tableGroupConfig;
        });
    }

    public static PartitionGroupRecord getPartitionGroupById(Long pgId) {
        return MetaDbUtil.queryMetaDbWrapper(null, (conn) -> {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(conn);
            return partitionGroupAccessor.getPartitionGroupById(pgId);
        });
    }

    public static List<PartitionGroupRecord> getOutDatePartitionGroupsByTgId(Connection conn, Long tableGroupId) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(conn);
        return partitionGroupAccessor.getOutDatedPartitionGroupsByTableGroupIdFromDelta(tableGroupId);
    }

    public static int[] insertOldDatedPartitionGroupToDeltaTable(List<PartitionGroupRecord> outDatedPartitionGroups,
                                                                 Connection connection) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(connection);
        return partitionGroupAccessor.insertOldDatedPartitionGroupToDelta(outDatedPartitionGroups);
    }

    public static int[] deleteOldDatedPartitionGroupFromDeltaTableByIds(List<Long> outDatePartitionGroupIds,
                                                                        Connection connection) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(connection);
        return partitionGroupAccessor.deleteOldDatedPartitionGroupFromDelta(outDatePartitionGroupIds);
    }

    public static int[] deleteNewPartitionGroupFromDeltaTableByTgIDAndPartNames(Long tableGroupId,
                                                                                List<String> newPartitionNames,
                                                                                Connection connection) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(connection);
        return partitionGroupAccessor.deleteNewPartitionGroupFromDelta(tableGroupId, newPartitionNames);
    }

    public static void deleteTableGroupInfoBySchema(String schemaName, Connection metadb) {
        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();

        tableGroupAccessor.setConnection(metadb);
        partitionGroupAccessor.setConnection(metadb);
        complexTaskOutlineAccessor.setConnection(metadb);
        List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getAllTableGroups(schemaName);
        tableGroupAccessor.deleteTableGroupsBySchema(schemaName);
        for (TableGroupRecord tableGroupRecord : tableGroupRecords) {
            partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupRecord.id, false);
            partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupRecord.id, true);

        }
        complexTaskOutlineAccessor.deleteComplexTaskBySchema(schemaName);
    }

    public static boolean deleteEmptyTableGroupInfo(String schemaName, Long tableGroupId, Connection metadb) {
        boolean isDelete = false;
        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        tableGroupAccessor.setConnection(metadb);
        partitionGroupAccessor.setConnection(metadb);
        List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getTableGroupsByID(tableGroupId);
        if (tableGroupRecords != null && tableGroupRecords.size() > 0) {
            if (tableGroupRecords.get(0).manual_create == 0) {
                tableGroupAccessor.deleteTableGroupsById(schemaName, tableGroupRecords.get(0).id);
            }
            partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupRecords.get(0).id, false);
            isDelete = true;
        }
        return isDelete;
    }

    public static List<PartitionGroupRecord> getAllUnVisiablePartitionGroupByGroupId(Long tableGroupId) {
        return getAllUnVisiablePartitionGroupByGroupId(null, tableGroupId);
    }

    public static List<PartitionGroupRecord> getAllUnVisiablePartitionGroupByGroupId(Connection metaDbConn,
                                                                                     Long tableGroupId) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(conn);

            return partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupId, true);
        });
    }

    public static List<PartitionGroupRecord> getPartitionGroupsByGroupId(Long tableGroupId) {
        return getPartitionGroupsByGroupId(null, tableGroupId);
    }

    public static List<PartitionGroupRecord> getPartitionGroupsByGroupId(Connection metaDbConn, Long tableGroupId) {
        return MetaDbUtil.queryMetaDbWrapper(metaDbConn, (conn) -> {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(conn);

            return partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupId, false);
        });
    }

    public static Map<Long, PartitionGroupRecord> getPartitionGroupsMapByGroupId(long tableGroupId) {
        return getPartitionGroupsMapByGroupId(null, tableGroupId);
    }

    public static Map<Long, PartitionGroupRecord> getPartitionGroupsMapByGroupId(Connection conn,
                                                                                 long tableGroupId) {
        List<PartitionGroupRecord> partitionGroupRecords = getPartitionGroupsByGroupId(conn, tableGroupId);
        Map<Long, PartitionGroupRecord> partitionGroupRecordsMap = new HashMap<>();
        for (PartitionGroupRecord record : partitionGroupRecords) {
            partitionGroupRecordsMap.put(record.getId(), record);
        }

        return partitionGroupRecordsMap;
    }

    public static List<GroupDetailInfoExRecord> mockTheOrderedLocation(String logicalDbName) {
        List<GroupDetailInfoExRecord> mockInfos = new ArrayList<>(2);
        GroupDetailInfoExRecord mockInfo1 = new GroupDetailInfoExRecord();
        mockInfo1.storageInstId = "mockSid1";
        mockInfo1.dbName = logicalDbName;
        mockInfo1.groupName = logicalDbName.toUpperCase() + "_P00000_GROUP";
        mockInfo1.phyDbName = logicalDbName.toLowerCase() + "_p00000";

        GroupDetailInfoExRecord mockInfo2 = new GroupDetailInfoExRecord();
        mockInfo2.storageInstId = "mockSid2";
        mockInfo2.dbName = logicalDbName;
        mockInfo2.groupName = logicalDbName.toUpperCase() + "_P00001_GROUP";
        mockInfo2.phyDbName = logicalDbName.toLowerCase() + "_p00001";
        mockInfos.add(mockInfo1);
        mockInfos.add(mockInfo2);
        return mockInfos;
    }
}
