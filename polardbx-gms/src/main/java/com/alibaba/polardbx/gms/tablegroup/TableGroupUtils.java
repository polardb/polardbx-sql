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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * utils to get the tablegroupInfo from metadb.
 *
 * @author luoyanxin
 */
public class TableGroupUtils {

    public static List<TableGroupConfig> getAllTableGroupInfoByDb(String dbName) {

        List<TableGroupConfig> result = new ArrayList<>();
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tableGroupAccessor.setConnection(conn);
            partitionGroupAccessor.setConnection(conn);
            tablePartitionAccessor.setConnection(conn);

            List<TableGroupRecord> tableGroupRecords = tableGroupAccessor.getAllTableGroups(dbName);
            for (TableGroupRecord tableGroupRecord : tableGroupRecords) {
                List<PartitionGroupRecord> partitionGroupRecords =
                    partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupRecord.id, false);

                List<TablePartRecordInfoContext> tablePartRecordInfoContexts =
                    tablePartitionAccessor
                        .getAllTablePartRecordInfoContextsByGroupId(dbName, tableGroupRecord.id, null);

                TableGroupConfig tableGroupConfig =
                    new TableGroupConfig(tableGroupRecord, partitionGroupRecords, tablePartRecordInfoContexts);
                result.add(tableGroupConfig);
            }

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return result;
    }

    public static TableGroupConfig getTableGroupInfoByGroupId(Long tableGroupId) {
        TableGroupConfig tableGroupConfig = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
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

                tableGroupConfig = new TableGroupConfig(tableGroupRecords.get(0),
                    partitionGroupRecords,
                    tablePartRecordInfoContexts);
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return tableGroupConfig;
    }

    public static TablePartRecordInfoContext getTablePartRecordInfoContextsByDbNameAndTableName(String dbName,
                                                                                                String tbName) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tablePartitionAccessor.setConnection(conn);

            return tablePartitionAccessor.getTablePartRecordInfoContextsByDbNameAndTableName(dbName, tbName);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static TableGroupConfig getTableGroupInfoByGroupName(String schemaName, String tableGroupName) {
        TableGroupConfig tableGroupConfig = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

            tableGroupAccessor.setConnection(conn);
            partitionGroupAccessor.setConnection(conn);
            tablePartitionAccessor.setConnection(conn);

            List<TableGroupRecord> tableGroupRecords =
                tableGroupAccessor.getTableGroupsBySchemaAndName(schemaName, tableGroupName);
            if (tableGroupRecords != null && tableGroupRecords.size() > 0) {
                List<PartitionGroupRecord> partitionGroupRecords =
                    partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupRecords.get(0).id, false);
                List<TablePartRecordInfoContext> tablePartRecordInfoContexts =
                    tablePartitionAccessor
                        .getAllTablePartRecordInfoContextsByGroupId(tableGroupRecords.get(0).schema,
                            tableGroupRecords.get(0).id,
                            null);

                tableGroupConfig = new TableGroupConfig(tableGroupRecords.get(0),
                    partitionGroupRecords,
                    tablePartRecordInfoContexts);
                String locality = tableGroupRecords.get(0).getLocality();
                if (TStringUtil.isNotBlank(locality)) {
                    tableGroupConfig.setLocality(locality);
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return tableGroupConfig;
    }

    public static Map<String, List<Long>> getAllPhysicalDbAndPartitionGroupMap() {
        Map<String, List<Long>> dbAndPartitionGroupId = new HashMap<>();
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();

            partitionGroupAccessor.setConnection(conn);

            List<PartitionGroupRecord> partitionGroupRecords = partitionGroupAccessor.getAllPartitionGroups();
            if (partitionGroupRecords != null && partitionGroupRecords.size() > 0) {
                for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
                    if (dbAndPartitionGroupId.containsKey(partitionGroupRecord.phy_db)) {
                        dbAndPartitionGroupId.get(partitionGroupRecord.phy_db).add(partitionGroupRecord.id);
                    } else {
                        List<Long> partGroupIds = new ArrayList<>();
                        partGroupIds.add(partitionGroupRecord.id);
                        dbAndPartitionGroupId.put(partitionGroupRecord.phy_db, partGroupIds);
                    }
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return dbAndPartitionGroupId;
    }

    public static PartitionGroupRecord getPartitionGroupById(Long pgId) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(conn);
            return partitionGroupAccessor.getPartitionGroupById(pgId);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static List<PartitionGroupRecord> getOutDatePartitionGroupsByTgId(Long tableGroupId) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(conn);
            return partitionGroupAccessor.getOutDatedPartitionGroupsByTableGroupIdFromDelta(tableGroupId);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
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
        if (tableGroupRecords != null && tableGroupRecords.size() > 0 && tableGroupRecords.get(0).manual_create == 0) {
            tableGroupAccessor.deleteTableGroupsById(schemaName, tableGroupRecords.get(0).id);
            partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupRecords.get(0).id, false);
            isDelete = true;
        }
        return isDelete;
    }

    public static List<PartitionGroupRecord> getAllUnVisiablePartitionGroupByGroupId(Long tableGroupId) {
        List<PartitionGroupRecord> partitionGroupRecords = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(conn);

            partitionGroupRecords =
                partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupId, true);

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return partitionGroupRecords;
    }

    public static List<PartitionGroupRecord> getPartitionGroupsByGroupId(Long tableGroupId) {
        List<PartitionGroupRecord> partitionGroupRecords = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            partitionGroupAccessor.setConnection(conn);

            partitionGroupRecords =
                partitionGroupAccessor.getPartitionGroupsByTableGroupId(tableGroupId, false);

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return partitionGroupRecords;
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
