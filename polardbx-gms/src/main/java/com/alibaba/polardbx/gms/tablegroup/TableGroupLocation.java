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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Compute location for various partition operations:
 * 1. create partition-table
 * 2. split-partition
 * 3. partition-balance
 * ......
 *
 * @author moyi
 * @since 2021/04
 */
public class TableGroupLocation {

    /**
     * Choose a group to place new partition, which should has the least physical table
     *
     * @param schema the schema of the new partition
     */
    public static GroupDetailInfoExRecord chooseGroupForNewPartition(String schema) {
        return getOrderedGroupList(schema).get(0);
    }

    public static GroupAllocator buildGroupAllocator(String schema, LocalityDesc localityDesc) {
        if (localityDesc.holdEmptyDnList()) {
            return new GroupAllocator(getOrderedGroupList(schema));
        } else {
            return buildGroupAllocatorByLocality(schema, localityDesc);
        }
    }

    public static GroupAllocator buildGroupAllocatorOfPartitionByLocality(String schema, LocalityDesc localityDesc) {
        List<GroupDetailInfoExRecord> groups = getOrderedGroupList(schema);
        groups = groups.stream()
            .filter(x -> localityDesc.matchStorageInstance(x.getStorageInstId()))
            .collect(Collectors.toList());
        return new GroupAllocator(groups);

    }

    public static GroupAllocator buildGroupAllocatorByGroup(String schema, List<GroupDetailInfoExRecord> groups) {
        return new GroupAllocator(groups);
    }

    public static GroupAllocator buildGroupAllocatorByGroup(String schema, List<GroupDetailInfoExRecord> groups,
                                                            int part_num) {
        return new GroupAllocator(groups, part_num);
    }

    public static GroupAllocator buildGroupAllocatorByLocality(String schema, LocalityDesc localityDesc) {
        List<GroupDetailInfoExRecord> groups = getOrderedGroupList(schema);
        groups = groups.stream()
            .filter(x -> localityDesc.matchStorageInstance(x.getStorageInstId()))
            .collect(Collectors.toList());
        return new GroupAllocator(groups);
    }

    /**
     * Get list of storage-group order by physical-table count on the group
     */

    public static List<GroupDetailInfoExRecord> getFullOrderedGroupList(String logicalDbName) {
        if (ConfigDataMode.isMock() || ConfigDataMode.isFastMock()) {
            return TableGroupUtils.mockTheOrderedLocation(logicalDbName);
        }
        List<GroupDetailInfoExRecord> storageGroupList;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            storageGroupList = queryMetaDbGroupList(conn, instId);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return storageGroupList;
    }

    public static List<GroupDetailInfoExRecord> getOrderedGroupList(String logicalDbName) {
        return getOrderedGroupList(logicalDbName, false);
    }

    public static List<Pair<GroupDetailInfoExRecord, TableGroupRecord>> getOrderedGroupListForSingleTable(
        String logicalDbName,
        LocalityDesc dbLocalityDesc,
        boolean includeToBeRemoveGroup) {
        List<GroupDetailInfoExRecord> storageGroupList;
        List<PartitionGroupExtRecord> partitionGroupList;

        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        final Map<Long, Long> tableCountMap;
        List<TableGroupRecord> singleTableGroups = new ArrayList<>();
        // query metadb for physical group and all partition-groups
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            tableGroupAccessor.setConnection(conn);
            singleTableGroups = tableGroupAccessor.getAllTableGroups(logicalDbName).stream()
                .filter(tableGroupRecord -> tableGroupRecord.isSingleTableGroup()
                    && tableGroupRecord.withBalanceSingleTableLocality())
                .collect(Collectors.toList());
            List<Long> singleTableGroupIds = singleTableGroups.stream()
                .map(tableGroupRecord -> tableGroupRecord.getId()).collect(Collectors.toList());
            partitionGroupList = queryMetaDbPartitionGroupList(logicalDbName, conn);
            storageGroupList = queryMetaDbGroupList(conn, instId).stream()
                .filter(storageGroup -> storageGroup.dbName.equals(logicalDbName))
                .filter(r -> (includeToBeRemoveGroup || DbGroupInfoManager.isNormalGroup(r.dbName,
                    r.groupName))) //exclude GROUP_TYPE_BEFORE_REMOVE if includeToBeRemoveGroup=false
                .filter(o -> dbLocalityDesc.matchStorageInstance(o.storageInstId))
                .collect(Collectors.toList());
            if (singleTableGroupIds.size() > 0) {
                tableCountMap = tableGroupAccessor.getTableCountPerGroup(singleTableGroupIds);
            } else {
                tableCountMap = new HashMap<>();
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        Map<Long, TableGroupRecord> tableGroupMap =
            singleTableGroups.stream().collect(Collectors.toMap(TableGroupRecord::getId, x -> {
                return x;
            }));
        Map<String, Long> tableGroupPhyDbMap = partitionGroupList.stream()
            .filter(partitionGroupExtRecord -> tableCountMap.containsKey(partitionGroupExtRecord.getTg_id()))
            .collect(Collectors.toMap(PartitionGroupRecord::getPhy_db, PartitionGroupRecord::getTg_id));
        // group partitions by storage-instance
        List<Pair<GroupDetailInfoExRecord, TableGroupRecord>> storageGroupTableGroupMap = new ArrayList<>();
        Map<GroupDetailInfoExRecord, Long> groupCountMap = new HashMap<>();
        for (GroupDetailInfoExRecord storageGroup : storageGroupList) {
            Long tableGroupId = tableGroupPhyDbMap.get(storageGroup.getPhyDbName());
            if (tableGroupId == null) {
                storageGroupTableGroupMap.add(new Pair(storageGroup, null));
                groupCountMap.put(storageGroup, 0L);
            } else {
                storageGroupTableGroupMap.add(new Pair(storageGroup, tableGroupMap.get(tableGroupId)));
                groupCountMap.put(storageGroup, tableCountMap.getOrDefault(tableGroupId, 0L));
            }
        }
        storageGroupTableGroupMap =
            storageGroupTableGroupMap.stream().sorted(Comparator.comparingLong(x -> groupCountMap.get(x.getKey())))
                .collect(Collectors.toList());
        return storageGroupTableGroupMap;
    }

    public static List<GroupDetailInfoExRecord> getOrderedGroupList(String logicalDbName,
                                                                    boolean includeToBeRemoveGroup) {
        if (ConfigDataMode.isMock() || ConfigDataMode.isFastMock()) {
            return TableGroupUtils.mockTheOrderedLocation(logicalDbName);
        }

        List<GroupDetailInfoExRecord> storageGroupList;
        List<PartitionGroupExtRecord> partitionGroupList;

        // query metadb for physical group and all partition-groups
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            storageGroupList = queryMetaDbGroupList(conn, instId);
            partitionGroupList = queryMetaDbPartitionGroupList(logicalDbName, conn);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        // group partitions by storage-instance
        Map<String, String> physicalDbToInstance =
            storageGroupList.stream().collect(Collectors.toMap(x -> x.phyDbName, x -> x.storageInstId));
        Map<String, Integer> instanceTableCount = new HashMap<>();
        physicalDbToInstance.values().forEach(x -> instanceTableCount.put(x, 0));
        for (PartitionGroupExtRecord partitionGroup : partitionGroupList) {
            String instance = physicalDbToInstance.get(partitionGroup.phy_db);
            instanceTableCount.compute(instance,
                (k, v) -> v == null ? partitionGroup.phy_tb_cnt.intValue() : v + partitionGroup.phy_tb_cnt.intValue());
        }

        // sort physical-groups according to physical-table count
        return storageGroupList.stream()
            .filter(r -> r.dbName.equalsIgnoreCase(logicalDbName))
            .filter(r -> (includeToBeRemoveGroup || DbGroupInfoManager.isNormalGroup(r.dbName,
                r.groupName))) //exclude GROUP_TYPE_BEFORE_REMOVE if includeToBeRemoveGroup=false
            .sorted(Comparator.comparingInt(x -> instanceTableCount.get(x.storageInstId)))
            .collect(Collectors.toList());
    }

    private static List<PartitionGroupExtRecord> queryMetaDbPartitionGroupList(String tableSchema, Connection conn) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(conn);
        return partitionGroupAccessor.getGetPhysicalTbCntPerPg(tableSchema);
    }

    private static List<GroupDetailInfoExRecord> queryMetaDbGroupList(Connection conn, String instId) {
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(conn);
        return groupDetailInfoAccessor.getCompletedGroupInfosByInstIdForPartitionTables(instId,
            DbGroupInfoRecord.GROUP_TYPE_NORMAL);
    }

    /**
     * Allocate group from a list of groups, use round-robin strategy
     */
    public static class GroupAllocator {
        private final List<GroupDetailInfoExRecord> groupList;
        private int nextToAllocate;

        private int partNum;
        private int avgPartNum;

        private int allocatedCount;

        GroupAllocator(List<GroupDetailInfoExRecord> groupList) {
            this.groupList = groupList;
            this.nextToAllocate = 0;
            this.partNum = 0;
        }

        GroupAllocator(List<GroupDetailInfoExRecord> groupList, int partNum) {
            this.groupList = groupList;
            this.nextToAllocate = 0;
            this.partNum = partNum;
            this.avgPartNum = partNum / groupList.size();
            this.allocatedCount = 0;
        }

        /**
         * Allocate a group
         */
        public String allocate() {
            String result = this.groupList.get(nextToAllocate).getGroupName();
            if (partNum == 0) {
                this.nextToAllocate = (this.nextToAllocate + 1) % this.groupList.size();
            } else {
                //Sequential count
                allocatedCount += 1;
                if (allocatedCount % avgPartNum == 0) {
                    this.nextToAllocate = (this.nextToAllocate + 1) % this.groupList.size();
                }
            }
            return result;
        }
    }

}
