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
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
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

    public static GroupAllocator buildGroupAllocator(String schema) {
        return new GroupAllocator(getOrderedGroupList(schema));
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
    public static List<GroupDetailInfoExRecord> getOrderedGroupList(String logicalDbName) {
        if (ConfigDataMode.isMock() || ConfigDataMode.isFastMock()) {
            return TableGroupUtils.mockTheOrderedLocation(logicalDbName);
        }

        List<GroupDetailInfoExRecord> storageGroupList;
        List<PartitionGroupExtRecord> partitionGroupList;

        // query metadb for physical group and all partition-groups
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            String instId = InstIdUtil.getInstId();
            storageGroupList = queryMetaDbGroupList(conn, instId);
            partitionGroupList = queryMetaDbPartitionGroupList(conn);
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
            .sorted(Comparator.comparingInt(x -> instanceTableCount.get(x.storageInstId)))
            .collect(Collectors.toList());
    }

    private static List<PartitionGroupExtRecord> queryMetaDbPartitionGroupList(Connection conn) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(conn);
        return partitionGroupAccessor.getGetPhysicalTbCntPerPg();
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

        GroupAllocator(List<GroupDetailInfoExRecord> groupList) {
            this.groupList = groupList;
            this.nextToAllocate = 0;
        }

        /**
         * Allocate a group
         */
        public String allocate() {
            String result = this.groupList.get(nextToAllocate).getGroupName();
            this.nextToAllocate = (this.nextToAllocate + 1) % this.groupList.size();
            return result;
        }
    }

}
