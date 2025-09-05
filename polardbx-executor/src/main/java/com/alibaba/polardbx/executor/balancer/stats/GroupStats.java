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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Sets;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Retrieve stats of groups
 *
 * @author moyi
 * @since 2021/05
 */
public class GroupStats {

    /**
     * Get all groups of cluster which is sharding database
     *
     * @return groups, group-by schema, storage-instance, and sorted by group count
     */
    public static Map<String, List<GroupsOfStorage>> getAllGroups() {
        return getGroupsImpl((schema) -> true);

    }

    /**
     * Get groups of specified database
     */
    public static List<GroupsOfStorage> getGroupsOfDb(String schema) {
        Map<String, List<GroupsOfStorage>> m = getGroupsImpl(db -> db.equalsIgnoreCase(schema));
        return m.get(schema);
    }

    /**
     * Get groups of partitioning database
     *
     * @return groups a map which key is schema, value is group list
     */
    public static Map<String, List<GroupsOfStorage>> getGroupsOfPartition(String schema) {
        return getGroupsImpl(db -> DbInfoManager.getInstance().isNewPartitionDb(db) && db.equalsIgnoreCase(schema));
    }

    private static Map<String, List<GroupsOfStorage>> getGroupsImpl(Function<String, Boolean> dbFilter) {
        Map<String, List<GroupsOfStorage>> result = new HashMap<>();

        Set<String> storageNodeList = getAllDataNodes();
        List<GroupDetailInfoExRecord> records = getGroupRecords();

        records.stream()
            .filter(x -> !SystemDbHelper.isDBBuildIn(x.dbName))
            .collect(Collectors.groupingBy(x -> x.dbName))
            .forEach((schema, groups) -> {
                if (!dbFilter.apply(schema)) {
                    return;
                }

                // query data-size
                Map<String, Pair<Long, Long>> dataSizeMap = StatsUtils.queryDbGroupDataSize(schema, groups);

                // retrieve existed groups
                List<GroupsOfStorage> groupOfStorage =
                    groups.stream()
                        .filter(x -> !GroupInfoUtil.isSingleGroup(x.groupName))
                        .collect(Collectors.groupingBy(x -> x.storageInstId))
                        .entrySet().stream()
                        .map(x -> new GroupsOfStorage(x.getKey(), x.getValue(), dataSizeMap))
                        .collect(Collectors.toList());

                // consider empty storage nodes
                Set<String> existedStorage = groupOfStorage.stream()
                    .map(x -> x.storageInst).collect(Collectors.toSet());
                Sets.difference(storageNodeList, existedStorage).forEach(x -> {
                    groupOfStorage.add(GroupsOfStorage.empty(x));
                });

                result.put(schema, groupOfStorage);
            });

        return result;
    }

    /**
     * Get all data-nodes that ready for placing group.
     * <p>
     * In scale-out scenario, the newly-added data-node is written into storage_info at first.
     * After that, the HAManager will detect it through listener. Then, this data-node is
     * ready for placing group.
     */
    private static Set<String> getAllDataNodes() {
        final StorageHaManager shm = StorageHaManager.getInstance();

        return shm.getStorageHaCtxCache().values().stream()
            .filter(StorageInstHaContext::isDNMaster)
            .map(StorageInstHaContext::getStorageInstId)
            .collect(Collectors.toSet());
    }

    private static List<GroupDetailInfoExRecord> getGroupRecords() {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> records =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());

            return records;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException("Failed to access metadb", ex);
        }
    }

    /**
     * All groups on a storage node
     */
    public static class GroupsOfStorage {
        final public String storageInst;
        final public List<GroupDetailInfoExRecord> groups;

        /**
         * <GroupName, DataSize>
         */
        final public Map<String, Pair<Long, Long>> groupDataSizeMap;

        public GroupsOfStorage(String storageInst,
                               List<GroupDetailInfoExRecord> groups,
                               Map<String, Pair<Long, Long>> groupDataSizeMap) {
            this.storageInst = storageInst;
            this.groups = groups;
            this.groupDataSizeMap = new HashMap<>();
            for (GroupDetailInfoExRecord groupRecord : groups) {
                this.groupDataSizeMap.put(groupRecord.groupName, groupDataSizeMap.get(groupRecord.getGroupName()));
            }
        }

        public GroupsOfStorage(String storageInst, List<GroupDetailInfoExRecord> groups) {
            this(storageInst, groups, new HashMap<>());
        }

        public Map<String, Pair<Long, Long>> getGroupDataSizeMap() {
            return this.groupDataSizeMap;
        }

        public static GroupsOfStorage empty(String storageInst) {
            return new GroupsOfStorage(storageInst, new ArrayList<>());
        }

        @Override
        public String toString() {
            return "GroupsOfStorage{" +
                "storageInst='" + storageInst + '\'' +
                ", groups=" + groups +
                ", groupDataSizeMap=" + groupDataSizeMap +
                '}';
        }
    }

}
