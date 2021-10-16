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

package com.alibaba.polardbx.executor.balancer.policy;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.ActionLockResource;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroup;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroups;
import com.alibaba.polardbx.executor.balancer.action.ActionMovePartition;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Move partitions between storage node if un-balanced.
 *
 * @author moyi
 * @since 2021/03
 */
public class PolicyDataBalance implements BalancePolicy {

    private static final Logger LOG = LoggerFactory.getLogger(PolicyDataBalance.class);

    private static final double UNDER_LOAD_RATIO = 0.9;
    private static final double OVER_LOAD_RATIO = 1.1;

    @Override
    public String name() {
        return SqlRebalance.POLICY_DATA_BALANCE;
    }

    @Override
    public List<BalanceAction> applyToShardingDb(ExecutionContext ec,
                                                 BalanceOptions options,
                                                 BalanceStats stats,
                                                 String schema) {
        List<ActionMoveGroup> actions = new ArrayList<>();

        // only apply to partitioning database
        if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            return Lists.newArrayList();
        }

        List<GroupStats.GroupsOfStorage> groupList = stats.getGroups();
        if (CollectionUtils.isEmpty(groupList)) {
            return Collections.emptyList();
        }

        long totalStorageCount = groupList.size();
        List<String> emptyStorage = groupList.stream()
            .filter(x -> x.groups.isEmpty())
            .filter(x -> isStorageReady(x.storageInst))
            .map(x -> x.storageInst)
            .collect(Collectors.toList());
        long emptyCount = emptyStorage.size();
        if (emptyCount == 0 || emptyCount == totalStorageCount) {
            return Lists.newArrayList();
        }
        long totalGroups = groupList.stream().mapToInt(x -> x.groups.size()).sum();
        double newAverage = totalGroups * 1.0 / totalStorageCount;

        List<String> sourceGroups = new ArrayList<>();
        for (GroupStats.GroupsOfStorage storage : groupList) {
            if (storage.groups.size() > newAverage) {
                int toMove = (int) Math.ceil(storage.groups.size() - newAverage);
                List<GroupDetailInfoExRecord> toMoveGroup = storage.groups.subList(0, toMove);

                toMoveGroup.forEach(g -> sourceGroups.add(g.groupName));
            }
        }

        // move source groups in round-robin strategy
        int targetStorageIndex = 0;
        for (String group : sourceGroups) {
            String targetStorage = emptyStorage.get(targetStorageIndex);
            List<String> sourceGroup = Arrays.asList(group);
            ActionMoveGroup moveGroup = new ActionMoveGroup(schema, sourceGroup, targetStorage, options.debug);
            actions.add(moveGroup);

            targetStorageIndex = (targetStorageIndex + 1) % (int) emptyCount;
        }

        if (CollectionUtils.isEmpty(actions)) {
            return Lists.newArrayList();
        }
        String name = ActionUtils.genRebalanceResourceName(SqlRebalance.RebalanceTarget.DATABASE, schema);
        ActionLockResource lock = new ActionLockResource(schema, name);

        return Arrays.asList(lock, new ActionMoveGroups(schema, actions));
    }

    @Override
    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
        if (pgList.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);
        Map<String, StorageInstHaContext> storageMap = StorageHaManager.getInstance().getStorageHaCtxCache();

        List<GroupOfPartitions> groupsOfPartitions = pgList.stream()
            .collect(Collectors.groupingBy(this::groupByGroup))
            .entrySet().stream()
            .map(x -> new GroupOfPartitions(x.getKey(), x.getValue()))
            .collect(Collectors.toList());
        // consider empty groups
        List<GroupOfPartitions> emptyGroups = new ArrayList<>();
        for (String groupName : stats.getAllGroups()) {
            boolean existed = groupsOfPartitions.stream().anyMatch(x -> x.groupKey.equalsIgnoreCase(groupName));
            if (!existed) {
                emptyGroups.add(GroupOfPartitions.createEmpty(groupName));
            }
        }
        groupsOfPartitions.addAll(emptyGroups);

        List<BalanceAction> actions = new ArrayList<>();
        // balance steps:
        // 1. sort groups by total disk size
        // 2. pick-up under-load and over-load group, which is defined by load-threshold
        // 3. try to move partitions from under-load group to over-load groups
        groupsOfPartitions.sort((x, y) -> (int) (x.getTotalDiskSize() - y.getTotalDiskSize()));
        long totalDiskSize = groupsOfPartitions.stream().mapToLong(GroupOfPartitions::getTotalDiskSize).sum();
        long avgDiskSize = totalDiskSize / groupsOfPartitions.size();
        if (totalDiskSize == 0 || avgDiskSize == 0) {
            return actions;
        }
        long underLoadDiskSize = (long) (avgDiskSize * UNDER_LOAD_RATIO);
        long overLoadDiskSize = (long) (avgDiskSize * OVER_LOAD_RATIO);
        int start = 0, end = groupsOfPartitions.size() - 1;
        while (start < end && actions.size() < options.maxActions) {
            GroupOfPartitions underLoadGroup = groupsOfPartitions.get(start);
            GroupOfPartitions overLoadGroup = groupsOfPartitions.get(end);

            // not under-load anymore
            if (underLoadGroup.getCurrentDiskSize() >= underLoadDiskSize) {
                start++;
                continue;
            }

            // not an READY storage instance
            GroupDetailInfoRecord groupRecord = groupDetail.get(underLoadGroup.groupKey);
            if (groupRecord == null) {
                start++;
                continue;
            }
            StorageInstHaContext storage = storageMap.get(groupRecord.getStorageInstId());
            if (storage == null || !storage.isAllReplicaReady()) {
                start++;
                LOG.warn("Not an available storage instance: " + storage);
                continue;
            }

            if (overLoadGroup.getCurrentDiskSize() <= overLoadDiskSize) {
                end--;
                continue;
            }
            // move a partition from over-load to under-load
            PartitionGroupStat moved = overLoadGroup.pickupMoveOut();
            if (moved == null) {
                end--;
                continue;
            }
            // if make the situation reversed, consider it as trivial
            if (underLoadGroup.getCurrentDiskSize() + moved.getTotalDiskSize() >
                overLoadGroup.getCurrentDiskSize()) {
                end--;
                continue;
            }
            overLoadGroup.moveOutPartition(moved);
            underLoadGroup.moveInPartition(moved);

            ActionMovePartition action =
                ActionMovePartition.createMoveToGroup(schemaName, moved.getFirstPartition(), underLoadGroup.groupKey);
            actions.add(action);
        }

        if (!actions.isEmpty()) {
            LOG.info("DataBalance move partition for data balance: " + actions);
        }
        return actions;
    }

    protected boolean isStorageReady(String storageInst) {
        Map<String, StorageInstHaContext> storageStatusMap = StorageHaManager.getInstance().getStorageHaCtxCache();

        return Optional.ofNullable(storageStatusMap.get(storageInst))
            .map(StorageInstHaContext::isAllReplicaReady)
            .orElse(false);
    }

    private Map<String, GroupDetailInfoRecord> getGroupDetails(String schema) {
        GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            accessor.setConnection(conn);
            List<GroupDetailInfoRecord> records =
                accessor.getGroupDetailInfoByInstIdAndDbName(InstIdUtil.getInstId(), schema);
            return records.stream().collect(Collectors.toMap(GroupDetailInfoRecord::getGroupName, x -> x));
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private String groupByGroup(PartitionGroupStat p) {
        return p.getFirstPartition().getLocation().getGroupKey();
    }

    /**
     * All partitions in a physical group of a storage-node
     */
    static class GroupOfPartitions {
        public String groupKey;
        public List<PartitionGroupStat> partitions;

        private final long totalDiskSize;
        public List<PartitionGroupStat> moveInPartitions;
        public List<PartitionGroupStat> moveOutPartitions;

        public GroupOfPartitions(String groupKey, List<PartitionGroupStat> parts) {
            this.groupKey = groupKey;
            this.partitions = new ArrayList<>(parts);
            this.partitions.sort(Comparator.comparingLong(PartitionGroupStat::getTotalDiskSize));
            this.totalDiskSize = this.partitions.stream().mapToLong((PartitionGroupStat::getTotalDiskSize)).sum();
            this.moveInPartitions = new ArrayList<>();
            this.moveOutPartitions = new ArrayList<>();
        }

        public static GroupOfPartitions createEmpty(String groupKey) {
            return new GroupOfPartitions(groupKey, Collections.emptyList());
        }

        /**
         * Pickup a partition to move out.
         * The partitions is sorted by disk-size increasingly, so we pickup small partitions as priority
         */
        public PartitionGroupStat pickupMoveOut() {
            for (PartitionGroupStat p : partitions) {
                if (!moveOutPartitions.contains(p)) {
                    return p;
                }
            }
            return null;
        }

        public void moveInPartition(PartitionGroupStat p) {
            this.moveInPartitions.add(p);
        }

        public void moveOutPartition(PartitionGroupStat p) {
            this.moveOutPartitions.add(p);
        }

        public long getTotalDiskSize() {
            return this.totalDiskSize;
        }

        public long getMoveInDiskSize() {
            return this.moveInPartitions.stream().mapToLong(x -> x.getTotalDiskSize()).sum();
        }

        public long getMoveOutDiskSize() {
            return this.moveOutPartitions.stream().mapToLong(x -> x.getTotalDiskSize()).sum();
        }

        /**
         * Original disk size plus moved partitions
         */
        public long getCurrentDiskSize() {
            return getTotalDiskSize() + getMoveInDiskSize() - getMoveOutDiskSize();
        }

        public long getNumPartitions() {
            return this.partitions.size();
        }
    }

}
