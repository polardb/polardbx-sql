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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.Balancer;
import com.alibaba.polardbx.executor.balancer.action.ActionInitPartitionDb;
import com.alibaba.polardbx.executor.balancer.action.ActionLockResource;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroup;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroups;
import com.alibaba.polardbx.executor.balancer.action.ActionMovePartition;
import com.alibaba.polardbx.executor.balancer.action.ActionMovePartitions;
import com.alibaba.polardbx.executor.balancer.action.ActionTaskAdapter;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DrainNodeOfTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseReleaseXLockTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TopologySyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TopologySyncThenReleaseXLockTask;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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
    public List<BalanceAction> applyToMultiDb(ExecutionContext ec,
                                              Map<String, BalanceStats> stats,
                                              BalanceOptions options,
                                              List<String> schemaNameList) {
        List<BalanceAction> result = new ArrayList<>();

        // Initialize new storage instance if needed
        List<DbInfoRecord> dbRecords = DbTopologyManager.getNewPartDbInfoFromMetaDb();
        boolean refreshTopology = false;
        if (CollectionUtils.isNotEmpty(dbRecords)) {
            ActionInitPartitionDb actionInit = new ActionInitPartitionDb(ec.getSchemaName());
            result.add(actionInit);
            refreshTopology = true;
        }

        // Balance each database
        for (String schema : schemaNameList) {
            for (BalanceAction action : applyToDb(ec, stats.get(schema), options, schema)) {
                if (!action.getName().equals(ActionInitPartitionDb.getActionName())) {
                    result.add(action);
                } else if (!refreshTopology) {
                    result.add(action);
                    refreshTopology = true;
                }
            }
        }

        return result;
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
        List<BucketOfGroups> buckets = groupList.stream().map(BucketOfGroups::new).collect(Collectors.toList());

        long totalStorageCount = buckets.size();
        List<String> emptyStorage = groupList.stream()
            .filter(x -> x.groups.isEmpty())
            .filter(x -> isStorageReady(x.storageInst))
            .map(x -> x.storageInst)
            .collect(Collectors.toList());
        if ((long) emptyStorage.size() == totalStorageCount) {
            return Lists.newArrayList();
        }
        long totalGroups = groupList.stream().mapToInt(x -> x.groups.size()).sum();
        double newAverage = totalGroups * 1.0 / totalStorageCount;

        // sort by group count descending, move from left to right
        Collections.sort(buckets);
        int left = 0, right = buckets.size() - 1;
        while (left < right) {
            BucketOfGroups lb = buckets.get(left);
            BucketOfGroups rb = buckets.get(right);

            if (lb.currentGroupCount() <= newAverage) {
                left++;
                continue;
            }
            if (rb.currentGroupCount() >= newAverage) {
                right--;
                continue;
            }
            if (lb.currentGroupCount() - 1 < rb.currentGroupCount() + 1) {
                left++;
                continue;
            }
            String g = lb.moveOut();
            if (g == null) {
                left++;
            }
            rb.moveIn(g);

            ActionMoveGroup moveGroup = new ActionMoveGroup(schema, Arrays.asList(g),
                rb.originGroups.storageInst, options.debug, stats);
            actions.add(moveGroup);
        }
        // shuffle actions to avoid make any storage node overload
        Collections.shuffle(actions);

        if (CollectionUtils.isEmpty(actions)) {
            return Lists.newArrayList();
        }
//
        final String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schema);
        final String schemaXLock = schema;
        ActionLockResource lock =
            new ActionLockResource(schema, com.google.common.collect.Sets.newHashSet(name, schemaXLock));

        MoveDatabaseReleaseXLockTask
            moveDatabaseReleaseXLockTask = new MoveDatabaseReleaseXLockTask(schema, schemaXLock);
        ActionTaskAdapter moveDatabaseXLockTaskAction = new ActionTaskAdapter(schema, moveDatabaseReleaseXLockTask);

        return Arrays.asList(lock, new ActionMoveGroups(schema, actions), moveDatabaseXLockTaskAction);
    }

    /**
     * 1. Create group on empty storage-node
     * 2. Move partition to balance data
     * 3. Replicate broadcast-table
     */
    @Override
    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        DdlHelper.getServerConfigManager().executeBackgroundSql("refresh topology", schemaName, null);
        final BalanceStats balanceStats = Balancer.collectBalanceStatsOfDatabase(schemaName);

        List<BalanceAction> actions = new ArrayList<>();

        String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        ActionLockResource lock = new ActionLockResource(schemaName, Sets.newHashSet(name));
        actions.add(lock);

        actions.add(new ActionInitPartitionDb(schemaName));

        List<PartitionGroupStat> pgList = balanceStats.getPartitionGroupStats();
        if (pgList.isEmpty()) {
            return actions;
        }
        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);
        Map<String, StorageInstHaContext> storageMap = StorageHaManager.getInstance().getStorageHaCtxCache();

        List<BucketOfPartitions> groupsOfPartitions = pgList.stream()
            .collect(Collectors.groupingBy(this::groupByGroup))
            .entrySet().stream()
            .map(x -> new BucketOfPartitions(x.getKey(), x.getValue()))
            .collect(Collectors.toList());
        // consider empty groups
        List<BucketOfPartitions> emptyGroups = new ArrayList<>();
        for (String groupName : balanceStats.getAllGroups()) {
            boolean existed = groupsOfPartitions.stream().anyMatch(x -> x.groupKey.equalsIgnoreCase(groupName));
            if (!existed) {
                emptyGroups.add(BucketOfPartitions.createEmpty(groupName));
            }
        }
        groupsOfPartitions.addAll(emptyGroups);

        // balance data:
        // 1. sort groups by total disk size
        // 2. pick-up under-load and over-load group, which is defined by load-threshold
        // 3. try to move partitions from under-load group to over-load groups
        Collections.sort(groupsOfPartitions, new Comparator<BucketOfPartitions>() {
            @Override
            public int compare(BucketOfPartitions o1, BucketOfPartitions o2) {
                if (o1.getTotalDiskSize() > o2.getTotalDiskSize()) {
                    return 1;
                } else if (o1.getTotalDiskSize() < o2.getTotalDiskSize()) {
                    return -1;
                }
                return 0;
            }
        });
        long totalDiskSize = groupsOfPartitions.stream().mapToLong(BucketOfPartitions::getTotalDiskSize).sum();
        long avgDiskSize = totalDiskSize / groupsOfPartitions.size();
        if (totalDiskSize == 0 || avgDiskSize == 0) {
            return actions;
        }
        long underLoadDiskSize = (long) (avgDiskSize * UNDER_LOAD_RATIO);
        long overLoadDiskSize = (long) (avgDiskSize * OVER_LOAD_RATIO);
        int start = 0, end = groupsOfPartitions.size() - 1;
        List<Pair<PartitionStat, String>> moves = new ArrayList<>();
        while (start < end && actions.size() < options.maxActions) {
            BucketOfPartitions underLoadGroup = groupsOfPartitions.get(start);
            BucketOfPartitions overLoadGroup = groupsOfPartitions.get(end);

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
            PartitionGroupStat moved = overLoadGroup.pickupMoveOut(false);
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

            moves.add(Pair.of(moved.getFirstPartition(), underLoadGroup.groupKey));
        }

        Map<String, List<ActionMovePartition>> movePartitionActions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        GeneralUtil.emptyIfNull(moves).stream()
            .collect(Collectors.groupingBy(Pair::getValue, Collectors.mapping(Pair::getKey, Collectors.toList())))
            .forEach((toGroup, partitions) -> {
                for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schemaName, partitions, toGroup, balanceStats)) {
                    if ((actions.size() + movePartitionActions.size()) >= options.maxActions) {
                        break;
                    }
                    movePartitionActions.computeIfAbsent(act.getTableGroupName(), o->new ArrayList<>()).add(act);
                }
            });

        actions.add(new ActionMovePartitions(schemaName, movePartitionActions));
        if (!actions.isEmpty()) {
            LOG.info("DataBalance move partition for data balance: " + actions);
        }
        return actions;
    }


    /**
     * 1. Create group on empty storage-node
     * 2. Move partition to balance data
     * 3. Replicate broadcast-table
     */
//    @Override
//    public List<BalanceAction> applyToTableGroup(ExecutionContext ec,
//                                                 BalanceOptions options,
//                                                 BalanceStats stats,
//                                                 String schemaName,
//                                                 String tableGroupName) {
//        LocalityManager lm = LocalityManager.getInstance();
//        //group
//        List<String> allowedGroupNameList = lm.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName).stream().map(groupInfo->groupInfo.getGroupName()).collect(Collectors.toList());
//
//        DdlHelper.getServerConfigManager().executeBackgroundSql("refresh topology", schemaName, null);
//        stats = Balancer.collectBalanceStatsOfTableGroup(schemaName, tableGroupName);
//
//        List<BalanceAction> actions = new ArrayList<>();
//
//        String name = ActionUtils.genRebalanceResourceName(SqlRebalance.RebalanceTarget.DATABASE, schemaName);
//        ActionLockResource lock = new ActionLockResource(schemaName, Sets.newHashSet(name));
//        actions.add(lock);
//
//        actions.add(new ActionInitPartitionDb(schemaName));
//
//        // structurize pg list into pg map
//
//        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
//        if (pgList.isEmpty()) {
//            return actions;
//        }
//        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);
//        Map<String, StorageInstHaContext> storageMap = StorageHaManager.getInstance().getStorageHaCtxCache();
//
//        List<BucketOfPartitions> groupsOfPartitions = pgList.stream()
//                .collect(Collectors.groupingBy(this::groupByGroup))
//                .entrySet().stream()
//                .map(x -> new BucketOfPartitions(x.getKey(), x.getValue()))
//                .collect(Collectors.toList());
//        // consider empty groups
//        List<BucketOfPartitions> emptyGroups = new ArrayList<>();
//        for (String groupName : stats.getAllGroups()) {
//            boolean existed = groupsOfPartitions.stream().anyMatch(x -> x.groupKey.equalsIgnoreCase(groupName));
//            if (!existed) {
//                emptyGroups.add(BucketOfPartitions.createEmpty(groupName));
//            }
//        }
//        emptyGroups = emptyGroups.stream().filter(bucket -> allowedGroupNameList.contains(bucket.groupKey)).collect(Collectors.toList());
//        groupsOfPartitions.addAll(emptyGroups);
//
//        // balance data:
//        // 1. sort groups by total disk size
//        // 2. pick-up under-load and over-load group, which is defined by load-threshold
//        // 3. try to move partitions from under-load group to over-load groups
//        Collections.sort(groupsOfPartitions, new Comparator<BucketOfPartitions>() {
//            @Override
//            public int compare(BucketOfPartitions o1, BucketOfPartitions o2) {
//                if (o1.getTotalDiskSize() > o2.getTotalDiskSize()) {
//                    return 1;
//                } else if (o1.getTotalDiskSize() < o2.getTotalDiskSize()) {
//                    return -1;
//                }
//                return 0;
//            }
//        });
//        long totalDiskSize = groupsOfPartitions.stream().mapToLong(BucketOfPartitions::getTotalDiskSize).sum();
//        long avgDiskSize = totalDiskSize / groupsOfPartitions.size();
//        if (totalDiskSize == 0 || avgDiskSize == 0) {
//            return actions;
//        }
//        long underLoadDiskSize = (long) (avgDiskSize * UNDER_LOAD_RATIO);
//        long overLoadDiskSize = (long) (avgDiskSize * OVER_LOAD_RATIO);
//        int start = 0, end = groupsOfPartitions.size() - 1;
//        List<Pair<PartitionStat, String>> moves = new ArrayList<>();
//        while (start < end && actions.size() < options.maxActions) {
//            BucketOfPartitions underLoadGroup = groupsOfPartitions.get(start);
//            BucketOfPartitions overLoadGroup = groupsOfPartitions.get(end);
//
//            // not under-load anymore
//            if (underLoadGroup.getCurrentDiskSize() >= underLoadDiskSize) {
//                start++;
//                continue;
//            }
//
//            // not an READY storage instance
//            GroupDetailInfoRecord groupRecord = groupDetail.get(underLoadGroup.groupKey);
//            if (groupRecord == null) {
//                start++;
//                continue;
//            }
//            StorageInstHaContext storage = storageMap.get(groupRecord.getStorageInstId());
//            if (storage == null || !storage.isAllReplicaReady()) {
//                start++;
//                LOG.warn("Not an available storage instance: " + storage);
//                continue;
//            }
//
//            if (overLoadGroup.getCurrentDiskSize() <= overLoadDiskSize) {
//                end--;
//                continue;
//            }
//            // move a partition from over-load to under-load
//            PartitionGroupStat moved = overLoadGroup.pickupMoveOut(true);
//            if (moved == null) {
//                end--;
//                continue;
//            }
//            // if make the situation reversed, consider it as trivial
//            if (underLoadGroup.getCurrentDiskSize() + moved.getTotalDiskSize() >
//                    overLoadGroup.getCurrentDiskSize()) {
//                end--;
//                continue;
//            }
//            overLoadGroup.moveOutPartition(moved);
//            underLoadGroup.moveInPartition(moved);
//
//            moves.add(Pair.of(moved.getFirstPartition(), underLoadGroup.groupKey));
//        }
//
//        GeneralUtil.emptyIfNull(moves).stream()
//                .collect(Collectors.groupingBy(Pair::getValue, Collectors.mapping(Pair::getKey, Collectors.toList())))
//                .forEach((toGroup, partitions) -> {
//                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schemaName, partitions,
//                            toGroup)) {
//                        if (actions.size() >= options.maxActions) {
//                            break;
//                        }
//                        actions.add(act);
//                    }
//                });
//
//        if (!actions.isEmpty()) {
//            LOG.info("DataBalance move partition for data balance: " + actions);
//        }
//
//        return actions;
//    }
    //take table group into consideration
    private List<PolicyDrainNode.MoveInDn> prepareMoveInDnsForTablegroup(List<PolicyDrainNode.DnDiskInfo> dnDiskInfo,
                                                                         String schemaName, String tableGroup) {
        List<PolicyDrainNode.MoveInDn> moveInDnList;
        List<GroupDetailInfoExRecord> detailInfoExRecords = LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroup);
        Set<String> storageIds =
            detailInfoExRecords.stream().map(o -> o.storageInstId.toLowerCase()).collect(Collectors.toSet());
        if (!dnDiskInfo.isEmpty()) {
            // choose dn list from disk_info
            moveInDnList = dnDiskInfo.stream()
                .map(PolicyDrainNode.MoveInDn::new)
                .filter(dn->storageIds.contains(dn.getDnDiskInfo().getInstance().toLowerCase()))
                .collect(Collectors.toList());
        } else {
            // choose dn list from metadb
            moveInDnList =
                StorageHaManager.getInstance().getMasterStorageList()
                    .stream()
                    .filter(StorageInstHaContext::isAllReplicaReady)
                    .map(StorageInstHaContext::getStorageMasterInstId)
                    .distinct()
                    .map(PolicyDrainNode.MoveInDn::new)
                    .collect(Collectors.toList());
            moveInDnList.removeIf(o -> !storageIds.contains(o.getDnDiskInfo().getInstance().toLowerCase()));
        }
        if (moveInDnList.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format("no available data-node to move in for table group %s", tableGroup));
        }
        return moveInDnList;
    }


    private Map<String, List<PolicyDrainNode.MoveInDn>> prepareMoveInDnsForPartitionGroup(List<PolicyDrainNode.DnDiskInfo> dnDiskInfo,
                                                                                          String schemaName, String tableGroup) {
        Map<String, List<GroupDetailInfoExRecord>> detailInfoExRecords = LocalityInfoUtils.getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroup);
        Map<String, List<PolicyDrainNode.MoveInDn>> moveInDnMap = new HashMap<>();
        for(String partition:detailInfoExRecords.keySet()) {
            Set<String> storageIds = detailInfoExRecords.get(partition).stream().map(o -> o.storageInstId.toLowerCase()).collect(Collectors.toSet());
            List<PolicyDrainNode.MoveInDn> moveInDnList;
            if (!dnDiskInfo.isEmpty()) {
                // choose dn list from disk_info
                moveInDnList = dnDiskInfo.stream()
                    .map(PolicyDrainNode.MoveInDn::new)
                    .filter(dn->storageIds.contains(dn.getDnDiskInfo().getInstance().toLowerCase()))
                    .collect(Collectors.toList());
            } else {
                // choose dn list from metadb
                moveInDnList =
                    StorageHaManager.getInstance().getMasterStorageList()
                        .stream()
                        .filter(StorageInstHaContext::isAllReplicaReady)
                        .map(StorageInstHaContext::getStorageMasterInstId)
                        .distinct()
                        .map(PolicyDrainNode.MoveInDn::new)
                        .collect(Collectors.toList());
                moveInDnList.removeIf(o -> !storageIds.contains(o.getDnDiskInfo().getInstance().toLowerCase()));
            }
            if (moveInDnList.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format("no available data-node to move in for partition group %s", partition));
            }
            moveInDnMap.put(partition, moveInDnList);
        }
        return moveInDnMap;
    }

    private String chooseTargetInst(List<PolicyDrainNode.MoveInDn> availableInstList) {
        int total = availableInstList.size();
        assert (total > 0);
        int x = ThreadLocalRandom.current().nextInt(total);
        return availableInstList.get(x).getDnDiskInfo().getInstance();
    }

    private String getStorageInstOfGroup(Map<String, String> cache, String schema, String groupName) {
        String inst = cache.get(groupName);
        if (inst == null) {
            inst = DbTopologyManager.getStorageInstIdByGroupName(schema, groupName);
            cache.put(groupName, inst);
        }
        return inst;
    }

    @Override
    public List<BalanceAction> applyToTableGroup(ExecutionContext ec,
                                                 BalanceOptions options,
                                                 BalanceStats stats,
                                                 String schemaName,
                                                 String tableGroupName) {
        List<BalanceAction> actions = new ArrayList<>();

        Map<String, String> groupToInst = Maps.newHashMap();
        List<PolicyDrainNode.DnDiskInfo> dnDiskInfo = PolicyDrainNode.DnDiskInfo.parseToList(options.diskInfo);

        // query locality and reset locality involving drain node
        // move partitions
        List<BalanceAction> actionMovePartitions = new ArrayList<>();
        Set<PartitionStat> moved = org.glassfish.jersey.internal.guava.Sets.newHashSet();
        List<PolicyDrainNode.MoveInDn> availableInstList = prepareMoveInDnsForTablegroup(dnDiskInfo, schemaName, tableGroupName);
        Map<String, List<PolicyDrainNode.MoveInDn>> availableInstListForPartitionGroup = prepareMoveInDnsForPartitionGroup(dnDiskInfo, schemaName, tableGroupName);
        List<Pair<PartitionStat, String>> movePartitions = new ArrayList<>();

        List<PartitionStat> partitionStats = stats.getPartitionStats().stream().filter(partitionStat ->
            partitionStat.getTableGroupName().equals(tableGroupName)).collect(Collectors.toList());

        for (PartitionStat partition : partitionStats){
            int tgType = partition.getTableGroupRecord().getTg_type();
            if (tgType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
                continue;
            }

            String inst = getStorageInstOfGroup(
                groupToInst,
                partition.getSchema(),
                partition.getLocation().getGroupKey());

            LOG.info(String.format("PartitionStat:%s groupKey=%s, dn=%s \n",
                partition, partition.getLocation().getGroupKey(), inst));

            if (!moved.contains(partition) && partition.getTableGroupName().equals(tableGroupName)) {
                String targetInst = "";
                targetInst = chooseTargetInst(availableInstListForPartitionGroup.getOrDefault(partition.getPartitionName(), availableInstList));
                moved.add(partition);
                movePartitions.add(Pair.of(partition, targetInst));
            }
        }
        GeneralUtil.emptyIfNull(movePartitions).stream()
            .collect(Collectors.groupingBy(Pair::getValue, Collectors.mapping(Pair::getKey, Collectors.toList())))
            .forEach((toInst, partList) -> {
                actionMovePartitions.addAll(ActionMovePartition.createMoveToInsts(schemaName, partList, toInst, stats));
            });

        TopologySyncTask topologySyncTask = new TopologySyncTask(schemaName);
        ActionTaskAdapter syncNewTopologyAction = new ActionTaskAdapter(schemaName, topologySyncTask);

        TableGroupSyncTask tableGroupSyncTask = new TableGroupSyncTask(schemaName, tableGroupName);
        ActionTaskAdapter syncTableGroupAction = new ActionTaskAdapter(schemaName, tableGroupSyncTask);


        List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
        GeneralUtil.emptyIfNull(stats.getTableGroupStats()).stream()
            .forEach(o -> {
                tableGroupConfigs.add(o.getTableGroupConfig());
            });
        DrainNodeOfTableGroupValidateTask drainNodeValidateTask = new DrainNodeOfTableGroupValidateTask(schemaName, tableGroupConfigs, tableGroupName);
        ActionTaskAdapter drainNodeValidateTaskAdapter = new ActionTaskAdapter(schemaName, drainNodeValidateTask);

        // lock
        final String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        final String schemaXLock = schemaName;
        ActionLockResource lock =
            new ActionLockResource(schemaName, com.google.common.collect.Sets.newHashSet(name, schemaXLock));

        TopologySyncThenReleaseXLockTask topologySyncThenReleaseXLockTask = new TopologySyncThenReleaseXLockTask(schemaName, schemaXLock);
        ActionTaskAdapter actionTopologySyncThenReleaseXLockTask = new ActionTaskAdapter(schemaName, topologySyncThenReleaseXLockTask);
        // combine actions
        actions.add(lock);
        actions.add(drainNodeValidateTaskAdapter);
        actions.add(actionTopologySyncThenReleaseXLockTask);
        actions.addAll(actionMovePartitions);
        actions.add(syncTableGroupAction);
        actions.add(syncNewTopologyAction);
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

    @Data
    static class BucketOfGroups implements Comparable<BucketOfGroups> {
        GroupStats.GroupsOfStorage originGroups;
        List<String> movedInGroups;
        List<String> movedOutGroups;

        public BucketOfGroups(GroupStats.GroupsOfStorage originGroups) {
            this.originGroups = originGroups;
            this.movedInGroups = new ArrayList<>();
            this.movedOutGroups = new ArrayList<>();
        }

        public int currentGroupCount() {
            return originGroups.groups.size() + movedInGroups.size() - movedOutGroups.size();
        }

        public String moveOut() {
            for (GroupDetailInfoExRecord g : originGroups.groups) {
                if (!movedOutGroups.contains(g.groupName)) {
                    movedOutGroups.add(g.groupName);
                    return g.groupName;
                }
            }
            return null;
        }

        public void moveIn(String group) {
            if (movedInGroups.contains(group)) {
                throw new IllegalArgumentException("Group already exists: " + group);
            }
            movedInGroups.add(group);
        }

        /**
         * Compare group count and instance name in descending order
         */
        @Override
        public int compareTo(BucketOfGroups o) {
            if (originGroups.groups.size() != o.getOriginGroups().groups.size()) {
                return Integer.compare(o.getOriginGroups().groups.size(), originGroups.groups.size());
            }
            return o.originGroups.storageInst.compareTo(originGroups.storageInst);
        }
    }

    /**
     * All partitions in a physical group of a storage-node
     */
    static class BucketOfPartitions {
        public String groupKey;
        public List<PartitionGroupStat> partitions;

        private final long totalDiskSize;
        public List<PartitionGroupStat> moveInPartitions;
        public List<PartitionGroupStat> moveOutPartitions;

        public BucketOfPartitions(String groupKey, List<PartitionGroupStat> parts) {
            this.groupKey = groupKey;
            this.partitions = new ArrayList<>(parts);
            this.partitions.sort(Comparator.comparingLong(PartitionGroupStat::getTotalDiskSize));
            this.totalDiskSize = this.partitions.stream().mapToLong((PartitionGroupStat::getTotalDiskSize)).sum();
            this.moveInPartitions = new ArrayList<>();
            this.moveOutPartitions = new ArrayList<>();
        }

        public static BucketOfPartitions createEmpty(String groupKey) {
            return new BucketOfPartitions(groupKey, Collections.emptyList());
        }

        /**
         * Pickup a partition to move out.
         * The partitions is sorted by disk-size increasingly, so we pickup small partitions as priority
         */
        public PartitionGroupStat pickupMoveOut(Boolean allowLocalityTable) {
            for (PartitionGroupStat p : partitions) {
                int tgType = p.getFirstPartition().getTableGroupRecord().getTg_type();
                String tgLocality = p.getFirstPartition().getTableGroupRecord().getLocality();
                String pgLocality = p.getFirstPartition().getPartitionGroupRecord().getLocality();
                if (tgType != TableGroupRecord.TG_TYPE_PARTITION_TBL_TG) {
                    continue;
                }
                if(!allowLocalityTable && (!StringUtils.isEmpty(tgLocality) || !StringUtils.isEmpty(pgLocality))){
                    continue;
                }
                if(allowLocalityTable && !StringUtils.isEmpty(pgLocality)){
                    continue;
                }
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
