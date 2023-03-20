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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.balancer.BalanceOptions;
import com.alibaba.polardbx.executor.balancer.action.ActionDrainDatabase;
import com.alibaba.polardbx.executor.balancer.action.ActionDropBroadcastTable;
import com.alibaba.polardbx.executor.balancer.action.ActionLockResource;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroup;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroups;
import com.alibaba.polardbx.executor.balancer.action.ActionMovePartition;
import com.alibaba.polardbx.executor.balancer.action.ActionMovePartitions;
import com.alibaba.polardbx.executor.balancer.action.ActionTaskAdapter;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.action.DropPhysicalDbTask;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CleanRemovedDbGroupMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CleanRemovedDbLocalityMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DrainCDCTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DrainNodeValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropDbGroupHideMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseReleaseXLockTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateGroupInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateNodeStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TopologySyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TopologySyncThenReleaseXLockTask;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.locality.LocalityDetailInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.GmsNodeManager.GmsNode;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Policy that drain out a data-node
 * 1. change status to NOT_READY, prevent create database on it in the future
 * 2. move out all groups, through ActionMoveGroup
 * 3. drain CDC binlog
 * 4. change CN & DN status to REMOVED
 *
 * @author moyi
 * @since 2021/05
 */
public class PolicyDrainNode implements BalancePolicy {

    private final static Logger LOG = SQLRecorderLogger.ddlLogger;
    private List<BalanceAction> actions;

    @Override
    public String name() {
        return SqlRebalance.POLICY_DRAIN_NODE;
    }

    /**
     * Apply to multiple database
     */
    @Override
    public List<BalanceAction> applyToMultiDb(ExecutionContext ec,
                                              Map<String, BalanceStats> stats,
                                              BalanceOptions options,
                                              List<String> schemaNameList) {
        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        drainNodeInfo.validate();

        List<String> dnInstIdList = drainNodeInfo.getDnInstIdList();
        List<String> cnIpPortList = new ArrayList<>();
        cnIpPortList.addAll(drainNodeInfo.getCnROIpPortList());
        cnIpPortList.addAll(drainNodeInfo.getCnRWIpPortList());
        LOG.info(String.format("apply drain_node policy: schemas=%s options=%s \nstats=%s",
            schemaNameList, options, stats));

        boolean ignoreRollback = true;
        BalanceAction updateStatusNotReady =
            new ActionTaskAdapter(
                DefaultDbSchema.NAME,
                new UpdateNodeStatusTask(
                    DefaultDbSchema.NAME,
                    new ArrayList<>(dnInstIdList),
                    Collections.emptyList(),
                    StorageInfoRecord.STORAGE_STATUS_READY,
                    StorageInfoRecord.STORAGE_STATUS_NOT_READY,
                    ignoreRollback)
            );

        BalanceAction updateStatusRemoved =
            new ActionTaskAdapter(
                DefaultDbSchema.NAME,
                new UpdateNodeStatusTask(
                    DefaultDbSchema.NAME,
                    new ArrayList<>(dnInstIdList),
                    new ArrayList<>(cnIpPortList),
                    StorageInfoRecord.STORAGE_STATUS_NOT_READY,
                    StorageInfoRecord.STORAGE_STATUS_REMOVED,
                    ignoreRollback));

        BalanceAction drainCDC =
            new ActionTaskAdapter(DefaultDbSchema.NAME, new DrainCDCTask(DefaultDbSchema.NAME, dnInstIdList));

        String resName = ActionUtils.genRebalanceClusterName();
        ActionLockResource lock = new ActionLockResource(
            DefaultDbSchema.NAME,
            com.google.common.collect.Sets.newHashSet(resName));

        List<BalanceAction> moveDataActions = new ArrayList<>();
        moveDataActions.add(lock);
        moveDataActions.add(updateStatusNotReady);

        SqlRebalance node = new SqlRebalance(SqlParserPos.ZERO);
        node.setRebalanceDatabase();
        node.setPolicy(options.policy);
        node.setDrainNode(options.drainNode);
        node.setLogicalDdl(false);
        node.setAsync(options.async);
        node.setDebug(options.debug);
        node.setExplain(options.explain);
        node.setMaxActions(options.maxActions);
        node.setMaxPartitionSize((int) options.maxPartitionSize);

        for (String schema : schemaNameList) {
            moveDataActions.add(new ActionDrainDatabase(schema, options.drainNode, node.toString(), stats.get(schema)));
        }

        moveDataActions.add(drainCDC);
        moveDataActions.add(updateStatusRemoved);
        return moveDataActions;
    }

    protected void doValidate(DrainNodeInfo drainNodeInfo) {
        drainNodeInfo.validate();
    }

    protected void doValidate(DrainNodeInfo drainNodeInfo, Boolean validateDeleteable) {
        drainNodeInfo.validate(validateDeleteable);
    }

    /**
     * Drain-out groups of a storage instance
     */
    @Override
    public List<BalanceAction> applyToShardingDb(ExecutionContext ec,
                                                 BalanceOptions options,
                                                 BalanceStats stats,
                                                 String schemaName) {
        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        doValidate(drainNodeInfo);

        List<PolicyDrainNode.DnDiskInfo> dnDiskInfo = DnDiskInfo.parseToList(options.diskInfo);
        List<GroupStats.GroupsOfStorage> groupList = stats.getGroups();
        if (CollectionUtils.isEmpty(groupList)) {
            return Collections.emptyList();
        }
        Map<String, Long> groupDataSizeMap = groupList.stream()
            .flatMap(x -> x.getGroupDataSizeMap().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue()));

        List<MoveInDn> moveInDnList;
        if (!dnDiskInfo.isEmpty()) {
            // choose dn list from disk_info
            moveInDnList = dnDiskInfo.stream()
                .filter(x -> !drainNodeInfo.containsDnInst(x.getInstance()))
                .map(PolicyDrainNode.MoveInDn::new)
                .collect(Collectors.toList());
        } else {
            // choose dn list from metadb
            moveInDnList = groupList
                .stream()
                .map(x -> x.storageInst)
                .distinct()
                .filter(x -> !drainNodeInfo.containsDnInst(x))
                .map(PolicyDrainNode.MoveInDn::new)
                .collect(Collectors.toList());
        }
        if (moveInDnList.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "no available data-node to move in");
        }
        long totalFreeSpace = moveInDnList.stream().mapToLong(x -> x.getDnDiskInfo().getFreeSpaceByte()).sum();

        List<GroupDetailInfoExRecord> moveOutGroups = groupList.stream()
            .filter(x -> drainNodeInfo.containsDnInst(x.storageInst))
            .flatMap(x -> x.groups.stream())
            .collect(Collectors.toList());

        List<ActionMoveGroup> actions = new ArrayList<>();

        // move groups to target data nodes
        int moveInDnIndex = -1;
        for (GroupDetailInfoExRecord group : moveOutGroups) {
            String groupName = group.groupName;
            long dataSize = groupDataSizeMap.get(groupName);

            // find a dn to move in
            ActionMoveGroup action = null;
            for (int i = 0; i < moveInDnList.size(); i++) {
                moveInDnIndex = (moveInDnIndex + 1) % moveInDnList.size();
                PolicyDrainNode.MoveInDn candidate = moveInDnList.get(moveInDnIndex);
                if (candidate.moveInGroup(schemaName, groupName, dataSize)) {
                    action = new ActionMoveGroup(schemaName, Arrays.asList(groupName),
                        candidate.getDnDiskInfo().getInstance(), options.debug, stats);
                    break;
                }
            }
            if (action == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format("not enough free space(%s) for drain node", totalFreeSpace));
            }
            actions.add(action);
        }

        if (CollectionUtils.isEmpty(actions)) {
            return Lists.newArrayList();
        }

        // lock
        final String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        final String schemaXLock = schemaName;
        ActionLockResource lock =
            new ActionLockResource(schemaName, com.google.common.collect.Sets.newHashSet(name, schemaXLock));

        MoveDatabaseReleaseXLockTask
            moveDatabaseReleaseXLockTask = new MoveDatabaseReleaseXLockTask(schemaName, schemaXLock);
        ActionTaskAdapter moveDatabaseXLockTaskAction = new ActionTaskAdapter(schemaName, moveDatabaseReleaseXLockTask);

        return Arrays.asList(lock, new ActionMoveGroups(schemaName, actions), moveDatabaseXLockTaskAction);
    }

    private List<MoveInDn> prepareMoveInDns(DrainNodeInfo drainNodeInfo, List<DnDiskInfo> dnDiskInfo,
                                            String schemaName) {
        List<MoveInDn> moveInDnList;
        List<GroupDetailInfoExRecord> detailInfoExRecords = TableGroupLocation.getOrderedGroupList(schemaName);
        Set<String> storageIds =
            detailInfoExRecords.stream().map(o -> o.storageInstId.toLowerCase()).collect(Collectors.toSet());
        if (!dnDiskInfo.isEmpty()) {
            // choose dn list from disk_info
            moveInDnList = dnDiskInfo.stream()
                .filter(x -> !drainNodeInfo.containsDnInst(x.getInstance()))
                .map(MoveInDn::new)
                .collect(Collectors.toList());
            for (MoveInDn moveInDn : moveInDnList) {
                if (!storageIds.contains(moveInDn.getDnDiskInfo().getInstance().toLowerCase())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        String.format("dn: [%s] is not available for schema: [%s]",
                            moveInDn.getDnDiskInfo().getInstance(),
                            schemaName));
                }
            }
        } else {
            // choose dn list from metadb
            moveInDnList =
                StorageHaManager.getInstance().getMasterStorageList()
                    .stream()
                    .filter(StorageInstHaContext::isAllReplicaReady)
                    .map(StorageInstHaContext::getStorageMasterInstId)
                    .filter(x -> !drainNodeInfo.containsDnInst(x))
                    .distinct()
                    .map(MoveInDn::new)
                    .collect(Collectors.toList());

            moveInDnList.removeIf(o -> !storageIds.contains(o.getDnDiskInfo().getInstance().toLowerCase()));
        }
        if (moveInDnList.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "no available data-node to move in");
        }
        return moveInDnList;
    }

    /**
     * Drain-out all partitions of a database
     * 0: Make the to-removed groups in `BEFORE_REMOVE` status, (To avoid broadcast table access and new-created table locate)
     * 1. Move out all partitions in this database
     * 2. Drop broadcast table in the storage instance(drop corresponding partition-group)
     * 3. Drop groups
     * 3.0 Make the to-removed groups in `REMOVING` status
     * 3.1 sync all cn to refresh topology and destroy the to-removed group datasource;
     * 3.2 drop phy db for all the to-removed groups;
     * 3.3 clean the meta info for all the to-removed groups.
     */
    @Override
    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        List<BalanceAction> actions = new ArrayList<>();

        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        doValidate(drainNodeInfo);

        Map<String, String> groupToInst = Maps.newHashMap();
        List<PolicyDrainNode.DnDiskInfo> dnDiskInfo = DnDiskInfo.parseToList(options.diskInfo);

        Map<String, GroupDetailInfoRecord> groupMap = PolicyUtils.getGroupDetails(schemaName);
        List<String> removedGroups =
            groupMap.values().stream()
                .filter(x -> drainNodeInfo.containsDnInst(x.getStorageInstId()))
                .map(GroupDetailInfoRecord::getGroupName)
                .collect(Collectors.toList());

        // query locality and reset locality involving drain node
        // move partitions
        List<LocalityDetailInfoRecord> localityDetailInfoRecords = PolicyUtils.getLocalityDetails(schemaName);
        List<LocalityDetailInfoRecord> toRemoveLocalityItems =
            localityDetailInfoRecords.stream().filter(x -> (x.getLocality() != null && !x.getLocality().isEmpty()))
                .filter(x -> drainNodeInfo.intersectDnInstList(LocalityDesc.parse(x.getLocality()).getDnList()))
                .collect(Collectors.toList());

        List<TableGroupConfig> tableGroupConfigList = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);
        Set<String> toSyncTableGroupSet = new HashSet<>();
        for (TableGroupConfig tableGroupConfig : tableGroupConfigList) {
            if (drainNodeInfo.intersectDnInstList(tableGroupConfig.getLocalityDesc().getDnList())) {
                toSyncTableGroupSet.add(tableGroupConfig.getTableGroupRecord().tg_name);
            } else {
                for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
                    if (drainNodeInfo.intersectDnInstList(
                        LocalityDesc.parse(partitionGroupRecord.getLocality()).getDnList())) {
                        toSyncTableGroupSet.add(tableGroupConfig.getTableGroupRecord().tg_name);
                        break;
                    }
                }
            }
        }

        List<String> toSyncTableGroup = new ArrayList<>(toSyncTableGroupSet);
        List<List<String>> toSyncTables = tableGroupConfigList.stream()
            .filter(tableGroupConfig -> toSyncTableGroup.contains(tableGroupConfig.getTableGroupRecord().tg_name))
            .map(tableGroupConfig -> tableGroupConfig.getTables().stream().map(o -> o.getTableName())
                .collect(Collectors.toList()))
            .collect(Collectors.toList());
        List<ActionMovePartition> actionMovePartitions = new ArrayList<>();
        Map<String, List<ActionMovePartition>> movePartitionActions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Set<PartitionStat> moved = Sets.newHashSet();
        List<MoveInDn> availableInstList = prepareMoveInDns(drainNodeInfo, dnDiskInfo, schemaName);
        List<Pair<PartitionStat, String>> movePartitions = new ArrayList<>();

        for (PartitionStat partition : stats.getPartitionStats()) {
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

            if (drainNodeInfo.containsDnInst(inst) && !moved.contains(partition)) {
                String targetInst = chooseTargetInst(availableInstList);
                moved.add(partition);
                movePartitions.add(Pair.of(partition, targetInst));
            }
        }
        GeneralUtil.emptyIfNull(movePartitions).stream()
            .collect(Collectors.groupingBy(Pair::getValue, Collectors.mapping(Pair::getKey, Collectors.toList())))
            .forEach((toInst, partList) -> {
                actionMovePartitions.addAll(ActionMovePartition.createMoveToInsts(schemaName, partList, toInst, stats));
            });

        GeneralUtil.emptyIfNull(actionMovePartitions).stream()
            .forEach(o -> movePartitionActions.computeIfAbsent(o.getTableGroupName(), m -> new ArrayList<>()).add(o));
        // remove broadcast tables
        ActionDropBroadcastTable dropBroadcastTable =
            new ActionDropBroadcastTable(schemaName, drainNodeInfo.getDnInstIdList());

        // Hide all the to-removed groups and notify all cn nodes to clean the to-removed group data sources
        List<String> toRemoveGroupNames = new ArrayList<>();
        toRemoveGroupNames.addAll(removedGroups);
        DropDbGroupHideMetaTask hideDbGroupMetaTask = new DropDbGroupHideMetaTask(schemaName, toRemoveGroupNames);
        ActionTaskAdapter hideToRemovedGroupMetaAction = new ActionTaskAdapter(schemaName, hideDbGroupMetaTask);

        TopologySyncTask topologySyncTask = new TopologySyncTask(schemaName);
        ActionTaskAdapter syncNewTopologyAction = new ActionTaskAdapter(schemaName, topologySyncTask);

        // remove physical db for all the to-removed groups
        List<BalanceAction> dropPhyDbActions = removedGroups.stream()
            .map(x -> new ActionTaskAdapter(schemaName, new DropPhysicalDbTask(schemaName, x)))
            .collect(Collectors.toList());

        // remove groups on the storage
        CleanRemovedDbGroupMetaTask cleanRemovedDbGroupMetaTask =
            new CleanRemovedDbGroupMetaTask(schemaName, toRemoveGroupNames);
        ActionTaskAdapter cleanRemovedDbGroupMetaAction =
            new ActionTaskAdapter(schemaName, cleanRemovedDbGroupMetaTask);

        CleanRemovedDbLocalityMetaTask cleanRemovedDbLocalityMetaTask =
            new CleanRemovedDbLocalityMetaTask(schemaName, toRemoveLocalityItems);
        ActionTaskAdapter cleanRemovedDbLocalityMetaAction =
            new ActionTaskAdapter(schemaName, cleanRemovedDbLocalityMetaTask);

        List<ActionTaskAdapter> syncTableGroupsAction = new ArrayList<>();
        for (int i = 0; i < toSyncTableGroup.size(); i++) {
            TableGroupSyncTask tableGroupSyncTask = new TableGroupSyncTask(schemaName, toSyncTableGroup.get(i));
            TablesSyncTask tablesSyncTask = new TablesSyncTask(schemaName, toSyncTables.get(i));
            syncTableGroupsAction.add(new ActionTaskAdapter(schemaName, tablesSyncTask));
            syncTableGroupsAction.add(new ActionTaskAdapter(schemaName, tableGroupSyncTask));
        }

        List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
        GeneralUtil.emptyIfNull(stats.getTableGroupStats()).stream()
            .forEach(o -> {
                if (o.getTableGroupConfig() != null && o.getTableGroupConfig().getTables() != null) {
                    o.getTableGroupConfig().getTables().clear();
                }
                tableGroupConfigs.add(o.getTableGroupConfig());
            });
        DrainNodeValidateTask drainNodeValidateTask = new DrainNodeValidateTask(schemaName, tableGroupConfigs);

        ActionTaskAdapter drainNodeValidateTaskAdapter = new ActionTaskAdapter(schemaName, drainNodeValidateTask);

        // lock
        final String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        final String schemaXLock = schemaName;
        ActionLockResource lock =
            new ActionLockResource(schemaName, com.google.common.collect.Sets.newHashSet(name, schemaXLock));

        // Modify status of group
        UpdateGroupInfoTask task = new UpdateGroupInfoTask(schemaName,
            removedGroups,
            DbGroupInfoRecord.GROUP_TYPE_NORMAL,
            DbGroupInfoRecord.GROUP_TYPE_BEFORE_REMOVE);
        ActionTaskAdapter actionUpdateGroupStatus = new ActionTaskAdapter(schemaName, task);

        TopologySyncThenReleaseXLockTask topologySyncThenReleaseXLockTask =
            new TopologySyncThenReleaseXLockTask(schemaName, schemaXLock);
        ActionTaskAdapter actionTopologySyncThenReleaseXLockTask =
            new ActionTaskAdapter(schemaName, topologySyncThenReleaseXLockTask);
        // combine actions
        actions.add(lock);
        actions.add(drainNodeValidateTaskAdapter);
        actions.add(actionUpdateGroupStatus);
        actions.add(actionTopologySyncThenReleaseXLockTask);
        actions.add(cleanRemovedDbLocalityMetaAction);
        actions.addAll(syncTableGroupsAction);
        actions.add(new ActionMovePartitions(schemaName, movePartitionActions));
        actions.add(dropBroadcastTable);
        actions.add(hideToRemovedGroupMetaAction);
        actions.add(syncNewTopologyAction);
        actions.addAll(dropPhyDbActions);
        actions.add(cleanRemovedDbGroupMetaAction);

        return actions;
    }

    private String chooseTargetInst(List<MoveInDn> availableInstList) {
        int total = availableInstList.size();
        assert (total > 0);
        int x = ThreadLocalRandom.current().nextInt(total);
        return availableInstList.get(x).getDnDiskInfo().getInstance();
    }

    private List<String> prepareAvailableInsts(DrainNodeInfo drainNodeInfo) {
        return StorageHaManager.getInstance().getStorageHaCtxCache().values().stream()
            .filter(StorageInstHaContext::isDNMaster)
            .map(StorageInstHaContext::getStorageInstId)
            .filter(x -> !drainNodeInfo.containsDnInst(x))
            .collect(Collectors.toList());
    }

    private String getStorageInstOfGroup(Map<String, String> cache, String schema, String groupName) {
        String inst = cache.get(groupName);
        if (inst == null) {
            inst = DbTopologyManager.getStorageInstIdByGroupName(schema, groupName);
            cache.put(groupName, inst);
        }
        return inst;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DrainNodeInfo {

        @JSONField(name = "dn")
        private List<String> dnInstIdList = new ArrayList<>();

        @JSONField(name = "cn_rw")
        private List<String> cnRWIpPortList = new ArrayList<>();

        @JSONField(name = "cn_ro")
        private List<String> cnROIpPortList = new ArrayList<>();

        public DrainNodeInfo(List<String> dnInstIdList) {
            this.dnInstIdList = dnInstIdList;
        }

        /**
         * Parse string to DrainNodeInfo
         * Two kinds of format are supported:
         * 1. Dn list: 'dn1,dn2,dn3'
         * 2. Complete info: '{dn: ["dn1", dn2"], cn_ro: ["cn1_ipport"], cn_rw: ["cn1_ipport]}'
         */
        public static DrainNodeInfo parse(String drainNodeInfo) {
            DrainNodeInfo result;

            drainNodeInfo = StringUtils.trim(drainNodeInfo);
            if (TStringUtil.isBlank(drainNodeInfo)) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "empty drain_node str");
            }

            if (drainNodeInfo.startsWith("{")) {
                result = JSON.parseObject(drainNodeInfo, DrainNodeInfo.class);
            } else {
                String[] slice = StringUtils.split(drainNodeInfo, ",");
                result = new DrainNodeInfo();
                result.dnInstIdList = Arrays.asList(slice);
            }

            return result;
        }

        public boolean containsDnInst(String dnInst) {
            return dnInstIdList.contains(dnInst);
        }

        public boolean intersectDnInstList(List<String> dnInstList) {
            return !Collections.disjoint(dnInstList, dnInstIdList);
        }

        public void validate() {
            validate(true);
        }

        /**
         * Validate the drain_node info
         * 1. Whether all dn instance are existed
         * 2. Whether all dn instance could be removed
         */
        public void validate(Boolean validateDeleteable) {
            if (CollectionUtils.isEmpty(dnInstIdList)) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "empty dn list");
            }

            Map<String, StorageInstHaContext> allStorage =
                StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();

            if (validateDeleteable) {
                Set<String> nonDeletableStorage;
                try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                    nonDeletableStorage = DbTopologyManager.getNonDeletableStorageInst(metaDbConn);
                } catch (Throwable ex) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        "failed to fetch the non deletable storage list");
                }

                for (String dnInst : dnInstIdList) {
                    StorageInstHaContext storage = allStorage.get(dnInst);
                    if (storage == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                            "storage not found: " + dnInst);
                    }

                    // check if dn is allowed to be deleted
                    boolean deletable =
                        DbTopologyManager.checkStorageInstDeletable(nonDeletableStorage, dnInst,
                            storage.getStorageKind());
                    if (!deletable) {
                        throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                            "storage not deletable: " + dnInst);
                    }
                }
            }

            // validate servers
            final GmsNodeManager gnm = GmsNodeManager.getInstance();
            Set<String> masterNodes = nodeToHostPort(gnm.getMasterNodes());
            Set<String> readonlyNodes = nodeToHostPort(gnm.getReadOnlyNodes());
            for (String rw : cnRWIpPortList) {
                if (!masterNodes.contains(rw)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "server not found: " + rw);
                }
            }
            for (String ro : cnROIpPortList) {
                if (!readonlyNodes.contains(ro)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "server not found: " + ro);
                }
            }
        }

        private Set<String> nodeToHostPort(List<GmsNode> serverList) {
            return serverList.stream().map(GmsNode::getHostPort).collect(Collectors.toSet());
        }
    }

    /**
     * Information about disk
     */
    @Data
    static public class DnDiskInfo {

        @JSONField(name = "node")
        private String instance;

        @JSONField(name = "free_space_byte")
        private long freeSpaceByte;

        @JSONCreator
        public DnDiskInfo(String instance, long diskFreeSpaceByte) {
            this.instance = instance;
            this.freeSpaceByte = diskFreeSpaceByte;
        }

        public static Map<String, DnDiskInfo> parseToMap(String diskInfo) {
            return parseToList(diskInfo).stream().collect(Collectors.toMap(x -> x.instance, x -> x));
        }

        public static List<DnDiskInfo> parseToList(String diskInfo) {
            if (TStringUtil.isBlank(diskInfo)) {
                return Collections.emptyList();
            }
            try {
                return JSON.parseArray(diskInfo, DnDiskInfo.class);
            } catch (JSONException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    "invalid disk_info: " + e.getMessage(), e);
            }
        }
    }

    @Data
    static public class MoveInDn {
        DnDiskInfo dnDiskInfo;

        // lists of schema.group
        List<Pair<String, String>> moveInGroups = new ArrayList<>();

        public MoveInDn(DnDiskInfo diskInfo) {
            this.dnDiskInfo = diskInfo;
        }

        /**
         * Create a data node which has infinite free-space
         */
        public MoveInDn(String dnInstance) {
            this.dnDiskInfo = new DnDiskInfo(dnInstance, -1);
        }

        /**
         * Move in a group if available
         */
        boolean moveInGroup(String schema, String groupName, long dataSize) {
            if (dnDiskInfo.freeSpaceByte == -1) {
                this.moveInGroups.add(Pair.of(schema, groupName));
                return true;
            }
            if (dnDiskInfo.freeSpaceByte >= dataSize) {
                this.moveInGroups.add(Pair.of(schema, groupName));
                dnDiskInfo.freeSpaceByte -= dataSize;
                return true;
            }
            return false;
        }
    }

}
