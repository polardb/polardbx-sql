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
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
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
import com.alibaba.polardbx.executor.balancer.action.ActionMoveTablePartition;
import com.alibaba.polardbx.executor.balancer.action.ActionTaskAdapter;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.ActionWriteDataDistLog;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.action.DropPhysicalDbTask;
import com.alibaba.polardbx.executor.balancer.serial.DataDistInfo;
import com.alibaba.polardbx.executor.balancer.solver.MixedModel;
import com.alibaba.polardbx.executor.balancer.solver.Solution;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CleanRemovedDbGroupMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CleanRemovedDbLocalityMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CleanUpdateDbLocalityMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DrainCDCTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DrainNodeSuccessValidateTask;
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
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
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
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.REBALANCE_MAX_UNIT_SIZE;
import static com.alibaba.polardbx.executor.balancer.policy.PolicyPartitionBalance.MAX_TABLEGROUP_SOLVED_BY_LP;
import static com.alibaba.polardbx.executor.balancer.policy.PolicyUtils.getGroupDetails;

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
        LOG.info(String.format("apply drain_node policy: schemas=%s options=%s",
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

    /**
     * Apply to multiple tenant database
     */
    @Override
    public List<BalanceAction> applyToMultiTenantDb(ExecutionContext ec,
                                                    Map<String, BalanceStats> stats,
                                                    BalanceOptions options,
                                                    String storagePoolName,
                                                    List<String> schemaNameList) {
        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        drainNodeInfo.validate();

        List<String> dnInstIdList = drainNodeInfo.getDnInstIdList();
        List<String> cnIpPortList = new ArrayList<>();
        cnIpPortList.addAll(drainNodeInfo.getCnROIpPortList());
        cnIpPortList.addAll(drainNodeInfo.getCnRWIpPortList());
        LOG.info(String.format("apply drain_node policy: schemas=%s options=%s",
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

        String resName = ActionUtils.genRebalanceTenantResourceName(storagePoolName);
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

        List<BalanceAction> moveDataActionForBalance = schemaNameList.stream()
            .flatMap(schema -> applyToTenantDb(ec, stats.get(schema), options, storagePoolName, schema).stream())
            .collect(Collectors.toList());
        moveDataActions.addAll(moveDataActionForBalance);

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
        if (StoragePoolManager.getInstance().isTriggered()) {
            List<String> storageInsts =
                StoragePoolManager.getInstance().getStoragePoolInfo(StoragePoolManager.DEFAULT_STORAGE_POOL_NAME)
                    .getDnLists();
            groupList =
                groupList.stream()
                    .filter(o -> storageInsts.contains(o.storageInst) || drainNodeInfo.containsDnInst(o.storageInst))
                    .collect(Collectors.toList());
            dnDiskInfo =
                dnDiskInfo.stream()
                    .filter(o -> storageInsts.contains(o.instance) || drainNodeInfo.containsDnInst(o.instance))
                    .collect(Collectors.toList());
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
        Map<String, String> targetGroupDnMap = new HashMap<>();

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
            targetGroupDnMap.put(groupName, action.getTarget());
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

        DrainNodeSuccessValidateTask drainNodeSuccessValidateTask =
            new DrainNodeSuccessValidateTask(schemaName, targetGroupDnMap);
        ActionTaskAdapter drainNodeSuccessValidateTaskAction =
            new ActionTaskAdapter(schemaName, drainNodeSuccessValidateTask);

        return Arrays.asList(lock, new ActionMoveGroups(schemaName, actions), moveDatabaseXLockTaskAction,
            drainNodeSuccessValidateTaskAction);
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

    public List<BalanceAction> generateMoveSingleTableAction(String schemaName,
                                                             Map<String, List<PartitionStat>> singleTableMap,
                                                             Map<String, GroupDetailInfoRecord> groupDetail,
                                                             DrainNodeInfo drainNodeInfo,
                                                             BalanceStats stats,
                                                             BalanceOptions options,
                                                             ExecutionContext ec) {

        List<PartitionStat> toRebalancePartitionList =
            singleTableMap.values().stream().flatMap(o -> o.stream()).collect(Collectors.toList());

        List<BalanceAction> actions = new ArrayList<>();
        int N = toRebalancePartitionList.size();
        if (N <= 0) {
            return actions;
        }
        int M = groupDetail.size();
        int[] originalPlace = new int[N];
        int[] targetPlace = new int[N];
        double[] partitionSize = new double[N];

        Map<Integer, PartitionStat> toRebalancePartitionMap = new HashMap<>();
        List<String> groupNames = new ArrayList<>(groupDetail.keySet());
        List<String> storageInsts =
            groupNames.stream().map(o -> groupDetail.get(o).storageInstId).collect(Collectors.toList());
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap<>();
        Map<String, Integer> storageInstReverseMap = new HashMap();
        for (int i = 0; i < M; i++) {
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
            storageInstReverseMap.put(storageInsts.get(i), i);
        }
        int[] drainNodeIndexes =
            drainNodeInfo.getDnInstIdList().stream().mapToInt(o -> storageInstReverseMap.get(o)).toArray();

        for (int i = 0; i < N; i++) {
            PartitionStat partitionStat = toRebalancePartitionList.get(i);
            toRebalancePartitionMap.put(i, partitionStat);
            String groupKey = partitionStat.getLocation().getGroupKey();
            originalPlace[i] = groupDetailReverseMap.get(groupKey);
            partitionSize[i] = partitionStat.getDataRows();
            targetPlace[i] = originalPlace[i];
        }

        double originalMu = MixedModel.caculateBalanceFactor(M, N, originalPlace, partitionSize).getValue();
        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }
        Solution solution = null;
        Date startTime = new Date();
        solution = MixedModel.solveMovePartition(M, N, originalPlace, partitionSize, drainNodeIndexes);
        if (solution.withValidSolve) {
            Date endTime = new Date();
            Long costMillis = endTime.getTime() - startTime.getTime();
            String logInfo = String.format(
                "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                schemaName, "single_tablegroup", costMillis, solution.strategy, originalMu, solution.mu,
                Arrays.toString(solution.targetPlace));
            EventLogger.log(EventType.REBALANCE_INFO, logInfo);
            targetPlace = solution.targetPlace;
//            double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//            double targetFactor = caculateBalanceFactor(M, N, targetPlace, partitionSize);
//            if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                return actions;
//            }
            List<Pair<PartitionStat, String>> moves = new ArrayList<>();

            for (int i = 0; i < N; i++) {
                if (targetPlace[i] != originalPlace[i]) {
                    moves.add(Pair.of(toRebalancePartitionList.get(i), groupDetailMap.get(targetPlace[i])));
                }
            }
            moves.sort(Comparator.comparingLong(o -> -o.getKey().getPartitionDiskSize()));

            for (Pair<PartitionStat, String> move : moves) {
                String toGroup = move.getValue();
                List<PartitionStat> partitions = new ArrayList<>();
                partitions.add(move.getKey());
                ActionMoveTablePartition act =
                    ActionMoveTablePartition.createMoveToGroups(schemaName, partitions, toGroup, stats).get(0);
                actions.add(act);
            }

        }
        return actions;
    }

    @Override
    public List<BalanceAction> applyToTenantPartitionDb(ExecutionContext ec, BalanceOptions options, BalanceStats stats,
                                                        String storagePoolName, String schemaName) {
        List<BalanceAction> actions = new ArrayList<>();

        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        doValidate(drainNodeInfo);
        // 1. Get groupMap here from schemaName and storagePoolName.
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        List<String> storageInstIds = storagePoolManager.getStoragePoolInfo(storagePoolName).getDnLists();

        Map<String, GroupDetailInfoRecord> fullDbGroupMap = PolicyUtils.getGroupDetails(schemaName);
        List<String> removedGroups =
            fullDbGroupMap.values().stream().filter(x -> drainNodeInfo.containsDnInst(x.getStorageInstId()))
                .map(GroupDetailInfoRecord::getGroupName).collect(Collectors.toList());
        Map<String, GroupDetailInfoRecord> groupMap = fullDbGroupMap.keySet().stream()
            .filter(o -> removedGroups.contains(o) || storageInstIds.contains(fullDbGroupMap.get(o).storageInstId))
            .collect(Collectors.toMap(o -> o, o -> fullDbGroupMap.get(o)));

        Set<String> groupInstIds = groupMap.values().stream().map(o -> o.storageInstId).collect(Collectors.toSet());
        Set<String> invalidStorageInsts =
            drainNodeInfo.dnInstIdList.stream().filter(o -> !groupInstIds.contains(o)).collect(
                Collectors.toSet());
        drainNodeInfo.removeInvalidStorageInsts(invalidStorageInsts);
        if (drainNodeInfo.dnInstIdList.isEmpty()) {
            return actions;
        }

//        if(!invalidStorageInsts.isEmpty()){
//            String errMsg = String.format("drain node info contains invalid storage info '%s'", StringUtils.join(invalidStorageInsts, ",").toString());
//            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
//        }

        // 2.1 Fetch all the LocalityItem in database.
        List<LocalityDetailInfoRecord> localityDetailInfoRecords = PolicyUtils.getLocalityDetails(schemaName);
//        List<LocalityDetailInfoRecord> toMoveDatabaseObject =
//            // for partition with null locality, all we need is to process tablegroup.
//            // for partition with locality, all we need is to process partitiongroup.
//            // for tablegroup without locality, all we need is to process database.
//            // for tablegroup with locality, all we need is to process tablegroup.
//            localityDetailInfoRecords.stream().filter(x -> (x.getLocality() != null && !x.getLocality().isEmpty()))
//                .filter(x -> drainNodeInfo.intersectDnInstList(LocalityInfoUtils.parse(x.getLocality()).getDnList()))
//                .collect(Collectors.toList());

        // 2.2 Filter toRemoveLocalityItem by Interaction of drainNodeInfo and localityItem.
        List<LocalityDetailInfoRecord> toRemoveLocalityItems =
            localityDetailInfoRecords.stream().filter(x -> (x.getLocality() != null && !x.getLocality().isEmpty()))
                .filter(x -> (!LocalityInfoUtils.parse(x.getLocality()).hasStoragePoolDefinition()
                    && drainNodeInfo.intersectDnInstList(LocalityInfoUtils.parse(x.getLocality()).getDnList())))
                .collect(Collectors.toList());

        // 2.3 Filter toSyncTableGroup by Interaction of drainNodeInfo and tgLocality, drainNodeInfo and pgLocality.
        List<TableGroupConfig> tableGroupConfigList = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);
        Set<String> toSyncTableGroupSet = new HashSet<>();
        for (TableGroupConfig tableGroupConfig : tableGroupConfigList) {
            LocalityDesc tgLocality = tableGroupConfig.getLocalityDesc();
            if (drainNodeInfo.intersectDnInstList(tgLocality.getDnList()) && !tgLocality.hasStoragePoolDefinition()) {
                toSyncTableGroupSet.add(tableGroupConfig.getTableGroupRecord().tg_name);
            } else {
                for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
                    LocalityDesc pgLocality = LocalityInfoUtils.parse(partitionGroupRecord.getLocality());
                    if (drainNodeInfo.intersectDnInstList(pgLocality.getDnList())
                        && !pgLocality.hasStoragePoolDefinition()) {
                        toSyncTableGroupSet.add(tableGroupConfig.getTableGroupRecord().tg_name);
                        break;
                    }
                }
            }
        }

        // 2.4 Generate toSyncTableGroup and toSyncTable.
        List<String> toSyncTableGroup = new ArrayList<>(toSyncTableGroupSet);
        List<List<String>> toSyncTables = tableGroupConfigList.stream()
            .filter(tableGroupConfig -> toSyncTableGroup.contains(tableGroupConfig.getTableGroupRecord().tg_name)).map(
                tableGroupConfig -> tableGroupConfig.getTables()).collect(Collectors.toList());
//        List<ActionMovePartition> actionMovePartitions = new ArrayList<>();
        // prepare move action

        // 3.1 Collect all pgList and filter toMovePgList by intersection of storageInstIds and pgPlacement.
        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats().stream().filter(o -> {
            return groupMap.containsKey(o.getFirstPartition().getLocation().getGroupKey());
        }).collect(Collectors.toList());
        Map<String, List<PartitionGroupStat>> pgListGroupByTg = GeneralUtil.emptyIfNull(pgList).stream()
            .collect(Collectors.groupingBy(o -> o.getTgName(), Collectors.mapping(o -> o, Collectors.toList())));

        // 3.2 filter toMoveTgList by intersection of drainNodeInfo and pgPlacement.
        Map<String, List<PartitionGroupStat>> toMovePartitionTgs = new HashMap<>();
        for (String tgName : pgListGroupByTg.keySet()) {
            List<PartitionGroupStat> pgListOfTg = pgListGroupByTg.get(tgName);
            List<PartitionGroupStat> toMovePgListOfTg = pgListOfTg.stream().filter(o -> {
                String storageInstId = groupMap.get(o.getFirstPartition().getLocation().getGroupKey()).storageInstId;
                return drainNodeInfo.containsDnInst(storageInstId);
            }).collect(Collectors.toList());
            if (!toMovePgListOfTg.isEmpty()) {
                toMovePartitionTgs.put(tgName, toMovePgListOfTg);
            }
        }
        // 3.3 filter broadcastTg.
        Set<String> boadcastTgSets =
            tableGroupConfigList.stream().filter(o -> o.getTableGroupRecord().isBroadCastTableGroup())
                .map(o -> o.getTableGroupRecord().getTg_name()).collect(Collectors.toSet());
        // 3.4 Messy operation.
        Set<String> toRemoveLocalityTgSet =
            pgListGroupByTg.keySet().stream().filter(o -> toSyncTableGroupSet.contains(o)).collect(Collectors.toSet());

        Set<String> validTgSet = toMovePartitionTgs.keySet();
        validTgSet.addAll(toRemoveLocalityTgSet);
        List<TableGroupConfig> tableGroupConfigs =
            tableGroupConfigList.stream().filter(o -> validTgSet.contains(o.getTableGroupRecord().tg_name)).collect(
                Collectors.toList());
        validTgSet.removeAll(boadcastTgSets);
        // 3.5 Final Filter.
        Map<String, List<PartitionGroupStat>> toRebalancePgListGroupByTg =
            pgListGroupByTg.keySet().stream().filter(o -> validTgSet.contains(o))
                .collect(Collectors.toMap(o -> o, o -> pgListGroupByTg.get(o)));

//        Date filterStatsEndTime = new Date();
//        Long filterStatsCostMillis = filterStatsEndTime.getTime() - filterStatsStartTime.getTime();
//        String filterStatslogInfo =
//            String.format(
//                "[schema %s] filter toRebalancePgList in %d ms: totalPgList = %d, filterPgList = %d, toRemoveLocalityPgList = %d",
//                schemaName, filterStatsCostMillis, pgList.size(),
//                toRebalancePgSize, toRebalancePgSize);
//        EventLogger.log(EventType.REBALANCE_INFO, filterStatslogInfo);

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }

        // 4.1 Map all the group/storageInst to index.
        int M = groupMap.size();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<Integer, String> storageInstMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        Map<String, Integer> storageInstReverseMap = new HashMap();
        List<String> groupNames = new ArrayList<>(groupMap.keySet());
        List<String> storageInsts =
            groupNames.stream().map(o -> groupMap.get(o).storageInstId).collect(Collectors.toList());
        for (int i = 0; i < M; i++) {
            storageInstMap.put(i, storageInsts.get(i));
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
            storageInstReverseMap.put(storageInsts.get(i), i);
        }

        int[] drainNodeIndexes =
            drainNodeInfo.getDnInstIdList().stream().mapToInt(o -> storageInstReverseMap.get(o)).toArray();

        // 4.2 get tgSize and sort tgNames by tgSize.
        Map<String, Long> tgDataSize = new HashMap<>();
        for (String tgName : toRebalancePgListGroupByTg.keySet()) {
            Long tgSize = toRebalancePgListGroupByTg.get(tgName).stream().map(PartitionGroupStat::getTotalDiskSize)
                .reduce(0L, Long::sum);
            tgDataSize.put(tgName, tgSize);
        }
        List<String> tableGroupNames = validTgSet.stream().collect(Collectors.toList());
        tableGroupNames.sort(Comparator.comparingLong(key -> tgDataSize.get(key)).reversed());
        DataDistInfo dataDistInfo = DataDistInfo.fromSchemaAndInstMap(schemaName, storageInstMap, groupDetailMap);
        List<PolicyPartitionBalance.MoveInfo> moves = new ArrayList<>();
        // 4.3 Compute PgMoveAction inside tg.
        for (int k = 0; k < tableGroupNames.size(); k++) {

            String tgName = tableGroupNames.get(k);
            List<PartitionGroupStat> toRebalancePgList = toRebalancePgListGroupByTg.get(tgName);
            int m = storageInsts.size();
            int N = toRebalancePgList.size();

            int[] originalPlace = new int[N];
            int[] targetPlace = new int[N];
            double[] partitionSize = new double[N];
            Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();

            for (int i = 0; i < N; i++) {
                PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
                toRebalancePgMap.put(i, partitionGroupStat);
                String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
                originalPlace[i] = groupDetailReverseMap.get(groupKey);
                partitionSize[i] = partitionGroupStat.getDataRows();
                targetPlace[i] = originalPlace[i];
            }
            Date startTime = new Date();
            String logInfo = String.format(
                "[schema %s, tablegroup %s] start to solve move partition problem: M=%d, N=%d, originalPlace=%s, partitionSize=%s",
                schemaName, tgName, M, N, Arrays.toString(originalPlace), Arrays.toString(partitionSize));
            EventLogger.log(EventType.REBALANCE_INFO, logInfo);

            Solution solution = null;
            // TODO: while select drain node index is empty.
            if (k < MAX_TABLEGROUP_SOLVED_BY_LP) {
                solution = MixedModel.solveMovePartition(m, N, originalPlace, partitionSize, drainNodeIndexes);
            } else {
                solution = MixedModel.solveMovePartitionByGreedy(m, N, originalPlace, partitionSize, drainNodeIndexes);
            }
            if (solution.withValidSolve) {
//                    Date endTime = new Date();
//                    Long costMillis = endTime.getTime() - startTime.getTime();
//                    logInfo =
//                        String.format(
//                            "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
//                            schemaName, tgName, costMillis, solution.strategy, originalMu, solution.mu,
//                            Arrays.toString(solution.targetPlace));
//                    EventLogger.log(EventType.REBALANCE_INFO, logInfo);
//                double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//                double targetFactor = caculateBalanceFactor(M, N, solution.targetPlace, partitionSize);
//                if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                    targetPlace = solution.targetPlace;
//                }
                targetPlace = solution.targetPlace;

                for (int i = 0; i < N; i++) {
                    if (targetPlace[i] != originalPlace[i]) {
                        moves.add(new PolicyPartitionBalance.MoveInfo(toRebalancePgMap.get(i).getFirstPartition(),
                            toRebalancePgMap.get(i).getTgName(), groupDetailMap.get(targetPlace[i]),
                            toRebalancePgMap.get(i).getDataRows(), toRebalancePgMap.get(i).getTotalDiskSize()));
                    }

                }
            }
            dataDistInfo.appendTgDataDist(tgName, toRebalancePgList, originalPlace, targetPlace);
        }

        // 4.4 generate move action by cluster move action.
        moves.sort(Comparator.comparingLong(o -> -o.dataSize));
        List<BalanceAction> moveDataActions = new ArrayList<>();
        for (int i = 0; i < moves.size(); ) {
            Long sumMoveSizes = 0L;
            int j = i;
            int nextI;
            for (; j < moves.size() && sumMoveSizes <= options.maxTaskUnitSize * 1024 * 1024; j++) {
                sumMoveSizes += moves.get(j).dataSize;
            }
            nextI = j;
            Map<String, List<ActionMovePartition>> movePartitionActions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
//            Long finalSumMoveRows = sumMoveSizes;
            GeneralUtil.emptyIfNull(moves.subList(i, nextI)).stream().collect(
                    Collectors.groupingBy(o -> o.targetDn, Collectors.mapping(o -> o.partitionStat, Collectors.toList())))
                .forEach((toGroup, partitions) -> {
                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schemaName, partitions,
                        toGroup,
                        stats)) {
//                    if (moveDataActions.size() >= options.maxActions) {
//                        break;
//                    }
                        movePartitionActions.computeIfAbsent(act.getTableGroupName(), o -> new ArrayList<>()).add(act);
                    }
                });
            if (!movePartitionActions.isEmpty()) {
                moveDataActions.add(new ActionMovePartitions(schemaName, movePartitionActions));
            }
            i = nextI;
        }

        //        for (String tgName : movesGroupByTg.keySet()) {
//            List<MoveInfo> movesOfTg = movesGroupByTg.get(tgName);
//            movesOfTg.sort(Comparator.comparingLong(o -> -o.dataSize));
//            for (int i = 0; i < movesOfTg.size(); ) {
//                Long sumMoveRows = 0L;
//                int j = i;
//                int nextI;
//                for (; j < movesOfTg.size() && sumMoveRows <= options.maxTaskUnitRows; j++) {
//                    sumMoveRows += movesOfTg.get(j).tableRows;
//                }
//                nextI = j;
//                GeneralUtil.emptyIfNull(movesOfTg.subList(i, nextI)).stream().collect(
//                    Collectors.groupingBy(o -> o.targetDn,
//                        Collectors.mapping(o -> o.partitionStat, Collectors.toList()))
//                ).forEach((toGroup, partitions) -> {
//                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schema, partitions,
//                        toGroup, stats)) {
//                        if (actions.size() >= options.maxActions) {
//                            break;
//                        }
//                        actions.add(act);
//                    }
//                });
//                i = nextI;
//            }
//        }
        // 4.6 log and add move actions.
        String distLogInfo =
            String.format("[schema %s] estimated data distribution: %s", schemaName, JSON.toJSONString(dataDistInfo));
        EventLogger.log(EventType.REBALANCE_INFO, distLogInfo);
        ActionWriteDataDistLog actionWriteDataDistLog = new ActionWriteDataDistLog(schemaName, dataDistInfo);
        moveDataActions.add(actionWriteDataDistLog);

        // 5 drain broadcast table.
        ActionDropBroadcastTable dropBroadcastTable =
            new ActionDropBroadcastTable(schemaName, drainNodeInfo.getDnInstIdList());

        // 6 Hide all the to-removed groups and notify all cn nodes to clean the to-removed group data sources
        List<String> toRemoveGroupNames = new ArrayList<>();
        toRemoveGroupNames.addAll(removedGroups);
        DropDbGroupHideMetaTask hideDbGroupMetaTask = new DropDbGroupHideMetaTask(schemaName, toRemoveGroupNames);
        ActionTaskAdapter hideToRemovedGroupMetaAction = new ActionTaskAdapter(schemaName, hideDbGroupMetaTask);

        // 7 syncTopology
        TopologySyncTask topologySyncTask = new TopologySyncTask(schemaName);
        ActionTaskAdapter syncNewTopologyAction = new ActionTaskAdapter(schemaName, topologySyncTask);

        // 8. remove physical db for all the to-removed groups
        List<BalanceAction> dropPhyDbActions =
            removedGroups.stream().map(x -> new ActionTaskAdapter(schemaName, new DropPhysicalDbTask(schemaName, x)))
                .collect(Collectors.toList());

        // 9. remove groups on the storage
        CleanRemovedDbGroupMetaTask cleanRemovedDbGroupMetaTask =
            new CleanRemovedDbGroupMetaTask(schemaName, toRemoveGroupNames);
        ActionTaskAdapter cleanRemovedDbGroupMetaAction =
            new ActionTaskAdapter(schemaName, cleanRemovedDbGroupMetaTask);

        // [2]. change localityInfo meta.
        CleanRemovedDbLocalityMetaTask cleanRemovedDbLocalityMetaTask =
            new CleanRemovedDbLocalityMetaTask(schemaName, toRemoveLocalityItems);
        ActionTaskAdapter cleanRemovedDbLocalityMetaAction =
            new ActionTaskAdapter(schemaName, cleanRemovedDbLocalityMetaTask);

        // [2]. sync tablegroup.
        List<ActionTaskAdapter> syncTableGroupsAction = new ArrayList<>();
        for (
            int i = 0; i < toSyncTableGroup.size(); i++) {
            TableGroupSyncTask tableGroupSyncTask = new TableGroupSyncTask(schemaName, toSyncTableGroup.get(i));
            TablesSyncTask tablesSyncTask = new TablesSyncTask(schemaName, toSyncTables.get(i));
            syncTableGroupsAction.add(new ActionTaskAdapter(schemaName, tablesSyncTask));
            syncTableGroupsAction.add(new ActionTaskAdapter(schemaName, tableGroupSyncTask));
        }

        // [0]. drainNodeValidate
        DrainNodeValidateTask drainNodeValidateTask = new DrainNodeValidateTask(schemaName, tableGroupConfigList);
        ActionTaskAdapter drainNodeValidateTaskAdapter = new ActionTaskAdapter(schemaName, drainNodeValidateTask);

        // [0]. lock
        final String name = ActionUtils.genRebalanceResourceName(RebalanceTarget.DATABASE, schemaName);
        final String schemaXLock = schemaName;
        ActionLockResource lock =
            new ActionLockResource(schemaName, com.google.common.collect.Sets.newHashSet(name, schemaXLock));

        // 10. Modify status of group
        UpdateGroupInfoTask task =
            new UpdateGroupInfoTask(schemaName, removedGroups, DbGroupInfoRecord.GROUP_TYPE_NORMAL,
                DbGroupInfoRecord.GROUP_TYPE_BEFORE_REMOVE);
        ActionTaskAdapter actionUpdateGroupStatus = new ActionTaskAdapter(schemaName, task);

        TopologySyncThenReleaseXLockTask topologySyncThenReleaseXLockTask =
            new TopologySyncThenReleaseXLockTask(schemaName, schemaXLock);
        ActionTaskAdapter actionTopologySyncThenReleaseXLockTask =
            new ActionTaskAdapter(schemaName, topologySyncThenReleaseXLockTask);
        // 11. combine actions
//        actions.add(lock);
        actions.add(drainNodeValidateTaskAdapter);
        actions.add(actionUpdateGroupStatus);
        actions.add(actionTopologySyncThenReleaseXLockTask);
        actions.add(cleanRemovedDbLocalityMetaAction);
        actions.addAll(syncTableGroupsAction);
        actions.addAll(moveDataActions);
        actions.add(dropBroadcastTable);
        actions.add(hideToRemovedGroupMetaAction);
        actions.add(syncNewTopologyAction);
        actions.addAll(dropPhyDbActions);
        actions.add(cleanRemovedDbGroupMetaAction);
        return actions;

    }

    public List<BalanceAction> applyToPartitionDbDrainStoragePool(ExecutionContext ec, BalanceOptions options,
                                                                  BalanceStats stats,
                                                                  String schemaName, DrainNodeInfo drainNodeInfo,
                                                                  DrainStoragePool drainStoragePool) {
        List<BalanceAction> actions = new ArrayList<>();
        Map<String, GroupDetailInfoRecord> groupMap = PolicyUtils.getGroupDetails(schemaName);
        List<String> removedGroups =
            groupMap.values().stream().filter(x -> drainNodeInfo.containsDnInst(x.getStorageInstId()))
                .map(GroupDetailInfoRecord::getGroupName).collect(Collectors.toList());

        // 1.1 find all database object that locality interact with given storagePool.
        // If a tablegroup or partitiongroup locality defination contains the drained storagePool.
        // There are two cases:
        // a. if tablegroup's primary storage pool is drained, then set tablegroup's storage pool = null.
        // the corresponding partition move to db's primary storage pool.
        // b. if tablegroup's non-primary storage pool is drained, then shrink tg's storage pool,
        // set pg's sp=null, the non-default partition move to tg's primary storage pool.
        // c. if tablegroup's locality is dn-spec, then drain it.
        Set<String> toMoveTableGroup = new HashSet<>();
        Set<String> toRemoveLocalityTableGroup = new HashSet<>();
        Set<Long> toRemoveLocalityPartitionGroup = new HashSet<>();
        Set<Long> toMovePartitionGroup = new HashSet<>();
        List<LocalityDetailInfoRecord> localityDetailInfoRecords = PolicyUtils.getLocalityDetails(schemaName);
        List<LocalityDetailInfoRecord> toRemoveLocalityItem = new ArrayList<>();
        List<Pair<LocalityDetailInfoRecord, LocalityDetailInfoRecord>> toUpdateLocalityItem = new ArrayList<>();
        for (LocalityDetailInfoRecord localityDetailInfoRecord : localityDetailInfoRecords) {
            LocalityDesc localityDesc = LocalityDesc.parse(localityDetailInfoRecord.locality);
            if (localityDesc.hasStoragePoolDefinition()) {
                if (drainStoragePool.containsStoragePool(localityDesc.getPrimaryStoragePoolName())) {
                    if (localityDetailInfoRecord.objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_TABLEGROUP) {
                        toMoveTableGroup.add(localityDetailInfoRecord.objectName);
                        toRemoveLocalityTableGroup.add(localityDetailInfoRecord.objectName);
                    } else if (localityDetailInfoRecord.objectType
                        == LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP) {
                        toMovePartitionGroup.add(localityDetailInfoRecord.objectId);
                        toRemoveLocalityPartitionGroup.add(localityDetailInfoRecord.objectId);
                    }
                    toRemoveLocalityItem.add(localityDetailInfoRecord);
                } else if (drainStoragePool.hasIntersectionWithStoragePoolList(localityDesc.getStoragePoolNames())) {
                    //only database and tablegroup.
                    Set<String> storagePoolNames =
                        drainStoragePool.intersectWithStoragePoolList(localityDesc.getStoragePoolNames());
                    LocalityDetailInfoRecord modififiedLocalityDetailInfoRecord =
                        new LocalityDetailInfoRecord(localityDetailInfoRecord);
                    LocalityDesc modifiedlocalityDesc = LocalityDesc.parse(modififiedLocalityDetailInfoRecord.locality)
                        .removeStoragePool(storagePoolNames);
                    String locality = modifiedlocalityDesc.toString();
                    modififiedLocalityDetailInfoRecord.setLocality(locality);
                    toUpdateLocalityItem.add(Pair.of(localityDetailInfoRecord, modififiedLocalityDetailInfoRecord));
                }
            } else if (localityDesc.hasBalanceSingleTable()) {
                //TODO: singleTable.

            } else {
                if (drainNodeInfo.intersectDnInstList(localityDesc.getDnList())) {
                    toRemoveLocalityItem.add(localityDetailInfoRecord);
                    if (localityDetailInfoRecord.objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP) {
                        toMoveTableGroup.add(localityDetailInfoRecord.objectName);
                        toRemoveLocalityTableGroup.add(localityDetailInfoRecord.objectName);
                    } else if (localityDetailInfoRecord.objectType
                        == LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP) {
                        toMovePartitionGroup.add(localityDetailInfoRecord.objectId);
                        toRemoveLocalityPartitionGroup.add(localityDetailInfoRecord.id);
                    }
                }
            }
        }

        // 2.1 filter pgList relevant to this drainNode.
        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();
        Set<String> toMoveTgFromPg = pgList.stream().filter(o -> toMovePartitionGroup.contains(o.getPgId()))
            .map(o -> o.getFirstPartition().getTableGroupName()).collect(Collectors.toSet());
        toMoveTableGroup.addAll(toMoveTgFromPg);

        Map<String, List<PartitionGroupStat>> pgListGroupByTg = GeneralUtil.emptyIfNull(pgList).stream()
            .collect(Collectors.groupingBy(o -> o.getTgName(), Collectors.mapping(o -> o, Collectors.toList())));

        List<TableGroupConfig> tableGroupConfigList = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);
        Map<String, TableGroupConfig> tgMap = new HashMap<>();
        for (TableGroupConfig tgConfig : tableGroupConfigList) {
            tgMap.put(tgConfig.getTableGroupRecord().tg_name, tgConfig);
        }

        List<String> toSyncTableGroup = new ArrayList<>(toMoveTableGroup);
        List<List<String>> toSyncTables = tableGroupConfigList.stream()
            .filter(tableGroupConfig -> toSyncTableGroup.contains(tableGroupConfig.getTableGroupRecord().tg_name)).map(
                tableGroupConfig -> tableGroupConfig.getTables()).collect(Collectors.toList());

        List<ActionMovePartition> actionMovePartitions = new ArrayList<>();
        // prepare move action

        Date filterStatsStartTime = new Date();

        // 2.2 get broadcast tg.
        Set<String> broadcastTgSets =
            tableGroupConfigList.stream().filter(o -> o.getTableGroupRecord().isBroadCastTableGroup())
                .map(o -> o.getTableGroupRecord().getTg_name()).collect(Collectors.toSet());
        toMoveTableGroup.removeAll(broadcastTgSets);

        // 2.3 get all the pgList.
        Map<String, List<PartitionGroupStat>> toRebalancePgListGroupByTg =
            pgListGroupByTg.keySet().stream().filter(o -> toMoveTableGroup.contains(o))
                .collect(Collectors.toMap(o -> o, o -> pgListGroupByTg.get(o)));

        int toRebalancePgSize =
            toRebalancePgListGroupByTg.keySet().stream().mapToInt(o -> toRebalancePgListGroupByTg.get(o).size()).sum();

        Date filterStatsEndTime = new Date();
        Long filterStatsCostMillis = filterStatsEndTime.getTime() - filterStatsStartTime.getTime();
        String filterStatslogInfo = String.format(
            "[schema %s] filter toRebalancePgList in %d ms: totalPgList = %d, filterPgList = %d, toRemoveLocalityPgList = %d",
            schemaName, filterStatsCostMillis, pgList.size(), toRebalancePgSize, toRebalancePgSize);
        EventLogger.log(EventType.REBALANCE_INFO, filterStatslogInfo);

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }

        // 3.1 map group detail to index.
        int M = groupMap.size();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<Integer, String> storageInstMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        Map<String, Integer> storageInstReverseMap = new HashMap();
        List<String> groupNames = new ArrayList<>(groupMap.keySet());
        List<String> storageInsts =
            groupNames.stream().map(o -> groupMap.get(o).storageInstId).collect(Collectors.toList());
        Map<String, String> storagePoolMap = StoragePoolManager.getInstance().storagePoolMap;
        // 3.2 unlike the standard procedures, we will still process tablegroup case by case.
        // because for each tablegroup, there are
        //group -> storagePool
        Map<String, String> spMap = groupMap.entrySet().stream().map(
                entry -> new AbstractMap.SimpleEntry<>(entry.getKey(),
                    storagePoolMap.getOrDefault(entry.getValue().storageInstId, "")))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (int i = 0; i < M; i++) {
            storageInstMap.put(i, storageInsts.get(i));
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
            storageInstReverseMap.put(storageInsts.get(i), i);
        }

        int[] drainNodeIndexes =
            drainNodeInfo.getDnInstIdList().stream().mapToInt(o -> storageInstReverseMap.get(o)).toArray();

        Map<String, Long> tgDataSize = new HashMap<>();
        for (String tgName : toRebalancePgListGroupByTg.keySet()) {
            Long tgSize = toRebalancePgListGroupByTg.get(tgName).stream().map(PartitionGroupStat::getTotalDiskSize)
                .reduce(0L, Long::sum);
            tgDataSize.put(tgName, tgSize);
        }
        List<String> tableGroupNames = tgDataSize.keySet().stream().collect(Collectors.toList());
        tableGroupNames.sort(Comparator.comparingLong(key -> tgDataSize.get(key)).reversed());
        DataDistInfo dataDistInfo = DataDistInfo.fromSchemaAndInstMap(schemaName, storageInstMap, groupDetailMap);
        List<PolicyPartitionBalance.MoveInfo> moves = new ArrayList<>();
        String dbLocality = LocalityManager.getInstance().getLocalityOfDb(schemaName).getLocality();
        // 3.2 compute move actions here.
        for (int k = 0; k < tableGroupNames.size(); k++) {
            String tgName = tableGroupNames.get(k);
            List<PartitionGroupStat> toRebalancePgListTg = toRebalancePgListGroupByTg.get(tgName);
            TableGroupConfig tgConfig = tgMap.get(tgName);
            //TODO: add db storage pool.
            String defaultLocality = dbLocality;
            if (toRemoveLocalityTableGroup.contains(tgName) || tgConfig.getLocalityDesc().holdEmptyLocality()) {
                defaultLocality = dbLocality;
            } else if (tgConfig.getLocalityDesc().hasStoragePoolDefinition()) {
                defaultLocality = tgConfig.getLocalityDesc().toString();
            } else {
                defaultLocality = tgConfig.getLocalityDesc().toString();
            }
            // cluster by localityString
            Map<String, List<PartitionGroupStat>> pgListClusterByLocality = new HashMap<>();
            for (PartitionGroupStat pgStat : toRebalancePgListTg) {
                Long pgId = pgStat.getPgId();
                LocalityDesc pglocalityDesc = LocalityDesc.parse(pgStat.pg.locality);
                if (toRemoveLocalityPartitionGroup.contains(pgId) || pglocalityDesc.holdEmptyLocality()) {
                    if (!pgListClusterByLocality.containsKey(defaultLocality)) {
                        pgListClusterByLocality.put(defaultLocality, new ArrayList<>());
                    }
                    pgListClusterByLocality.get(defaultLocality).add(pgStat);
                } else if (pglocalityDesc.hasStoragePoolDefinition()) {
                    if (!pgListClusterByLocality.containsKey(pglocalityDesc.toString())) {
                        pgListClusterByLocality.put(pglocalityDesc.toString(), new ArrayList<>());
                    }
                    pgListClusterByLocality.get(pglocalityDesc.toString()).add(pgStat);
                } else {
                    if (!pgListClusterByLocality.containsKey(pglocalityDesc.toString())) {
                        pgListClusterByLocality.put(pglocalityDesc.toString(), new ArrayList<>());
                    }
                    pgListClusterByLocality.get(pglocalityDesc.toString()).add(pgStat);
                }
            }
            // iteration over each cluster.
            for (String localityStr : pgListClusterByLocality.keySet()) {
                List<PartitionGroupStat> toRebalancePgList = pgListClusterByLocality.get(localityStr);
                int N = toRebalancePgList.size();

                int[] originalPlace = new int[N];
                int[] targetPlace = new int[N];
                double[] partitionSize = new double[N];
                Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();

                for (int i = 0; i < N; i++) {
                    PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
                    toRebalancePgMap.put(i, partitionGroupStat);
                    String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
                    originalPlace[i] = groupDetailReverseMap.get(groupKey);
                    partitionSize[i] = partitionGroupStat.getDataRows();
                    targetPlace[i] = originalPlace[i];
                }
                Date startTime = new Date();
                String logInfo = String.format(
                    "[schema %s, tablegroup %s] start to solve move partition problem: M=%d, N=%d, originalPlace=%s, partitionSize=%s",
                    schemaName, tgName, M, N, Arrays.toString(originalPlace), Arrays.toString(partitionSize));
                EventLogger.log(EventType.REBALANCE_INFO, logInfo);

                // construct drainNodeIndex.
                LocalityDesc localityDesc = LocalityInfoUtils.parse(localityStr);
                Set<String> drainNodeByLocality =
                    storageInsts.stream().filter(o -> !localityDesc.matchStorageInstance(o))
                        .collect(Collectors.toSet());
                int[] drainNodeIndexesByLocality = drainNodeByLocality.stream().map(o -> storageInstReverseMap.get(o)).
                    mapToInt(Integer::intValue).toArray();
                Solution solution = null;
                if (k < MAX_TABLEGROUP_SOLVED_BY_LP) {
                    solution = MixedModel.solveMovePartition(M, N, originalPlace, partitionSize, solveLevel,
                        drainNodeIndexesByLocality);
                } else {
                    solution = MixedModel.solveMovePartitionByGreedy(M, N, originalPlace, partitionSize,
                        drainNodeIndexesByLocality);
                }
                if (solution.withValidSolve) {
                    Date endTime = new Date();
                    Long costMillis = endTime.getTime() - startTime.getTime();
                    logInfo = String.format(
                        "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                        schemaName, tgName, costMillis, solution.strategy, 0.0f, solution.mu,
                        Arrays.toString(solution.targetPlace));
                    EventLogger.log(EventType.REBALANCE_INFO, logInfo);
//                double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//                double targetFactor = caculateBalanceFactor(M, N, solution.targetPlace, partitionSize);
//                if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                    targetPlace = solution.targetPlace;
//                }
                    targetPlace = solution.targetPlace;

                    for (int i = 0; i < N; i++) {
                        if (targetPlace[i] != originalPlace[i]) {
                            moves.add(new PolicyPartitionBalance.MoveInfo(toRebalancePgMap.get(i).getFirstPartition(),
                                toRebalancePgMap.get(i).getTgName(), groupDetailMap.get(targetPlace[i]),
                                toRebalancePgMap.get(i).getDataRows(), toRebalancePgMap.get(i).getTotalDiskSize()));
                        }

                    }
                }
                dataDistInfo.appendTgDataDist(tgName, toRebalancePgList, originalPlace, targetPlace);
            }

            // sort by dataSize desc.
//        int toIndex = Math.min(options.maxActions * 20, moves.size());
//        moves = moves.subList(0, toIndex);
//        Map<String, List<PolicyPartitionBalance.MoveInfo>> movesGroupByTg = moves.stream().collect(
//            Collectors.groupingBy(o -> o.tgName, Collectors.mapping(o -> o, Collectors.toList()))
//        );
        }
        // 3.3 cluster move action.
        moves.sort(Comparator.comparingLong(o -> -o.dataSize));
        List<BalanceAction> moveDataActions = new ArrayList<>();
        for (int i = 0; i < moves.size(); ) {
            Long sumMoveSizes = 0L;
            int j = i;
            int nextI;
            for (; j < moves.size() && sumMoveSizes <= options.maxTaskUnitSize * 1024 * 1024; j++) {
                sumMoveSizes += moves.get(j).dataSize;
            }
            nextI = j;
            Map<String, List<ActionMovePartition>> movePartitionActions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
//            Long finalSumMoveRows = sumMoveSizes;
            GeneralUtil.emptyIfNull(moves.subList(i, nextI)).stream().collect(
                    Collectors.groupingBy(o -> o.targetDn, Collectors.mapping(o -> o.partitionStat, Collectors.toList())))
                .forEach((toGroup, partitions) -> {
                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schemaName, partitions,
                        toGroup, stats)) {
//                    if (moveDataActions.size() >= options.maxActions) {
//                        break;
//                    }
                        movePartitionActions.computeIfAbsent(act.getTableGroupName(), o -> new ArrayList<>()).add(act);
                    }
                });
            if (!movePartitionActions.isEmpty()) {
                moveDataActions.add(new ActionMovePartitions(schemaName, movePartitionActions));
            }
            i = nextI;
        }

        String distLogInfo =
            String.format("[schema %s] estimated data distribution: %s", schemaName, JSON.toJSONString(dataDistInfo));
        EventLogger.log(EventType.REBALANCE_INFO, distLogInfo);
        ActionWriteDataDistLog actionWriteDataDistLog = new ActionWriteDataDistLog(schemaName, dataDistInfo);
        moveDataActions.add(actionWriteDataDistLog);

        // 4. drop broadcast
        ActionDropBroadcastTable dropBroadcastTable =
            new ActionDropBroadcastTable(schemaName, drainNodeInfo.getDnInstIdList());

        // 5. Hide all the to-removed groups and notify all cn nodes to clean the to-removed group data sources
        List<String> toRemoveGroupNames = new ArrayList<>();
        toRemoveGroupNames.addAll(removedGroups);
        DropDbGroupHideMetaTask hideDbGroupMetaTask = new DropDbGroupHideMetaTask(schemaName, toRemoveGroupNames);
        ActionTaskAdapter hideToRemovedGroupMetaAction = new ActionTaskAdapter(schemaName, hideDbGroupMetaTask);

        TopologySyncTask topologySyncTask = new TopologySyncTask(schemaName);
        ActionTaskAdapter syncNewTopologyAction = new ActionTaskAdapter(schemaName, topologySyncTask);

        // 6. remove physical db for all the to-removed groups
        List<BalanceAction> dropPhyDbActions =
            removedGroups.stream().map(x -> new ActionTaskAdapter(schemaName, new DropPhysicalDbTask(schemaName, x)))
                .collect(Collectors.toList());

        // 7. remove groups on the storage
        CleanRemovedDbGroupMetaTask cleanRemovedDbGroupMetaTask =
            new CleanRemovedDbGroupMetaTask(schemaName, toRemoveGroupNames);
        ActionTaskAdapter cleanRemovedDbGroupMetaAction =
            new ActionTaskAdapter(schemaName, cleanRemovedDbGroupMetaTask);

        // 8. change locality meta in db.
        CleanUpdateDbLocalityMetaTask cleanUpdateDbLocalityMetaTask =
            new CleanUpdateDbLocalityMetaTask(schemaName, toRemoveLocalityItem, toUpdateLocalityItem);
        ActionTaskAdapter cleanUpdateDbLocalityMetaAction =
            new ActionTaskAdapter(schemaName, cleanUpdateDbLocalityMetaTask);

        List<ActionTaskAdapter> syncTableGroupsAction = new ArrayList<>();
        for (int i = 0; i < toSyncTableGroup.size(); i++) {
            TableGroupSyncTask tableGroupSyncTask = new TableGroupSyncTask(schemaName, toSyncTableGroup.get(i));
            TablesSyncTask tablesSyncTask = new TablesSyncTask(schemaName, toSyncTables.get(i));
            syncTableGroupsAction.add(new ActionTaskAdapter(schemaName, tablesSyncTask));
            syncTableGroupsAction.add(new ActionTaskAdapter(schemaName, tableGroupSyncTask));
        }

        List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
        GeneralUtil.emptyIfNull(stats.getTableGroupStats()).stream().forEach(o -> {
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
        UpdateGroupInfoTask task =
            new UpdateGroupInfoTask(schemaName, removedGroups, DbGroupInfoRecord.GROUP_TYPE_NORMAL,
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
        actions.add(cleanUpdateDbLocalityMetaAction);
        actions.addAll(syncTableGroupsAction);
        actions.addAll(moveDataActions);
        actions.add(dropBroadcastTable);
        actions.add(hideToRemovedGroupMetaAction);
        actions.add(syncNewTopologyAction);
        actions.addAll(dropPhyDbActions);
        actions.add(cleanRemovedDbGroupMetaAction);

        return actions;
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
    public List<BalanceAction> applyToPartitionDbNew(ExecutionContext ec, BalanceOptions options, BalanceStats stats,
                                                     String schemaName) {
        List<BalanceAction> actions = new ArrayList<>();

        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        DrainStoragePool drainStoragePool = DrainStoragePool.parse(options.drainStoragePool);
        if (!drainStoragePool.holdsEmptyStoragePool()) {
            return applyToPartitionDbDrainStoragePool(ec, options, stats, schemaName, drainNodeInfo, drainStoragePool);

        }

        Map<String, GroupDetailInfoRecord> groupMap = PolicyUtils.getGroupDetails(schemaName);
        List<String> removedGroups =
            groupMap.values().stream().filter(x -> drainNodeInfo.containsDnInst(x.getStorageInstId()))
                .map(GroupDetailInfoRecord::getGroupName).collect(Collectors.toList());

        // remove all tableGroup locality containing drain node;
        List<LocalityDetailInfoRecord> localityDetailInfoRecords = PolicyUtils.getLocalityDetails(schemaName);
        List<LocalityDetailInfoRecord> toRemoveLocalityItems =
            localityDetailInfoRecords.stream().filter(x -> (x.getLocality() != null && !x.getLocality().isEmpty()))
                .filter(x -> drainNodeInfo.intersectDnInstList(LocalityInfoUtils.parse(x.getLocality()).getDnList()))
                .collect(Collectors.toList());

        List<TableGroupConfig> tableGroupConfigList = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);
        Map<String, TableGroupConfig> tableGroupConfigMap =
            tableGroupConfigList.stream().collect(Collectors.toMap(o -> o.getTableGroupRecord().tg_name, o -> o));
        Set<String> toSyncTableGroupSet = new HashSet<>();
        for (TableGroupConfig tableGroupConfig : tableGroupConfigList) {
            if (drainNodeInfo.intersectDnInstList(tableGroupConfig.getLocalityDesc().getDnList())) {
                toSyncTableGroupSet.add(tableGroupConfig.getTableGroupRecord().tg_name);
            } else {
                for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
                    if (drainNodeInfo.intersectDnInstList(
                        LocalityInfoUtils.parse(partitionGroupRecord.getLocality()).getDnList())) {
                        toSyncTableGroupSet.add(tableGroupConfig.getTableGroupRecord().tg_name);
                        break;
                    }
                }
            }
        }

        List<String> toSyncTableGroup = new ArrayList<>(toSyncTableGroupSet);
        List<List<String>> toSyncTables = tableGroupConfigList.stream()
            .filter(tableGroupConfig -> toSyncTableGroup.contains(tableGroupConfig.getTableGroupRecord().tg_name)).map(
                tableGroupConfig -> tableGroupConfig.getTables()).collect(Collectors.toList());
        List<ActionMovePartition> actionMovePartitions = new ArrayList<>();
        // prepare move action

        Date filterStatsStartTime = new Date();
        // 1. collect all pg list.
        // 2. filter pg list without locality. NOTICE! empty tablegroup is ignored!
        // 3. filter pg list with locality holding draining node. NOTICE! empty tablegroup holding draining node is ignored!
        // 4. merge these two pg list.
        // 5. what if pg is empty? // IMPOSSIBLE
        // 6. TODO: what if tg holds complex locality?
        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();

        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        Map<String, List<PartitionGroupStat>> pgListGroupByTg = GeneralUtil.emptyIfNull(pgList).stream()
            .collect(Collectors.groupingBy(o -> o.getTgName(), Collectors.mapping(o -> o, Collectors.toList())));

        Set<String> boadcastTgSets =
            tableGroupConfigList.stream().filter(o -> o.getTableGroupRecord().isBroadCastTableGroup())
                .map(o -> o.getTableGroupRecord().getTg_name()).collect(Collectors.toSet());
        Set<String> nonLocalityTgSet = pgListGroupByTg.keySet().stream().filter(
                o -> LocalityInfoUtils.withoutRestrictedLocality(groupDetailInfoExRecordList, tableGroupConfigMap.get(o)))
            .collect(Collectors.toSet());
        Set<String> toRemoveLocalityTgSet =
            pgListGroupByTg.keySet().stream().filter(o -> toSyncTableGroupSet.contains(o)).collect(Collectors.toSet());

        Set<String> validTgSet = nonLocalityTgSet.stream().map(String::new).collect(Collectors.toSet());
        validTgSet.addAll(toRemoveLocalityTgSet);
        validTgSet.removeAll(boadcastTgSets);
        Map<String, List<PartitionGroupStat>> toRebalancePgListGroupByTg =
            pgListGroupByTg.keySet().stream().filter(o -> validTgSet.contains(o))
                .collect(Collectors.toMap(o -> o, o -> pgListGroupByTg.get(o)));

        int toRebalancePgSize =
            toRebalancePgListGroupByTg.keySet().stream().mapToInt(o -> toRebalancePgListGroupByTg.get(o).size()).sum();

        Date filterStatsEndTime = new Date();
        Long filterStatsCostMillis = filterStatsEndTime.getTime() - filterStatsStartTime.getTime();
        String filterStatslogInfo = String.format(
            "[schema %s] filter toRebalancePgList in %d ms: totalPgList = %d, filterPgList = %d, toRemoveLocalityPgList = %d",
            schemaName, filterStatsCostMillis, pgList.size(), toRebalancePgSize, toRebalancePgSize);
        EventLogger.log(EventType.REBALANCE_INFO, filterStatslogInfo);

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }

        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);
        int M = groupDetail.size();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<Integer, String> storageInstMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        Map<String, Integer> storageInstReverseMap = new HashMap();
        List<String> groupNames = new ArrayList<>(groupDetail.keySet());
        List<String> storageInsts =
            groupNames.stream().map(o -> groupDetail.get(o).storageInstId).collect(Collectors.toList());
        Map<String, String> storagePoolMap = StoragePoolManager.getInstance().storagePoolMap;
        //group -> storagePool
        Map<String, String> spMap = groupDetail.entrySet().stream().map(
                entry -> new AbstractMap.SimpleEntry<>(entry.getKey(),
                    storagePoolMap.getOrDefault(entry.getValue().storageInstId, "")))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (int i = 0; i < M; i++) {
            storageInstMap.put(i, storageInsts.get(i));
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
            storageInstReverseMap.put(storageInsts.get(i), i);
        }

        int[] drainNodeIndexes =
            drainNodeInfo.getDnInstIdList().stream().mapToInt(o -> storageInstReverseMap.get(o)).toArray();

        Map<String, List<Integer>> spIndexMap = spMap.keySet().stream().collect(Collectors.groupingBy(o -> spMap.get(o),
            Collectors.mapping(o -> groupDetailReverseMap.get(o), Collectors.toList())));
//        for(String sp:spIndexMap.keySet()){
//            for(int drainNodeIndex:drainNodeIndexes){
//                spIndexMap.get(sp).remove(drainNodeIndex);
//            }
//        }

        Map<String, Long> tgDataSize = new HashMap<>();
        for (String tgName : toRebalancePgListGroupByTg.keySet()) {
            Long tgSize = toRebalancePgListGroupByTg.get(tgName).stream().map(PartitionGroupStat::getTotalDiskSize)
                .reduce(0L, Long::sum);
            tgDataSize.put(tgName, tgSize);
        }
        List<String> tableGroupNames = validTgSet.stream().collect(Collectors.toList());
        tableGroupNames.sort(Comparator.comparingLong(key -> tgDataSize.get(key)).reversed());
        DataDistInfo dataDistInfo = DataDistInfo.fromSchemaAndInstMap(schemaName, storageInstMap, groupDetailMap);
        List<PolicyPartitionBalance.MoveInfo> moves = new ArrayList<>();
        for (int k = 0; k < tableGroupNames.size(); k++) {

            String tgName = tableGroupNames.get(k);
            List<PartitionGroupStat> toRebalancePgListTg = toRebalancePgListGroupByTg.get(tgName);
            Map<String, String> pgSpMap = LocalityInfoUtils.getAllowedStoragePoolOfPartitionGroup(schemaName, tgName);
            Map<String, List<PartitionGroupStat>> toRebalancePgListGroupBySp =
                GeneralUtil.emptyIfNull(toRebalancePgListTg).stream().collect(
                    Collectors.groupingBy(o -> pgSpMap.get(o.getFirstPartition().getPartitionName()),
                        Collectors.mapping(o -> o, Collectors.toList())));
            for (String sp : toRebalancePgListGroupBySp.keySet()) {
                List<PartitionGroupStat> toRebalancePgList = toRebalancePgListGroupBySp.get(sp);
                int m = spIndexMap.get(sp).size();
                int N = toRebalancePgList.size();

                int[] originalPlace = new int[N];
                int[] targetPlace = new int[N];
                double[] partitionSize = new double[N];
                Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();

                for (int i = 0; i < N; i++) {
                    PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
                    toRebalancePgMap.put(i, partitionGroupStat);
                    String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
                    originalPlace[i] = groupDetailReverseMap.get(groupKey);
                    partitionSize[i] = partitionGroupStat.getDataRows();
                    targetPlace[i] = originalPlace[i];
                }
                Date startTime = new Date();
                String logInfo = String.format(
                    "[schema %s, tablegroup %s] start to solve move partition problem: M=%d, N=%d, originalPlace=%s, partitionSize=%s",
                    schemaName, tgName, M, N, Arrays.toString(originalPlace), Arrays.toString(partitionSize));
                EventLogger.log(EventType.REBALANCE_INFO, logInfo);

                Solution solution = null;
                int[] spIndexes = spIndexMap.get(sp).stream().mapToInt(Integer::intValue).toArray();
                Set<Integer> drainNodeIndexSet = Arrays.stream(drainNodeIndexes).boxed().collect(Collectors.toSet());
                int[] selectedDrainNodeIndexes =
                    Arrays.stream(spIndexes).filter(o -> drainNodeIndexSet.contains(o)).toArray();
                // TODO: while select drain node index is empty.
                double originalMu =
                    MixedModel.caculateBalanceFactor(m, N, originalPlace, spIndexes, partitionSize).getValue();
                if (k < MAX_TABLEGROUP_SOLVED_BY_LP) {
                    solution = MixedModel.solveMovePartition(m, N, originalPlace, partitionSize, spIndexes, solveLevel,
                        selectedDrainNodeIndexes);
                } else {
                    solution = MixedModel.solveMovePartitionByGreedy(m, N, originalPlace, partitionSize, spIndexes,
                        selectedDrainNodeIndexes);
                }
                if (solution.withValidSolve) {
                    Date endTime = new Date();
                    Long costMillis = endTime.getTime() - startTime.getTime();
                    logInfo = String.format(
                        "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                        schemaName, tgName, costMillis, solution.strategy, originalMu, solution.mu,
                        Arrays.toString(solution.targetPlace));
                    EventLogger.log(EventType.REBALANCE_INFO, logInfo);
//                double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//                double targetFactor = caculateBalanceFactor(M, N, solution.targetPlace, partitionSize);
//                if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                    targetPlace = solution.targetPlace;
//                }
                    targetPlace = solution.targetPlace;

                    for (int i = 0; i < N; i++) {
                        if (targetPlace[i] != originalPlace[i]) {
                            moves.add(new PolicyPartitionBalance.MoveInfo(toRebalancePgMap.get(i).getFirstPartition(),
                                toRebalancePgMap.get(i).getTgName(), groupDetailMap.get(targetPlace[i]),
                                toRebalancePgMap.get(i).getDataRows(), toRebalancePgMap.get(i).getTotalDiskSize()));
                        }

                    }
                }
                dataDistInfo.appendTgDataDist(tgName, toRebalancePgList, originalPlace, targetPlace);
            }

            // sort by dataSize desc.
//        int toIndex = Math.min(options.maxActions * 20, moves.size());
//        moves = moves.subList(0, toIndex);
//        Map<String, List<PolicyPartitionBalance.MoveInfo>> movesGroupByTg = moves.stream().collect(
//            Collectors.groupingBy(o -> o.tgName, Collectors.mapping(o -> o, Collectors.toList()))
//        );
        }
        moves.sort(Comparator.comparingLong(o -> -o.dataSize));
        List<BalanceAction> moveDataActions = new ArrayList<>();
        for (int i = 0; i < moves.size(); ) {
            Long sumMoveSizes = 0L;
            int j = i;
            int nextI;
            for (; j < moves.size() && sumMoveSizes <= options.maxTaskUnitSize * 1024 * 1024; j++) {
                sumMoveSizes += moves.get(j).dataSize;
            }
            nextI = j;
            Map<String, List<ActionMovePartition>> movePartitionActions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
//            Long finalSumMoveRows = sumMoveSizes;
            GeneralUtil.emptyIfNull(moves.subList(i, nextI)).stream().collect(
                    Collectors.groupingBy(o -> o.targetDn, Collectors.mapping(o -> o.partitionStat, Collectors.toList())))
                .forEach((toGroup, partitions) -> {
                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schemaName, partitions,
                        toGroup, stats)) {
//                    if (moveDataActions.size() >= options.maxActions) {
//                        break;
//                    }
                        movePartitionActions.computeIfAbsent(act.getTableGroupName(), o -> new ArrayList<>()).add(act);
                    }
                });
            if (!movePartitionActions.isEmpty()) {
                moveDataActions.add(new ActionMovePartitions(schemaName, movePartitionActions));
            }
            i = nextI;
        }

//        for (String tgName : movesGroupByTg.keySet()) {
//            List<MoveInfo> movesOfTg = movesGroupByTg.get(tgName);
//            movesOfTg.sort(Comparator.comparingLong(o -> -o.dataSize));
//            for (int i = 0; i < movesOfTg.size(); ) {
//                Long sumMoveRows = 0L;
//                int j = i;
//                int nextI;
//                for (; j < movesOfTg.size() && sumMoveRows <= options.maxTaskUnitRows; j++) {
//                    sumMoveRows += movesOfTg.get(j).tableRows;
//                }
//                nextI = j;
//                GeneralUtil.emptyIfNull(movesOfTg.subList(i, nextI)).stream().collect(
//                    Collectors.groupingBy(o -> o.targetDn,
//                        Collectors.mapping(o -> o.partitionStat, Collectors.toList()))
//                ).forEach((toGroup, partitions) -> {
//                    for (ActionMovePartition act : ActionMovePartition.createMoveToGroups(schema, partitions,
//                        toGroup, stats)) {
//                        if (actions.size() >= options.maxActions) {
//                            break;
//                        }
//                        actions.add(act);
//                    }
//                });
//                i = nextI;
//            }
//        }
        String distLogInfo =
            String.format("[schema %s] estimated data distribution: %s", schemaName, JSON.toJSONString(dataDistInfo));
        EventLogger.log(EventType.REBALANCE_INFO, distLogInfo);
        ActionWriteDataDistLog actionWriteDataDistLog = new ActionWriteDataDistLog(schemaName, dataDistInfo);
        moveDataActions.add(actionWriteDataDistLog);

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
        List<BalanceAction> dropPhyDbActions =
            removedGroups.stream().map(x -> new ActionTaskAdapter(schemaName, new DropPhysicalDbTask(schemaName, x)))
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
        GeneralUtil.emptyIfNull(stats.getTableGroupStats()).stream().forEach(o -> {
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
        UpdateGroupInfoTask task =
            new UpdateGroupInfoTask(schemaName, removedGroups, DbGroupInfoRecord.GROUP_TYPE_NORMAL,
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
        actions.addAll(moveDataActions);
        actions.add(dropBroadcastTable);
        actions.add(hideToRemovedGroupMetaAction);
        actions.add(syncNewTopologyAction);
        actions.addAll(dropPhyDbActions);
        actions.add(cleanRemovedDbGroupMetaAction);

        return actions;
    }

    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        List<BalanceAction> actions = new ArrayList<>();

        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        DrainStoragePool drainStoragePool = DrainStoragePool.parse(options.drainStoragePool);
        if (!drainStoragePool.holdsEmptyStoragePool()) {
            return applyToPartitionDbDrainStoragePool(ec, options, stats, schemaName, drainNodeInfo, drainStoragePool);

        }

        Map<String, String> groupToInst = Maps.newHashMap();
        List<PolicyDrainNode.DnDiskInfo> dnDiskInfo = DnDiskInfo.parseToList(options.diskInfo);

        Map<String, GroupDetailInfoRecord> groupMap = PolicyUtils.getGroupDetails(schemaName);
        List<String> removedGroups =
            groupMap.values().stream()
                .filter(x -> drainNodeInfo.containsDnInst(x.getStorageInstId()))
                .map(GroupDetailInfoRecord::getGroupName)
                .collect(Collectors.toList());

        // remove all tableGroup locality containing drain node;
        List<LocalityDetailInfoRecord> localityDetailInfoRecords = PolicyUtils.getLocalityDetails(schemaName);
        List<LocalityDetailInfoRecord> toRemoveLocalityItems =
            localityDetailInfoRecords.stream().filter(x -> (x.getLocality() != null && !x.getLocality().isEmpty()))
                .filter(x -> drainNodeInfo.intersectDnInstList(LocalityDesc.parse(x.getLocality()).getDnList()))
                .collect(Collectors.toList());

        List<TableGroupConfig> tableGroupConfigList = TableGroupUtils.getAllTableGroupInfoByDb(schemaName);
        Map<String, TableGroupConfig> tableGroupConfigMap = tableGroupConfigList
            .stream().collect(Collectors.toMap(o -> o.getTableGroupRecord().tg_name, o -> o));
        Set<String> toRebalanceSequentialTg = tableGroupConfigMap.keySet().stream()
            .filter(o -> tableGroupConfigMap.get(o).getLocalityDesc().getHashRangeSequentialPlacement())
            .collect(Collectors.toSet());
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
            .map(tableGroupConfig -> tableGroupConfig.getTables())
            .collect(Collectors.toList());
        List<ActionMovePartition> actionMovePartitions = new ArrayList<>();
        // prepare move action

        Date filterStatsStartTime = new Date();
        // 1. collect all pg list.
        // 2. filter pg list without locality. NOTICE! empty tablegroup is ignored!
        // 3. filter pg list with locality holding draining node. NOTICE! empty tablegroup holding draining node is ignored!
        // 4. merge these two pg list.
        // 5. what if pg is empty? // IMPOSSIBLE
        // 6. TODO: what if tg holds complex locality?
        List<PartitionGroupStat> pgList = stats.getPartitionGroupStats();

        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        Map<String, List<PartitionGroupStat>> pgListGroupByTg =
            GeneralUtil.emptyIfNull(pgList).stream()
                .collect(Collectors.groupingBy(o -> o.getTgName(),
                    Collectors.mapping(o -> o, Collectors.toList())));

        Set<String> boadcastTgSets = tableGroupConfigList.stream()
            .filter(o -> o.getTableGroupRecord().isBroadCastTableGroup())
            .map(o -> o.getTableGroupRecord().getTg_name()).collect(Collectors.toSet());
        Set<String> balanceSingleTableTgSets = tableGroupConfigList.stream()
            .filter(o -> o.getLocalityDesc().getBalanceSingleTable())
            .map(o -> o.getTableGroupRecord().getTg_name()).collect(Collectors.toSet());
        Set<String> nonLocalityTgSet = pgListGroupByTg.keySet().stream()
            .filter(o -> LocalityInfoUtils.withoutRestrictedLocality(groupDetailInfoExRecordList,
                tableGroupConfigMap.get(o)))
            .collect(Collectors.toSet());
        Set<String> toRemoveLocalityTgSet = pgListGroupByTg.keySet().stream()
            .filter(o -> toSyncTableGroupSet.contains(o))
            .collect(Collectors.toSet());
        Set<String> validTgSet = nonLocalityTgSet.stream().map(String::new).collect(Collectors.toSet());
        validTgSet.addAll(toRemoveLocalityTgSet);
        validTgSet.removeAll(boadcastTgSets);
        validTgSet.removeAll(balanceSingleTableTgSets);
        Map<String, List<PartitionGroupStat>> toRebalancePgListGroupByTg =
            pgListGroupByTg.keySet().stream()
                .filter(o -> validTgSet.contains(o))
                .collect(Collectors.toMap(o -> o, o -> pgListGroupByTg.get(o)));

        int toRebalancePgSize = toRebalancePgListGroupByTg.keySet().stream()
            .mapToInt(o -> toRebalancePgListGroupByTg.get(o).size()).sum();

        Date filterStatsEndTime = new Date();
        Long filterStatsCostMillis = filterStatsEndTime.getTime() - filterStatsStartTime.getTime();
        String filterStatslogInfo =
            String.format(
                "[schema %s] filter toRebalancePgList in %d ms: totalPgList = %d, filterPgList = %d, toRemoveLocalityPgList = %d",
                schemaName, filterStatsCostMillis, pgList.size(),
                toRebalancePgSize, toRebalancePgSize);
        EventLogger.log(EventType.REBALANCE_INFO, filterStatslogInfo);

        MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.MIN_COST;
        if (!options.solveLevel.equals("DEFAULT") && !options.solveLevel.isEmpty()) {
            solveLevel = MixedModel.SolveLevel.BALANCE_DEFAULT;
        }

        Map<String, GroupDetailInfoRecord> groupDetail = getGroupDetails(schemaName);
        int M = groupDetail.size();
        Map<Integer, String> groupDetailMap = new HashMap<>();
        Map<Integer, String> storageInstMap = new HashMap<>();
        Map<String, Integer> groupDetailReverseMap = new HashMap();
        Map<String, Integer> storageInstReverseMap = new HashMap();
        List<String> groupNames = new ArrayList<>(groupDetail.keySet());
        List<String> storageInsts =
            groupNames.stream().map(o -> groupDetail.get(o).storageInstId).collect(Collectors.toList());

        for (int i = 0; i < M; i++) {
            storageInstMap.put(i, storageInsts.get(i));
            groupDetailMap.put(i, groupNames.get(i));
            groupDetailReverseMap.put(groupNames.get(i), i);
            storageInstReverseMap.put(storageInsts.get(i), i);
        }

        Set<Integer> drainNodeIndexSet =
            drainNodeInfo.getDnInstIdList().stream().map(o -> storageInstReverseMap.getOrDefault(o, -1))
                .collect(Collectors.toSet());
        if (drainNodeIndexSet.contains(-1) || drainNodeIndexSet.size() < drainNodeInfo.getDnInstIdList().size()
            || drainNodeIndexSet.isEmpty()) {
            EventLogger.log(EventType.DDL_WARN,
                "drain node contains wrong storage_inst_id: " + drainNodeInfo.getDnInstIdList().toString());
        } else {
            int[] drainNodeIndexes = drainNodeIndexSet.stream().mapToInt(o -> o).toArray();

            Map<String, Long> tgDataSize = new HashMap<>();
            for (String tgName : toRebalancePgListGroupByTg.keySet()) {
                Long tgSize =
                    toRebalancePgListGroupByTg.get(tgName).stream().map(PartitionGroupStat::getTotalDiskSize).reduce(0L,
                        Long::sum);
                tgDataSize.put(tgName, tgSize);
            }
            List<String> tableGroupNames = validTgSet.stream().collect(Collectors.toList());
            tableGroupNames.sort(Comparator.comparingLong(key -> tgDataSize.get(key)).reversed());
            DataDistInfo dataDistInfo = DataDistInfo.fromSchemaAndInstMap(schemaName, storageInstMap, groupDetailMap);
            List<PolicyPartitionBalance.MoveInfo> moves = new ArrayList<>();
            for (int k = 0; k < tableGroupNames.size(); k++) {

                String tgName = tableGroupNames.get(k);
                List<PartitionGroupStat> toRebalancePgList = toRebalancePgListGroupByTg.get(tgName);
                int N = toRebalancePgList.size();

                int[] originalPlace = new int[N];
                int[] targetPlace = new int[N];
                double[] partitionSize = new double[N];
                Map<Integer, PartitionGroupStat> toRebalancePgMap = new HashMap<>();

                for (int i = 0; i < N; i++) {
                    PartitionGroupStat partitionGroupStat = toRebalancePgList.get(i);
                    toRebalancePgMap.put(i, partitionGroupStat);
                    String groupKey = partitionGroupStat.getFirstPartition().getLocation().getGroupKey();
                    originalPlace[i] = groupDetailReverseMap.get(groupKey);
                    partitionSize[i] = partitionGroupStat.getDataRows();
                    targetPlace[i] = originalPlace[i];
                }
                Date startTime = new Date();
                String logInfo = String.format(
                    "[schema %s, tablegroup %s] start to solve move partition problem: M=%d, N=%d, originalPlace=%s, partitionSize=%s",
                    schemaName, tgName, M, N, Arrays.toString(originalPlace), Arrays.toString(partitionSize));
                EventLogger.log(EventType.REBALANCE_INFO, logInfo);

                double originalMu = MixedModel.caculateBalanceFactor(M, N, originalPlace, partitionSize).getValue();
                Solution solution = null;
                if (toRebalanceSequentialTg.contains(tableGroupNames.get(k))) {
                    solution =
                        MixedModel.solveMoveSequentialPartition(M, N, originalPlace, partitionSize, drainNodeIndexes);
                } else if (k < MAX_TABLEGROUP_SOLVED_BY_LP) {
                    solution =
                        MixedModel.solveMovePartition(M, N, originalPlace, partitionSize, solveLevel, drainNodeIndexes);
                } else {
                    solution =
                        MixedModel.solveMovePartitionByGreedy(M, N, originalPlace, partitionSize, drainNodeIndexes);
                }
                if (solution.withValidSolve) {
                    Date endTime = new Date();
                    Long costMillis = endTime.getTime() - startTime.getTime();
                    logInfo =
                        String.format(
                            "[schema %s, tablegroup %s] get solution in %d ms: solved via %s, originalMu = %f, mu=%f, targetPlace=%s",
                            schemaName, tgName, costMillis, solution.strategy, originalMu, solution.mu,
                            Arrays.toString(solution.targetPlace));
                    EventLogger.log(EventType.REBALANCE_INFO, logInfo);

//                double originalFactor = caculateBalanceFactor(M, N, originalPlace, partitionSize);
//                double targetFactor = caculateBalanceFactor(M, N, solution.targetPlace, partitionSize);
//                if (originalFactor - targetFactor > TOLORANT_BALANCE_ERR) {
//                    targetPlace = solution.targetPlace;
//                }
                    targetPlace = solution.targetPlace;

                    for (int i = 0; i < N; i++) {
                        if (targetPlace[i] != originalPlace[i]) {
                            moves.add(new PolicyPartitionBalance.MoveInfo(toRebalancePgMap.get(i).getFirstPartition(),
                                toRebalancePgMap.get(i).getTgName(),
                                groupDetailMap.get(targetPlace[i]),
                                toRebalancePgMap.get(i).getDataRows(),
                                toRebalancePgMap.get(i).getTotalDiskSize()
                            ));
                        }

                    }
                }
                dataDistInfo.appendTgDataDist(tgName, toRebalancePgList, originalPlace, targetPlace);
            }

            // sort by dataSize desc.
            moves.sort(Comparator.comparingLong(o -> -o.dataSize));
//        int toIndex = Math.min(options.maxActions * 20, moves.size());
//        moves = moves.subList(0, toIndex);
            Map<String, List<PolicyPartitionBalance.MoveInfo>> movesGroupByTg = moves.stream().collect(
                Collectors.groupingBy(o -> o.tgName, Collectors.mapping(o -> o, Collectors.toList()))
            );
            long maxTaskUnitSize = ec.getParamManager().getLong(REBALANCE_MAX_UNIT_SIZE);
            if (maxTaskUnitSize < 1024) {
                maxTaskUnitSize = options.maxTaskUnitSize;
            }
            List<BalanceAction> moveDataActions = new ArrayList<>();
            for (String tgName : movesGroupByTg.keySet()) {
                List<PolicyPartitionBalance.MoveInfo> movesGroup = movesGroupByTg.get(tgName);
                List<ActionMovePartitions> movePartitionsList =
                    PolicyPartitionBalance.shuffleToGroup(schemaName, movesGroup, maxTaskUnitSize, stats);
                moveDataActions.addAll(movePartitionsList);
            }
            String distLogInfo =
                String.format("[schema %s] estimated data distribution: %s", schemaName,
                    JSON.toJSONString(dataDistInfo));
            EventLogger.log(EventType.REBALANCE_INFO, distLogInfo);
            ActionWriteDataDistLog actionWriteDataDistLog = new ActionWriteDataDistLog(schemaName, dataDistInfo);
            moveDataActions.add(actionWriteDataDistLog);

            // remove broadcast tables
            ActionDropBroadcastTable dropBroadcastTable =
                new ActionDropBroadcastTable(schemaName, drainNodeInfo.getDnInstIdList());

            // Hide all the to-removed groups and notify all cn nodes to clean the to-removed group data sources
            List<String> toRemoveGroupNames = new ArrayList<>();
            toRemoveGroupNames.addAll(removedGroups);
            DropDbGroupHideMetaTask hideDbGroupMetaTask =
                new DropDbGroupHideMetaTask(schemaName, toRemoveGroupNames);
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

            ActionTaskAdapter drainNodeValidateTaskAdapter =
                new ActionTaskAdapter(schemaName, drainNodeValidateTask);

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

            Map<String, List<PartitionStat>> toRebalanceSingleTableListGroupByTg =
                GeneralUtil.emptyIfNull(pgList).stream()
                    .collect(
                        Collectors.groupingBy(o -> o.getTgName(), Collectors.mapping(o -> o, Collectors.toList())))
                    .entrySet().stream()
                    .filter(
                        o -> LocalityInfoUtils.withSingleTableBalanceLocality(tableGroupConfigMap.get(o.getKey())))
                    .collect(
                        Collectors.toMap(o -> o.getKey(),
                            o -> o.getValue().stream().flatMap(p -> p.partitions.stream())
                                .collect(Collectors.toList())));
            List<BalanceAction> moveSingleTableActions =
                generateMoveSingleTableAction(schemaName, toRebalanceSingleTableListGroupByTg, groupDetail,
                    drainNodeInfo,
                    stats,
                    options,
                    ec);
            // combine actions
            actions.add(lock);
            actions.add(drainNodeValidateTaskAdapter);
            actions.add(actionUpdateGroupStatus);
            actions.add(actionTopologySyncThenReleaseXLockTask);
            actions.add(cleanRemovedDbLocalityMetaAction);
            actions.addAll(syncTableGroupsAction);
            actions.addAll(moveDataActions);
            actions.addAll(moveSingleTableActions);
            actions.add(dropBroadcastTable);
            actions.add(hideToRemovedGroupMetaAction);
            actions.add(syncNewTopologyAction);
            actions.addAll(dropPhyDbActions);
            actions.add(cleanRemovedDbGroupMetaAction);

//            Map<String, List<PartitionStat>> toRebalanceSingleTableListGroupByTg =
//                GeneralUtil.emptyIfNull(pgList).stream()
//                    .collect(Collectors.groupingBy(o -> o.getTgName(), Collectors.mapping(o -> o, Collectors.toList())))
//                    .entrySet().stream()
//                    .filter(o -> LocalityInfoUtils.withSingleTableBalanceLocality(tableGroupConfigMap.get(o.getKey())))
//                    .collect(
//                        Collectors.toMap(o -> o.getKey(),
//                            o -> o.getValue().stream().flatMap(p -> p.partitions.stream())
//                                .collect(Collectors.toList())));
//            List<BalanceAction> moveSingleTableActions =
//                generateMoveSingleTableAction(schemaName, toRebalanceSingleTableListGroupByTg, groupDetail,
//                    drainNodeInfo,
//                    stats,
//                    options);

        }
        return actions;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class DrainStoragePool {
        @JSONField(name = "storage_pool")
        private List<String> storagePoolList = new ArrayList<>();

        public Boolean holdsEmptyStoragePool() {
            return storagePoolList.isEmpty();
        }

        public static DrainStoragePool parse(String drainStoragePoolInfo) {
            DrainStoragePool drainStoragePool = new DrainStoragePool();
            if (TStringUtil.isBlank(drainStoragePoolInfo)) {
                return drainStoragePool;
            }
            drainStoragePoolInfo = StringUtils.trim(drainStoragePoolInfo);
            String[] drainStoragePools = drainStoragePoolInfo.split(",");
            drainStoragePool.setStoragePoolList(Arrays.stream(drainStoragePools).collect(Collectors.toList()));
            return drainStoragePool;
        }

        public boolean containsStoragePool(String primaryStoragePoolName) {
            return storagePoolList.contains(primaryStoragePoolName);
        }

        public boolean hasIntersectionWithStoragePoolList(List<String> storagePoolNameList) {
            return storagePoolNameList.stream().anyMatch(o -> storagePoolList.contains(o));
        }

        public Set<String> intersectWithStoragePoolList(List<String> storagePoolNameList) {
            return storagePoolNameList.stream().filter(o -> storagePoolList.contains(o))
                .collect(Collectors.toSet());
        }
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

        public void removeInvalidStorageInsts(Set<String> invalidStorageInsts) {
            this.dnInstIdList = this.dnInstIdList.stream().filter(o -> !invalidStorageInsts.contains(o)).collect(
                Collectors.toList());

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
