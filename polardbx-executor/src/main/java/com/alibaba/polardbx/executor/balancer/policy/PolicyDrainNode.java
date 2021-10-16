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
import com.alibaba.polardbx.executor.balancer.action.ActionDrainCDCStorage;
import com.alibaba.polardbx.executor.balancer.action.ActionLockResource;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroup;
import com.alibaba.polardbx.executor.balancer.action.ActionMoveGroups;
import com.alibaba.polardbx.executor.balancer.action.ActionMovePartition;
import com.alibaba.polardbx.executor.balancer.action.ActionUpdateNodeStatus;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.GroupStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.NodeInfo;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlRebalance;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

        BalanceAction updateStatusNotReady =
            new ActionUpdateNodeStatus(
                ec.getSchemaName(),
                new ArrayList<>(dnInstIdList),
                Collections.emptyList(),
                StorageInfoRecord.STORAGE_STATUS_NOT_READY);

        BalanceAction updateStatusRemoved =
            new ActionUpdateNodeStatus(
                ec.getSchemaName(),
                new ArrayList<>(dnInstIdList),
                new ArrayList<>(cnIpPortList),
                StorageInfoRecord.STORAGE_STATUS_REMOVED);

        BalanceAction drainCDC = new ActionDrainCDCStorage(ec.getSchemaName(), dnInstIdList);

        String resName = ActionUtils.genRebalanceClusterName();
        ActionLockResource lock = new ActionLockResource(null, resName);

        List<BalanceAction> moveDataActions = new ArrayList<>();
        moveDataActions.add(lock);
        moveDataActions.add(updateStatusNotReady);
        moveDataActions.addAll(
            schemaNameList.stream()
                .flatMap(schema -> applyToDb(ec, stats.get(schema), options, schema).stream())
                .collect(Collectors.toList()));
        moveDataActions.add(drainCDC);
        moveDataActions.add(updateStatusRemoved);
        return moveDataActions;
    }

    /**
     * Drain-out groups of a storage instance
     */
    @Override
    public List<BalanceAction> applyToShardingDb(ExecutionContext ec,
                                                 BalanceOptions options,
                                                 BalanceStats stats,
                                                 String schema) {
        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        List<PolicyDrainNode.DnDiskInfo> dnDiskInfo = DnDiskInfo.parseToList(options.diskInfo);
        List<GroupStats.GroupsOfStorage> groupList = stats.getGroups();
        if (CollectionUtils.isEmpty(groupList)) {
            return Collections.emptyList();
        }
        Map<String, Long> groupDataSizeMap = groupList.stream()
            .flatMap(x -> x.getGroupDataSizeMap().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

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
                if (candidate.moveInGroup(schema, groupName, dataSize)) {
                    action = new ActionMoveGroup(schema, Arrays.asList(groupName),
                        candidate.getDnDiskInfo().getInstance(), options.debug);
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

        String name = ActionUtils.genRebalanceResourceName(SqlRebalance.RebalanceTarget.DATABASE, schema);
        ActionLockResource lock = new ActionLockResource(schema, name);

        return Arrays.asList(lock, new ActionMoveGroups(schema, actions));
    }

    private List<MoveInDn> prepareMoveInDns(DrainNodeInfo drainNodeInfo, List<DnDiskInfo> dnDiskInfo,
                                            List<GroupStats.GroupsOfStorage> groupList) {
        List<MoveInDn> moveInDnList;
        if (!dnDiskInfo.isEmpty()) {
            // choose dn list from disk_info
            moveInDnList = dnDiskInfo.stream()
                .filter(x -> !drainNodeInfo.containsDnInst(x.getInstance()))
                .map(MoveInDn::new)
                .collect(Collectors.toList());
        } else {
            // choose dn list from metadb
            moveInDnList = groupList
                .stream()
                .map(x -> x.storageInst)
                .distinct()
                .filter(x -> !drainNodeInfo.containsDnInst(x))
                .map(MoveInDn::new)
                .collect(Collectors.toList());
        }
        if (moveInDnList.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "no available data-node to move in");
        }
        return moveInDnList;
    }

    /**
     * Drain-out all partitions of a storage instance
     */
    @Override
    public List<BalanceAction> applyToPartitionDb(ExecutionContext ec,
                                                  BalanceOptions options,
                                                  BalanceStats stats,
                                                  String schemaName) {
        List<BalanceAction> actions = new ArrayList<>();

        DrainNodeInfo drainNodeInfo = DrainNodeInfo.parse(options.drainNode);
        Map<String, String> groupToInst = Maps.newHashMap();
        List<PolicyDrainNode.DnDiskInfo> dnDiskInfo = DnDiskInfo.parseToList(options.diskInfo);
        List<GroupStats.GroupsOfStorage> groupList = stats.getGroups();
        if (CollectionUtils.isEmpty(groupList)) {
            return Collections.emptyList();
        }

        List<MoveInDn> availableInstList = prepareMoveInDns(drainNodeInfo, dnDiskInfo, groupList);

        Set<String> moved = Sets.newHashSet();
        for (PartitionStat partition : stats.getPartitionStats()) {
            String inst =
                getStorageInstOfGroup(groupToInst, partition.getSchema(), partition.getLocation().getGroupKey());
            if (drainNodeInfo.containsDnInst(inst) && !moved.contains(partition.getPartitionName())) {
                String targetInst = chooseTargetInst(availableInstList);
                ActionMovePartition action = ActionMovePartition.createMoveToInst(schemaName, partition, targetInst);
                actions.add(action);
                moved.add(partition.getPartitionName());
            }
        }

        // remove broadcast tables
        TableGroupConfig broadcastTg =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getBroadcastTableGroupConfig();
        List<TablePartRecordInfoContext> tables = broadcastTg.getTables();
        LOG.info("remove broadcast tables: " + tables);

        Map<String, GroupDetailInfoRecord> groupMap = PolicyUtils.getGroupDetails(schemaName);
        for (PartitionGroupRecord pg : GeneralUtil.emptyIfNull(broadcastTg.getPartitionGroupRecords())) {
            String phyDbName = pg.getPhy_db();
            String groupName = GroupInfoUtil.buildGroupNameFromPhysicalDb(phyDbName);
            GroupDetailInfoRecord group = groupMap.get(groupName);
            // // TODO(moyi) remove this partition group
            if (drainNodeInfo.containsDnInst(group.getStorageInstId())) {

            }

        }

        // TODO(moyi) remove groups on the storage
//        for (String groupName : groupMap.keySet()) {
//            DropStorageGroupTask dropGroup = new DropStorageGroupTask(schemaName, groupName);
//            actions.add(dropGroup);
//        }

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

        /**
         * Validate the drain_node info
         * 1. Whether all dn instance are existed
         * 2. Whether all dn instance could be removed
         */
        public void validate() {
            if (CollectionUtils.isEmpty(dnInstIdList)) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, "empty dn list");
            }

            Map<String, StorageInstHaContext> allStorage =
                StorageHaManager.getInstance().refreshAndGetStorageInstHaContextCache();

            for (String dnInst : dnInstIdList) {
                StorageInstHaContext storage = allStorage.get(dnInst);
                if (storage == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        "storage not found: " + dnInst);
                }
                if (!storage.isInstanceDeletable()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        "storage not deletable: " + dnInst);
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

        private Set<String> nodeToHostPort(List<NodeInfo> serverList) {
            return serverList.stream().map(NodeInfo::getHostPort).collect(Collectors.toSet());
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
