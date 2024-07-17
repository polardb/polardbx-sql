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

package com.alibaba.polardbx.optimizer.locality;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Locality Calculus. via Locality Desc
 *
 * @author taojinkun
 * @since 2022/03
 */
public class LocalityInfoUtils {

    private static final String DN_PREFIX = "dn=";

    private static final String BALANCE_PREFIX = "balance_single_table=";

    private static final String STORAGE_POOL_PREFIX = "storage_pools=";

    public static LocalityDesc Intersect(LocalityDesc localityDesc1, LocalityDesc localityDesc2) {
        if (localityDesc1.holdEmptyDnList()) {
            return localityDesc2;
        } else if (localityDesc2.holdEmptyDnList()) {
            return localityDesc1;
        } else {
            Set<String> dnSet =
                localityDesc1.getDnSet().stream().map(String::new)
                    .collect(Collectors.toSet());
            dnSet.retainAll(localityDesc2.getDnSet());
            return new LocalityDesc(dnSet);
        }
    }

    public static String allocatePhyDb(String schemaName, LocalityDesc localityDesc) {
        List<String> groupNames =
            getGroupDetails(schemaName, localityDesc.getDnList()).values().stream().map(o -> o.getGroupName()).collect(
                Collectors.toList());
        if (groupNames.isEmpty()) {
            String errMessage =
                String.format("unable to allocate physical db for invalid locality [%s]", localityDesc.toString());
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMessage);
        }
        Random random = new Random();
        int index = random.nextInt(groupNames.size()); // 生成一个0到list.size()-1之间的随机整数
        String groupName = groupNames.get(index); // 获取对应索引的元素
        return GroupInfoUtil.buildPhysicalDbNameFromGroupName(groupName);
    }

    public static Map<String, GroupDetailInfoRecord> getGroupDetails(String schema, List<String> storageInsts) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
            accessor.setConnection(conn);
            List<GroupDetailInfoRecord> records =
                accessor.getGroupDetailInfoByInstIdAndDbName(InstIdUtil.getInstId(), schema)
                    .stream().filter(o -> storageInsts.contains(o.storageInstId)).collect(Collectors.toList());
            return records.stream().collect(Collectors.toMap(GroupDetailInfoRecord::getGroupName, x -> x));
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static LocalityDesc Intersect(LocalityDesc... localityDescs) {
        return Arrays.stream(localityDescs).reduce(LocalityInfoUtils::Intersect).get();
    }

    public static LocalityDesc Union(LocalityDesc localityDesc1, LocalityDesc localityDesc2) {
        if (localityDesc1.holdEmptyDnList()) {
            return localityDesc1;
        } else if (localityDesc2.holdEmptyDnList()) {
            return localityDesc2;
        } else {
            Set<String> dnSet = localityDesc1.getDnSet().stream().map(String::new).collect(Collectors.toSet());
            dnSet.addAll(localityDesc2.getDnList());
            return new LocalityDesc(dnSet);
        }
    }

    public static LocalityDesc Union(LocalityDesc... localityDescs) {
        return Arrays.stream(localityDescs).reduce(LocalityInfoUtils::Union).get();
    }

    public static class CheckAction {
        public boolean checkPartition(String partition, LocalityDesc localityDesc) {
            return true;
        }
    }

    public static class CollectAction extends CheckAction {
        final List<LocalityDesc> PartitionsLocalityDesc = new ArrayList<>();

        public List<LocalityDesc> getPartitionsLocalityDesc() {
            return PartitionsLocalityDesc;
        }

        @Override
        public boolean checkPartition(String partition, LocalityDesc localityDesc) {
            PartitionsLocalityDesc.add(localityDesc);
            return true;
        }
    }

    public static void checkTableGroupLocalityCompatiable(String schemaName, String tableGroupName,
                                                          Map<String, String> moveOut,
                                                          Map<String, String> targetLocality) {
        Map<String, List<GroupDetailInfoExRecord>> allowedGroup =
            getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroupName);
        for (String partition : moveOut.keySet()) {
            if (targetLocality.containsKey(partition)) {
                LocalityDesc targetPartLocality = LocalityInfoUtils.parse(targetLocality.get(partition));
                if (!targetPartLocality.matchStorageInstance(moveOut.get(partition))) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                        String.format("alter tablegroup operation not compatebale with partition [%s].[%s] locality ",
                            tableGroupName, partition), null);
                }
            } else {
                Set<String> targetAllowedDnIds =
                    allowedGroup.get(partition).stream().map(o -> o.getStorageInstId()).collect(
                        Collectors.toSet());
                if (!targetAllowedDnIds.contains(moveOut.get(partition))) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                        String.format("alter tablegroup operation not compatebale with partition [%s].[%s] locality ",
                            tableGroupName, partition), null);
                }
            }
        }
    }

    public static void checkTableGroupLocalityCompatiable(String schemaName, String tableGroupName,
                                                          Collection<String> partitions, CheckAction checkAction) {
        String dbLocality = LocalityManager.getInstance().getLocalityOfDb(
                DbInfoManager.getInstance().getDbInfo(schemaName).id)
            .getLocality();
        LocalityDesc schemaLocality = LocalityInfoUtils.parse(dbLocality);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager().
            getTableGroupConfigByName(tableGroupName);
        Map<String, List<GroupDetailInfoExRecord>> allowedGroupInfo =
            getAllowedGroupInfoOfPartitionGroup(schemaName, tableGroupName);

        for (String partition : partitions) {
            List<String> allowedPartitionGroupDns =
                allowedGroupInfo.get(partition).stream().map(o -> o.storageInstId).collect(Collectors.toList());
            LocalityDesc allowedPartitionGroupLocality = LocalityDesc.parse(
                LocalityDesc.DN_PREFIX +
                    org.apache.commons.lang.StringUtils.join(allowedPartitionGroupDns, ","));
            if (!checkAction.checkPartition(partition, allowedPartitionGroupLocality)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                    String.format("alter tablegroup operation not compatebale with partition [%s].[%s] locality ",
                        tableGroupConfig.getTableGroupRecord().getTg_name(), partition), null);
            }
        }
    }

    public static boolean equals(String locality1, String locality2) {
        LocalityDesc l1 = LocalityDesc.parse(locality1);
        LocalityDesc l2 = LocalityDesc.parse(locality2);
        return l1.toString().equals(l2.toString());
    }

    public static List<GroupDetailInfoExRecord> getAllowedGroupInfoOfTableGroup(String schemaName,
                                                                                String tableGroupName) {
        // we would only get dnSet, not full dnSet here.
        LocalityDesc dbLocalityDesc =
            LocalityInfoUtils.parse(LocalityManager.getInstance().getLocalityOfDb(schemaName).getLocality());
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        LocalityDesc localityDesc = LocalityInfoUtils.parse(tableGroupConfig.getLocalityDesc().toString());
        if (localityDesc.holdEmptyDnList()) {
            return groupDetailInfoExRecordList.stream()
                .filter(o -> dbLocalityDesc.matchStorageInstance(o.storageInstId))
                .collect(Collectors.toList());
        } else {
            return groupDetailInfoExRecordList.stream().filter(o -> localityDesc.matchStorageInstance(o.storageInstId))
                .collect(Collectors.toList());
        }
    }

    public static Map<String, List<GroupDetailInfoExRecord>> getAllowedGroupInfoOfPartitionGroup(String schemaName,
                                                                                                 String tableGroupName) {
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        LocalityDesc dbLocalityDesc =
            LocalityInfoUtils.parse(LocalityManager.getInstance().getLocalityOfDb(schemaName).getLocality());
        List<GroupDetailInfoExRecord> tgRecords =
            groupDetailInfoExRecordList.stream().filter(o -> dbLocalityDesc.matchStorageInstance(o.storageInstId))
                .collect(Collectors.toList());
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        LocalityDesc tgLocalityDesc = LocalityInfoUtils.parse(tableGroupConfig.getLocalityDesc().toString());
        if (!tgLocalityDesc.holdEmptyDnList()) {
            tgRecords =
                groupDetailInfoExRecordList.stream().filter(o -> tgLocalityDesc.matchStorageInstance(o.storageInstId))
                    .collect(Collectors.toList());
        }

        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
//            .filter(partitionGroup -> !StringUtils.isEmpty(partitionGroup.getLocality())).collect(Collectors.toList());

        Map<String, List<GroupDetailInfoExRecord>> groupInfoOfPartitionGroups =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
            LocalityDesc localityDesc = LocalityInfoUtils.parse(partitionGroupRecord.locality);
            if (!localityDesc.holdEmptyDnList()) {
                List<GroupDetailInfoExRecord> records =
                    groupDetailInfoExRecordList.stream().filter(o -> localityDesc.matchStorageInstance(o.storageInstId))
                        .collect(Collectors.toList());
                groupInfoOfPartitionGroups.put(partitionGroupRecord.partition_name, records);
            } else {
                groupInfoOfPartitionGroups.put(partitionGroupRecord.partition_name, tgRecords);
            }
        }
        return groupInfoOfPartitionGroups;
    }

    public static Map<String, String> getAllowedStoragePoolOfPartitionGroup(String schemaName,
                                                                            String tableGroupName) {
        String dbLocality = LocalityManager.getInstance().getLocalityOfDb(schemaName).getLocality();
        String tgStoragePoolName = LocalityInfoUtils.parse(dbLocality).getPrimaryStoragePoolName();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (!StringUtils.isEmpty(tableGroupConfig.getLocalityDesc().getPrimaryStoragePoolName())) {
            tgStoragePoolName = tableGroupConfig.getLocalityDesc().getPrimaryStoragePoolName();
        }
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        Map<String, String> storagePoolOfPartitionGroups = new HashMap<>();
        for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
            String storagePoolName = tgStoragePoolName;
            LocalityDesc localityDesc = LocalityDesc.parse(partitionGroupRecord.locality);
            if (!StringUtils.isEmpty(localityDesc.getPrimaryStoragePoolName())) {
                storagePoolName = localityDesc.getPrimaryStoragePoolName();
            }
            storagePoolName = Optional.ofNullable(storagePoolName).orElse(StoragePoolManager.DEFAULT_STORAGE_POOL_NAME);
            storagePoolOfPartitionGroups.put(partitionGroupRecord.partition_name, storagePoolName);
        }
        return storagePoolOfPartitionGroups;
    }

    public static List<GroupDetailInfoExRecord> getAllowedGroupInfoOfPartitionGroup(String schemaName,
                                                                                    String tableGroupName,
                                                                                    String firstLevelPartition,
                                                                                    Set<String> partitionGroups,
                                                                                    boolean ignorePartGroupLocality) {
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        LocalityDesc dbLocalityDesc =
            LocalityInfoUtils.parse(LocalityManager.getInstance().getLocalityOfDb(schemaName).getLocality());
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        List<GroupDetailInfoExRecord> records = groupDetailInfoExRecordList.stream()
            .filter(o -> dbLocalityDesc.fullMatchStorageInstance(o.storageInstId))
            .collect(Collectors.toList());
        List<GroupDetailInfoExRecord> finalRecords = new ArrayList<>();
        for (GroupDetailInfoExRecord record : records) {
            finalRecords.add(record);
        }
        LocalityDesc tgLocalityDesc = tableGroupConfig.getLocalityDesc();
        if (!tgLocalityDesc.holdEmptyDnList()) {
            finalRecords = records.stream()
                .filter(o -> tableGroupConfig.getLocalityDesc().matchStorageInstance(o.storageInstId))
                .collect(Collectors.toList());
        }
        if (!ignorePartGroupLocality) {
            List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(partitionGroup -> partitionGroups.contains(partitionGroup.getPartition_name()))
                .collect(Collectors.toList());
            if (GeneralUtil.isEmpty(partitionGroupRecords)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                    String.format("can't find the partitionGroup [%s].[%s] ",
                        tableGroupConfig.getTableGroupRecord().getTg_name(), firstLevelPartition));
            }
            LocalityDesc localityDesc = LocalityInfoUtils.parse(partitionGroupRecords.get(0).locality);
            if (!localityDesc.holdEmptyDnList()) {
                finalRecords =
                    records.stream().filter(o -> localityDesc.matchStorageInstance(o.storageInstId))
                        .collect(Collectors.toList());
            }
        }
        return finalRecords;
    }

    public static Boolean withoutRestrictedAllowedGroup(String schemaName, Long tableGroupId,
                                                        String partitiongGroupName) {
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(partitionGroup -> partitionGroup.getPartition_name().equalsIgnoreCase(partitiongGroupName))
            .collect(Collectors.toList());
        LocalityDesc localityDesc = LocalityDesc.parse(partitionGroupRecords.get(0).locality);
        List<GroupDetailInfoExRecord> records = groupDetailInfoExRecordList.stream()
            .filter(o -> tableGroupConfig.getLocalityDesc().matchStorageInstance(o.storageInstId))
            .collect(Collectors.toList());
        if (!localityDesc.holdEmptyDnList()) {
            records =
                groupDetailInfoExRecordList.stream().filter(o -> localityDesc.matchStorageInstance(o.storageInstId))
                    .collect(Collectors.toList());
        }
        return groupDetailInfoExRecordList.equals(records);
    }

    public static Boolean withoutRestrictedLocality(List<GroupDetailInfoExRecord> groupDetailInfoExRecordList,
                                                    TableGroupConfig tableGroupConfig) {
        LocalityDesc tgLocalityDesc = LocalityDesc.parse(tableGroupConfig.getTableGroupRecord().locality);
        if (!tgLocalityDesc.holdEmptyDnList()) {
            List<GroupDetailInfoExRecord> records =
                groupDetailInfoExRecordList.stream().filter(o -> tgLocalityDesc.matchStorageInstance(o.storageInstId))
                    .collect(Collectors.toList());
            if (groupDetailInfoExRecordList.size() != records.size()) {
                return false;
            }
        }
        for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
            LocalityDesc localityDesc = LocalityDesc.parse(partitionGroupRecord.locality);
            if (!localityDesc.holdEmptyDnList()) {
                List<GroupDetailInfoExRecord> records =
                    groupDetailInfoExRecordList.stream().filter(o -> localityDesc.matchStorageInstance(o.storageInstId))
                        .collect(Collectors.toList());
                if (groupDetailInfoExRecordList.size() != records.size()) {
                    return false;
                }
            }
        }
        return true;
    }

    public static Boolean withSingleTableBalanceLocality(TableGroupConfig tableGroupConfig) {
        LocalityDesc tgLocalityDesc = LocalityDesc.parse(tableGroupConfig.getTableGroupRecord().locality);
        return tgLocalityDesc.getBalanceSingleTable();
    }

    public static LocalityDesc parse(String str) {
        LocalityDesc result = LocalityDesc.parse(str);
        if (!result.getStoragePoolNames().isEmpty()) {
            List<String> storagePoolNames = result.getStoragePoolNames();
            String primaryStoragePoolName = result.getPrimaryStoragePoolName();
            Set<String> fullDnSet = new HashSet<>();
            for (String storagePoolName : storagePoolNames) {
                if (StoragePoolManager.getInstance().inValidStoragePoolName(storagePoolName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format(
                        "invalid locality: '%s', storage pool name not defined '%s'",
                        str, storagePoolName));
                }
                StoragePoolInfo storagePoolInfo = StoragePoolManager.getInstance().getStoragePoolInfo(storagePoolName);
                fullDnSet.addAll(LocalityDesc.parse(DN_PREFIX + storagePoolInfo.getDnIds()).getDnSet());
            }
            StoragePoolInfo storagePoolInfo =
                StoragePoolManager.getInstance().getStoragePoolInfo(primaryStoragePoolName);
            result.setDnSet(LocalityDesc.parse(DN_PREFIX + storagePoolInfo.getDnIds()).getDnSet());
            result.setFullDnSet(fullDnSet);
            result.setPrimaryDnId(storagePoolInfo.getUndeletableDnId());
        }
        return result;
    }
}
