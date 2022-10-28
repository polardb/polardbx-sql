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
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Locality Calculus. via Locality Desc
 *
 * @author taojinkun
 * @since 2022/03
 */
public class LocalityInfoUtils {
    public static LocalityDesc Intersect(LocalityDesc localityDesc1, LocalityDesc localityDesc2) {
        if (localityDesc1.holdEmptyDnList()) {
            return localityDesc2;
        } else if (localityDesc2.holdEmptyDnList()) {
            return localityDesc1;
        } else {
            List<String> dnList =
                localityDesc1.getDnList().stream().filter(dn -> localityDesc2.getDnList().contains(dn))
                    .collect(Collectors.toList());
            return new LocalityDesc(dnList);
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
            HashSet<String> dnSet = new HashSet<>(localityDesc1.getDnList());
            dnSet.addAll(localityDesc2.getDnList());
            return new LocalityDesc(dnSet.stream().collect(Collectors.toList()));
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
                                                          Collection<String> partitions, CheckAction checkAction) {
        String dbLocality = LocalityManager.getInstance().getLocalityOfDb(
                DbInfoManager.getInstance().getDbInfo(schemaName).id)
            .getLocality();
        LocalityDesc schemaLocality = LocalityDesc.parse(dbLocality);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager().
            getTableGroupConfigByName(tableGroupName);
        LocalityDesc allowedTableGroupLocality = Intersect(schemaLocality, tableGroupConfig.getLocalityDesc());

        for (String partition : partitions) {
            LocalityDesc partitionGroupLocality = LocalityDesc.parse(
                tableGroupConfig.getPartitionGroupByName(partition).getLocality()
            );
            LocalityDesc allowedPartitionGroupLocality =
                partitionGroupLocality.holdEmptyDnList() ? allowedTableGroupLocality : partitionGroupLocality;
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
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        LocalityDesc localityDesc = tableGroupConfig.getLocalityDesc();
        if (localityDesc.holdEmptyDnList()) {
            return groupDetailInfoExRecordList;
        } else {
            return groupDetailInfoExRecordList.stream().filter(o -> localityDesc.matchStorageInstance(o.storageInstId))
                .collect(Collectors.toList());
        }
    }

    public static Map<String, List<GroupDetailInfoExRecord>> getAllowedGroupInfoOfPartitionGroup(String schemaName,
                                                                                                 String tableGroupName) {
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(partitionGroup -> !StringUtils.isEmpty(partitionGroup.getLocality())).collect(Collectors.toList());

        Map<String, List<GroupDetailInfoExRecord>> groupInfoOfPartitionGroups = new HashMap<>();
        for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
            LocalityDesc localityDesc = LocalityDesc.parse(partitionGroupRecord.locality);
            if (!localityDesc.holdEmptyDnList()) {
                List<GroupDetailInfoExRecord> records =
                    groupDetailInfoExRecordList.stream().filter(o -> localityDesc.matchStorageInstance(o.storageInstId))
                        .collect(Collectors.toList());
                groupInfoOfPartitionGroups.put(partitionGroupRecord.partition_name, records);
            }
        }
        return groupInfoOfPartitionGroups;
    }

    public static List<GroupDetailInfoExRecord> getAllowedGroupInfoOfPartitionGroup(String schemaName,
                                                                                    String tableGroupName,
                                                                                    String partitiongGroupName) {
        List<GroupDetailInfoExRecord> groupDetailInfoExRecordList = TableGroupLocation.getOrderedGroupList(schemaName);
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
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
        return records;
    }
}
