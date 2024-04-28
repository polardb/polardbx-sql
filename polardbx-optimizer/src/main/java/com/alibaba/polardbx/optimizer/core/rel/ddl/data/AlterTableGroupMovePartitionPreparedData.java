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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterTableGroupMovePartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupMovePartitionPreparedData() {
    }

    /*
    key: target storage instanceId
    val: partitions to be moved
     */
    private Map<String, Set<String>> targetPartitionsLocation;

    @Override
    public Map<String, String> getNewPartitionLocalities() {
        return newPartitionLocalities;
    }

    @Override
    public void setNewPartitionLocalities(Map<String, String> newPartitionLocalities) {
        this.newPartitionLocalities = newPartitionLocalities;
    }

    private Map<String, String> newPartitionLocalities = new HashMap<>();

    public Map<String, Set<String>> getTargetPartitionsLocation() {
        return targetPartitionsLocation;
    }

    public void setTargetPartitionsLocation(
        Map<String, Set<String>> targetPartitionsLocation) {
        this.targetPartitionsLocation = targetPartitionsLocation;
    }

    public boolean isUsePhysicalBackfill() {
        return usePhysicalBackfill;
    }

    public void setUsePhysicalBackfill(boolean usePhysicalBackfill) {
        this.usePhysicalBackfill = usePhysicalBackfill;
    }

    @Override
    public void prepareInvisiblePartitionGroup() {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(getTableGroupName());
        Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
        List<GroupDetailInfoExRecord> groupDetailInfoExRecords = getTargetGroupDetailInfoExRecords();
        List<String> newPartitionNames = new ArrayList<>();
        List<GroupDetailInfoExRecord> groupDetailInfos = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : targetPartitionsLocation.entrySet()) {
            List<GroupDetailInfoExRecord> groupDetailInfoExRecordsForSpecInst =
                groupDetailInfoExRecords.stream().filter(o -> o.storageInstId.equalsIgnoreCase(entry.getKey())).collect(
                    Collectors.toList());
            int i = 0;
            int targetDbCount = groupDetailInfoExRecordsForSpecInst.size();
            for (String newPartitionName : entry.getValue()) {
                PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                partitionGroupRecord.visible = 0;
                partitionGroupRecord.partition_name = newPartitionName;
                partitionGroupRecord.tg_id = tableGroupId;

                partitionGroupRecord.phy_db = groupDetailInfoExRecordsForSpecInst.get(i % targetDbCount).phyDbName;

                partitionGroupRecord.locality = "";
                LocalityDesc localityDesc = LocalityInfoUtils.parse(newPartitionLocalities.get(newPartitionName));
                if (!localityDesc.holdEmptyLocality()) {
                    partitionGroupRecord.locality = localityDesc.toString();
                }
                partitionGroupRecord.pax_group_id = 0L;
                inVisiblePartitionGroups.add(partitionGroupRecord);
                newPartitionNames.add(newPartitionName);
                groupDetailInfos.add(groupDetailInfoExRecordsForSpecInst.get(i % targetDbCount));
            }
        }
        setInvisiblePartitionGroups(inVisiblePartitionGroups);
        setNewPartitionNames(newPartitionNames);
        setOldPartitionNames(newPartitionNames);
        setTargetGroupDetailInfoExRecords(groupDetailInfos);
    }
}
