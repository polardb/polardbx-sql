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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlterTableGroupMergePartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupMergePartitionPreparedData() {
    }

    /*
    key: target partition
    value: partitions to be merge
    */
    private Map<String, List<String>> mergePartitions;
    /*
    key: target partition
    value: storage instance
    */
    private Pair<String, String> targetStorageInstPair;

    private boolean mergeSubPartition;
    private boolean hasSubPartition;
    //for non-template logical partitions merge
    private String newPhysicalPartName;
    private List<String> templatePartNames;
    private List<String> oldPartitionGroupNames;
    private List<String> newPartitionGroupNames;

    public LocalityDesc getTargetLocality() {
        return targetLocality;
    }

    public void setTargetLocality(LocalityDesc targetLocality) {
        this.targetLocality = targetLocality;
    }

    private LocalityDesc targetLocality;

    public Map<String, List<String>> getMergePartitions() {
        return mergePartitions;
    }

    public void setMergePartitions(Map<String, List<String>> mergePartitions) {
        this.mergePartitions = mergePartitions;
        List<String> oldPartitions = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : mergePartitions.entrySet()) {
            newPartitions.add(entry.getKey());
            oldPartitions.addAll(entry.getValue());
        }
        setOldPartitionNames(oldPartitions);
        setNewPartitionNames(newPartitions);
    }

    public Pair<String, String> getTargetStorageInstPair() {
        return targetStorageInstPair;
    }

    public void setTargetStorageInstPair(Pair<String, String> targetStorageInstPair) {
        this.targetStorageInstPair = targetStorageInstPair;
    }

    public boolean isMergeSubPartition() {
        return mergeSubPartition;
    }

    public void setMergeSubPartition(boolean mergeSubPartition) {
        this.mergeSubPartition = mergeSubPartition;
    }

    public boolean isHasSubPartition() {
        return hasSubPartition;
    }

    public void setHasSubPartition(boolean hasSubPartition) {
        this.hasSubPartition = hasSubPartition;
    }

    public List<String> getTemplatePartNames() {
        return templatePartNames;
    }

    public void setTemplatePartNames(List<String> templatePartNames) {
        this.templatePartNames = templatePartNames;
    }

    public String getNewPhysicalPartName() {
        return newPhysicalPartName;
    }

    public void setNewPhysicalPartName(String newPhysicalPartName) {
        this.newPhysicalPartName = newPhysicalPartName;
    }

    public List<String> getOldPartitionGroupNames() {
        return oldPartitionGroupNames;
    }

    public void setOldPartitionGroupNames(List<String> oldPartitionGroupNames) {
        this.oldPartitionGroupNames = oldPartitionGroupNames;
    }

    public List<String> getNewPartitionGroupNames() {
        return newPartitionGroupNames;
    }

    public void setNewPartitionGroupNames(List<String> newPartitionGroupNames) {
        this.newPartitionGroupNames = newPartitionGroupNames;
    }

    @Override
    public void prepareInvisiblePartitionGroup() {
        if (!mergeSubPartition && hasSubPartition) {
            List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
            TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(getTableGroupName());
            Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
            int targetDbCount = targetGroupDetailInfoExRecords.size();
            int i = 0;
            if (isUseTemplatePart()) {
                //the physical partitions will allign with the subpartition template after merge in the new logical partition
                for (String newTemplatePartName : templatePartNames) {
                    PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                    partitionGroupRecord.visible = 0;
                    partitionGroupRecord.partition_name = getNewPartitionNames().get(0) + newTemplatePartName;
                    partitionGroupRecord.tg_id = tableGroupId;

                    partitionGroupRecord.phy_db = targetGroupDetailInfoExRecords.get(i % targetDbCount).phyDbName;

                    partitionGroupRecord.locality = "";
                    partitionGroupRecord.pax_group_id = 0L;
                    inVisiblePartitionGroups.add(partitionGroupRecord);
                    i++;
                }
            } else {
                //only has one physical partition after merge in the new logical partition
                PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                partitionGroupRecord.visible = 0;
                partitionGroupRecord.partition_name = newPhysicalPartName;
                partitionGroupRecord.tg_id = tableGroupId;
                partitionGroupRecord.pax_group_id = 0L;

                LocalityDesc defaultLocalityDesc = tableGroupConfig.getLocalityDesc();
                partitionGroupRecord.phy_db = targetGroupDetailInfoExRecords.get(i % targetDbCount).phyDbName;
                partitionGroupRecord.locality = targetLocality.toString();
                inVisiblePartitionGroups.add(partitionGroupRecord);
            }

            setInvisiblePartitionGroups(inVisiblePartitionGroups);
        } else {
            TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(getTableGroupName());
            int i = 0;
            int j = 0;
            int targetDbCount = targetGroupDetailInfoExRecords.size();
            Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
            List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
            do {
                for (String newPartitionName : getNewPartitionNames()) {
                    PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                    if (isUseTemplatePart() && getLogicalParts().size() > 0) {
                        partitionGroupRecord.partition_name = getLogicalParts().get(j) + newPartitionName;
                    } else {
                        partitionGroupRecord.partition_name = newPartitionName;
                    }
                    partitionGroupRecord.visible = 0;
                    partitionGroupRecord.tg_id = tableGroupId;
                    partitionGroupRecord.phy_db = targetGroupDetailInfoExRecords.get(i % targetDbCount).phyDbName;
                    partitionGroupRecord.locality = targetLocality.toString();
                    i++;
                    partitionGroupRecord.pax_group_id = 0L;
                    inVisiblePartitionGroups.add(partitionGroupRecord);
                }
                j++;
            } while (isUseTemplatePart() && getLogicalParts().size() > j);
            setInvisiblePartitionGroups(inVisiblePartitionGroups);
        }
    }
}
