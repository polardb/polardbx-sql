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

import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.DefaultSchema.getSchemaName;

public class AlterTableGroupBasePreparedData extends DdlPreparedData {

    public AlterTableGroupBasePreparedData() {
    }

    private String tableGroupName;

    private List<String> newPartitionNames;
    private List<String> oldPartitionNames;
    private List<String> excludeStorageInstIds;
    private List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords;
    private ComplexTaskMetaManager.ComplexTaskType taskType;
    /*
     * the new create partition group, it's invisible when
     * the alter tablegroup operation is not finish
     */
    private List<PartitionGroupRecord> invisiblePartitionGroups;
    private String sourceSql;

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public List<String> getNewPartitionNames() {
        return newPartitionNames;
    }

    public void setNewPartitionNames(List<String> newPartitionNames) {
        this.newPartitionNames = newPartitionNames;
    }

    public List<String> getOldPartitionNames() {
        return oldPartitionNames;
    }

    public void setOldPartitionNames(List<String> oldPartitionNames) {
        this.oldPartitionNames = oldPartitionNames;
    }

    public List<PartitionGroupRecord> getInvisiblePartitionGroups() {
        return invisiblePartitionGroups;
    }

    public void setInvisiblePartitionGroups(
        List<PartitionGroupRecord> invisiblePartitionGroups) {
        this.invisiblePartitionGroups = invisiblePartitionGroups;
    }

    public List<String> getExcludeStorageInstIds() {
        return excludeStorageInstIds;
    }

    public void setExcludeStorageInstIds(List<String> excludeStorageInstIds) {
        this.excludeStorageInstIds = excludeStorageInstIds;
    }

    public List<GroupDetailInfoExRecord> getTargetGroupDetailInfoExRecords() {
        return targetGroupDetailInfoExRecords;
    }

    public void setTargetGroupDetailInfoExRecords(
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords) {
        this.targetGroupDetailInfoExRecords = targetGroupDetailInfoExRecords;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public void prepareInvisiblePartitionGroup() {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        int i = 0;
        int targetDbCount = targetGroupDetailInfoExRecords.size();
        Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
        for (String newPartitionName : getNewPartitionNames()) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.visible = 0;
            partitionGroupRecord.partition_name = newPartitionName;
            partitionGroupRecord.tg_id = tableGroupId;

            partitionGroupRecord.phy_db = targetGroupDetailInfoExRecords.get(i % targetDbCount).phyDbName;

            partitionGroupRecord.locality = "";
            partitionGroupRecord.pax_group_id = 0L;
            inVisiblePartitionGroups.add(partitionGroupRecord);
            i++;
        }
        setInvisiblePartitionGroups(inVisiblePartitionGroups);
    }

    public ComplexTaskMetaManager.ComplexTaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(ComplexTaskMetaManager.ComplexTaskType taskType) {
        this.taskType = taskType;
    }
}
