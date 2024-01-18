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
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;

import java.util.List;

public class AlterTableGroupItemPreparedData extends DdlPreparedData {

    public AlterTableGroupItemPreparedData(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    private String tableGroupName;
    private PartitionSpec defaultPartitionSpec;
    private List<String> newPhyTables;
    private List<GroupDetailInfoExRecord> groupDetailInfoExRecords;
    private List<String> oldPartitionNames;
    private List<String> newPartitionNames;
    private ComplexTaskMetaManager.ComplexTaskType taskType;
    private String primaryTableName;
    private Long tableVersion;
    /*
     * the new create partition group, it's invisible when
     * the alter tablegroup operation is not finish
     */
    private List<PartitionGroupRecord> invisiblePartitionGroups;
    private boolean operateOnSubPartition;

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public List<String> getNewPhyTables() {
        return newPhyTables;
    }

    public void setNewPhyTables(List<String> newPhyTables) {
        this.newPhyTables = newPhyTables;
    }

    public List<GroupDetailInfoExRecord> getGroupDetailInfoExRecords() {
        return groupDetailInfoExRecords;
    }

    public void setGroupDetailInfoExRecords(
        List<GroupDetailInfoExRecord> groupDetailInfoExRecords) {
        this.groupDetailInfoExRecords = groupDetailInfoExRecords;
    }

    public PartitionSpec getDefaultPartitionSpec() {
        return defaultPartitionSpec;
    }

    public void setDefaultPartitionSpec(PartitionSpec defaultPartitionSpec) {
        this.defaultPartitionSpec = defaultPartitionSpec;
    }

    public List<String> getOldPartitionNames() {
        return oldPartitionNames;
    }

    public void setOldPartitionNames(List<String> oldPartitionNames) {
        this.oldPartitionNames = oldPartitionNames;
    }

    public List<String> getNewPartitionNames() {
        return newPartitionNames;
    }

    public void setNewPartitionNames(List<String> newPartitionNames) {
        this.newPartitionNames = newPartitionNames;
    }

    public List<PartitionGroupRecord> getInvisiblePartitionGroups() {
        return invisiblePartitionGroups;
    }

    public void setInvisiblePartitionGroups(List<PartitionGroupRecord> invisiblePartitionGroups) {
        this.invisiblePartitionGroups = invisiblePartitionGroups;
    }

    public ComplexTaskMetaManager.ComplexTaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(ComplexTaskMetaManager.ComplexTaskType taskType) {
        this.taskType = taskType;
    }

    public Long getTableVersion() {
        return tableVersion;
    }

    public void setTableVersion(Long tableVersion) {
        this.tableVersion = tableVersion;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    public boolean isOperateOnSubPartition() {
        return operateOnSubPartition;
    }

    public void setOperateOnSubPartition(boolean operateOnSubPartition) {
        this.operateOnSubPartition = operateOnSubPartition;
    }
}
