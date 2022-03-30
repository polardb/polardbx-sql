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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AlterTableSetTableGroupRefreshMetaTask")
public class AlterTableSetTableGroupRefreshMetaTask extends AlterTableGroupRefreshMetaBaseTask {

    protected String tableName;
    protected Long sourceTableGroupId;

    @JSONCreator
    public AlterTableSetTableGroupRefreshMetaTask(String schemaName, String tableGroupName, Long sourceTableGroupId,
                                                  String tableName) {
        super(schemaName, tableGroupName);
        this.tableName = tableName;
        this.sourceTableGroupId = sourceTableGroupId;
    }

    /**
     * 1、update table/partition's groupid;
     * 2、cleanup partition_group_delta
     * 3、cleanup table_partition_delta
     */
    @Override
    public void refreshTableGroupMeta(Connection metaDbConnection) {

        TableGroupConfig newTableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        PartitionInfo oldPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        dbGroupInfoAccessor.setConnection(metaDbConnection);

        updateTaskStatus(metaDbConnection);

        long newTableGroupId = newTableGroupConfig.getTableGroupRecord().id;
        long oldTableGroupId = oldPartitionInfo.getTableGroupId();

        TablePartitionConfig oldTablePartitionConfig =
            tablePartitionAccessor.getTablePartitionConfig(schemaName, tableName, false);

        // 1.1、update table's groupid
        tablePartitionAccessor.updateGroupIdById(newTableGroupId, oldTablePartitionConfig.getTableConfig().id);

        List<TablePartitionSpecConfig> tablePartitionSpecConfigs = oldTablePartitionConfig.getPartitionSpecConfigs();
        List<PartitionGroupRecord> partitionGroupRecords = newTableGroupConfig.getPartitionGroupRecords();

        for (TablePartitionSpecConfig tablePartitionSpecConfig : tablePartitionSpecConfigs) {
            TablePartitionRecord tablePartitionRecord = tablePartitionSpecConfig.getSpecConfigInfo();
            PartitionGroupRecord partitionGroupRecord = partitionGroupRecords.stream()
                .filter(o -> o.getPartition_name().equalsIgnoreCase(tablePartitionRecord.getPartName())).findFirst()
                .orElse(null);
            if (partitionGroupRecord == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can't find the partition:" + partitionGroupRecord.getPartition_name());
            }
            // 1.2、update partition's groupid
            tablePartitionAccessor.updateGroupIdById(partitionGroupRecord.id, tablePartitionRecord.getId());
        }

        // 2、cleanup partition_group_delta
        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(newTableGroupId, true);
        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(oldTableGroupId, true);

        // 3、cleanup table_partition_delta
        // only delete the related records
        tablePartitionAccessor
            .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);

    }
}
