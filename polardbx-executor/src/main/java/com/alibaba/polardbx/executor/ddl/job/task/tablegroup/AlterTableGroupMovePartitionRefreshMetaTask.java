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
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AlterTableGroupMovePartitionRefreshMetaTask")
public class AlterTableGroupMovePartitionRefreshMetaTask extends AlterTableGroupRefreshMetaBaseTask {

    @JSONCreator
    public AlterTableGroupMovePartitionRefreshMetaTask(String schemaName, String tableGroupName) {
        super(schemaName, tableGroupName);
    }

    /**
     * 1、update partition group's pyhsical location;
     * 2、cleanup partition_group_delta
     * 3、cleanup table_partition_delta
     */
    @Override
    public void refreshTableGroupMeta(Connection metaDbConnection) {

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        dbGroupInfoAccessor.setConnection(metaDbConnection);

        updateTaskStatus(metaDbConnection);

        long tableGroupId = tableGroupConfig.getTableGroupRecord().id;

        List<PartitionGroupRecord> outDatedPartRecords =
            partitionGroupAccessor.getOutDatedPartitionGroupsByTableGroupIdFromDelta(tableGroupId);
        List<PartitionGroupRecord> newPartitionGroups = partitionGroupAccessor
            .getPartitionGroupsByTableGroupId(tableGroupId, true);
        for (PartitionGroupRecord record : outDatedPartRecords) {
            // 1、update the partition group's physical location
            PartitionGroupRecord newRecord = newPartitionGroups.stream()
                .filter(o -> o.getPartition_name().equalsIgnoreCase(record.getPartition_name())).findFirst()
                .orElse(null);
            assert newRecord != null;
            partitionGroupAccessor.updatePhyDbById(record.id, newRecord.phy_db);
        }

        // 2、cleanup partition_group_delta
        partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupId, true);

        for (TablePartRecordInfoContext infoContext : tableGroupConfig.getAllTables()) {
            // 3、cleanup table_partition_delta
            // only delete the related records
            String tableName = infoContext.getTableName();
            tablePartitionAccessor
                .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
        }

    }
}
