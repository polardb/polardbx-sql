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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */

@Getter
@TaskName(name = "AlterTableGroupRemoveTempPartitionTask")
public class AlterTableGroupRemoveTempPartitionTask extends BaseDdlTask {

    final String tempPartition;
    final Long tableGroupId;

    @JSONCreator
    public AlterTableGroupRemoveTempPartitionTask(String schemaName, String tempPartition, Long tableGroupId) {
        super(schemaName);
        this.tempPartition = tempPartition;
        this.tableGroupId = tableGroupId;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);
        List<PartitionGroupRecord> partitionGroupRecords =
            partitionGroupAccessor.getTempPartitionGroupsByTableGroupIdAndNameFromDelta(tableGroupId, tempPartition);
        List<TablePartitionRecord> tablePartitionRecords =
            tablePartitionAccessor.getTablePartitionsByDbNameGroupId(schemaName, tableGroupId);
        for (PartitionGroupRecord record : partitionGroupRecords) {
            partitionGroupAccessor.deletePartitionGroupByIdFromDelta(record.getTg_id(), record.partition_name);
            for (TablePartitionRecord tablePartitionRecord : GeneralUtil.emptyIfNull(tablePartitionRecords)) {
                tablePartitionAccessor.deleteTablePartitionByGidAndPartNameFromDelta(schemaName,
                    tablePartitionRecord.getTableName(), record.id,
                    tempPartition);
            }
        }
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }
}