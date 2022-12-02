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
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "CleanupEmptyTableGroupTask")
public class CleanupEmptyTableGroupTask extends BaseDdlTask {

    final String tableGroupName;

    @JSONCreator
    public CleanupEmptyTableGroupTask(String schemaName, String tableGroupName) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tableGroupAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);

        List<TableGroupRecord>
            tableGroupRecordList = tableGroupAccessor.getTableGroupsBySchemaAndName(schemaName, tableGroupName, true);
        if (GeneralUtil.isNotEmpty(tableGroupRecordList)) {
            TableGroupRecord tableGroupRecord = tableGroupRecordList.get(0);
            List<TablePartitionRecord> tablePartitionRecords =
                tablePartitionAccessor.getTablePartitionsByDbNameGroupId(schemaName, tableGroupRecord.id);
            if (GeneralUtil.isEmpty(tablePartitionRecords)) {
                TableGroupUtils
                    .deleteEmptyTableGroupInfo(schemaName, tableGroupRecord.id, metaDbConnection);
            }
        }
        updateSupportedCommands(true, false, metaDbConnection);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }
}
