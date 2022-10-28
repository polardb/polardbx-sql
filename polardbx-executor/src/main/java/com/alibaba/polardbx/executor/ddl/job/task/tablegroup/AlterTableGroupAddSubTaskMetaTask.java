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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "AlterTableGroupAddSubTaskMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableGroupAddSubTaskMetaTask extends BaseDdlTask {

    protected String tableName;
    protected String sourceSql;
    protected int type;
    protected String tableGroupName;
    protected Long tableGroupId;
    protected int status;
    protected TablePartitionRecord logTableRec;
    protected List<TablePartitionRecord> partRecList;
    protected Map<String, List<TablePartitionRecord>> subPartRecInfos;

    @JSONCreator
    public AlterTableGroupAddSubTaskMetaTask(String schemaName, String tableName, String tableGroupName,
                                             Long tableGroupId, String sourceSql, int status, int type,
                                             TablePartitionRecord logTableRec, List<TablePartitionRecord> partRecList,
                                             Map<String, List<TablePartitionRecord>> subPartRecInfos) {
        super(schemaName);
        this.tableName = tableName;
        this.sourceSql = sourceSql;
        this.type = type;
        this.tableGroupName = tableGroupName;
        this.tableGroupId = tableGroupId;
        this.status = status;
        this.logTableRec = logTableRec;
        this.partRecList = partRecList;
        this.subPartRecInfos = subPartRecInfos;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        ComplexTaskOutlineRecord complexTaskOutlineRecord = new ComplexTaskOutlineRecord();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);

        complexTaskOutlineRecord.setObjectName(tableName);
        complexTaskOutlineRecord.setJob_id(getJobId());
        complexTaskOutlineRecord.setTableSchema(getSchemaName());
        complexTaskOutlineRecord.setTableGroupName(tableGroupName);
        complexTaskOutlineRecord.setType(type);
        complexTaskOutlineRecord.setStatus(status);
        complexTaskOutlineRecord.setSubTask(1);
        ComplexTaskMetaManager.insertComplexTask(complexTaskOutlineRecord, metaDbConnection);

        //correct the partition group id for partRecList which is 0 now
        updateTablePartitionsInfo();
        tablePartitionAccessor.addNewTablePartitionConfigs(logTableRec,
            partRecList,
            subPartRecInfos,
            true, true);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        ComplexTaskMetaManager
            .deleteComplexTaskByJobIdAndObjName(getJobId(), schemaName, tableName, metaDbConnection);
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor
            .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    private void updateTablePartitionsInfo() {
        List<PartitionGroupRecord> unVisiablePartitionGroupRecords =
            TableGroupUtils.getAllUnVisiablePartitionGroupByGroupId(tableGroupId);
        for (TablePartitionRecord record : partRecList) {
            for (PartitionGroupRecord precord : unVisiablePartitionGroupRecords) {
                if (precord.partition_name.equalsIgnoreCase(record.partName)) {
                    record.setGroupId(precord.id);
                    break;
                }
            }
        }
    }

}
