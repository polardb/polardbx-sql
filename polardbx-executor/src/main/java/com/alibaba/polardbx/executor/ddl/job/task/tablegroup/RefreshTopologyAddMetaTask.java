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
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.HashSet;
import java.util.List;

@Getter
@TaskName(name = "RefreshTopologyAddMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class RefreshTopologyAddMetaTask extends AlterTableGroupAddMetaTask {

    public RefreshTopologyAddMetaTask(String schemaName, String tableGroupName, Long tableGroupId, String sourceSql,
                                      int status, int type, List<String> targetDbList, List<String> newPartitions) {
        super(schemaName, tableGroupName, tableGroupId, sourceSql, status, type, new HashSet<>(), targetDbList,
            newPartitions);
    }

    @JSONCreator
    public RefreshTopologyAddMetaTask(String schemaName, String tableGroupName, Long tableGroupId, String sourceSql,
                                      int status, int type, List<String> targetDbList, List<String> newPartitions, List<String> localities) {
        super(schemaName, tableGroupName, tableGroupId, sourceSql, status, type, new HashSet<>(), targetDbList,
            newPartitions, localities);
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (String newPart : newPartitions) {
            ComplexTaskOutlineRecord complexTaskOutlineRecord = new ComplexTaskOutlineRecord();
            complexTaskOutlineRecord.setObjectName(newPart);
            complexTaskOutlineRecord.setJob_id(getJobId());
            complexTaskOutlineRecord.setTableSchema(getSchemaName());
            complexTaskOutlineRecord.setTableGroupName(tableGroupName);
            complexTaskOutlineRecord.setType(type);
            complexTaskOutlineRecord.setStatus(status);
            complexTaskOutlineRecord.setSubTask(0);
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            ComplexTaskMetaManager.insertComplexTask(complexTaskOutlineRecord, metaDbConnection);
        }

        addNewPartitionGroup(metaDbConnection);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (String newPart : newPartitions) {
            ComplexTaskMetaManager
                .deleteComplexTaskByJobIdAndObjName(getJobId(), schemaName, newPart, metaDbConnection);
        }
        TableGroupUtils
            .deleteNewPartitionGroupFromDeltaTableByTgIDAndPartNames(tableGroupId, newPartitions, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

}
