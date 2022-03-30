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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

/**
 * Created by luoyanxin.
 */

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Getter
@TaskName(name = "MoveDatabaseAddMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class MoveDatabaseAddMetaTask extends BaseDdlTask {

    protected String sourceSql;
    protected int type;
    protected Long tableGroupId = -1L;
    protected int status;
    protected List<String> objectNames;
    protected int subTask;

    @JSONCreator
    public MoveDatabaseAddMetaTask(String schemaName,
                                   List<String> objectNames,
                                   String sourceSql,
                                   int status,
                                   int type,
                                   int subTask) {
        super(schemaName);
        this.sourceSql = sourceSql;
        this.type = type;
        this.status = status;
        this.objectNames = objectNames;
        this.subTask = subTask;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (String objectName : objectNames) {
            ComplexTaskOutlineRecord complexTaskOutlineRecord = new ComplexTaskOutlineRecord();
            complexTaskOutlineRecord.setObjectName(objectName);
            complexTaskOutlineRecord.setJob_id(getJobId());
            complexTaskOutlineRecord.setTableSchema(getSchemaName());
            complexTaskOutlineRecord.setTableGroupName("");
            complexTaskOutlineRecord.setType(type);
            complexTaskOutlineRecord.setStatus(status);
            complexTaskOutlineRecord.setSourceSql(sourceSql);
            complexTaskOutlineRecord.setSubTask(subTask);
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            ComplexTaskMetaManager.insertComplexTask(complexTaskOutlineRecord, metaDbConnection);
        }
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        for (String objectName : objectNames) {
            ComplexTaskMetaManager
                .deleteComplexTaskByJobIdAndObjName(getJobId(), schemaName, objectName, metaDbConnection);
            if (subTask == 1) {
                tableInfoManager.updateVersionAndNotify(schemaName, objectName);
            }
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        if (subTask == 1) {
            //for creating status
            for (String tableName : objectNames) {
                SyncManagerHelper.sync(
                    new TableMetaChangePreemptiveSyncAction(schemaName, tableName, 500L, 500L, TimeUnit.MICROSECONDS));
            }
        }

    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected String remark() {
        return "|objectNames: " + objectNames.toString() + "|status:" + String.valueOf(this.status);
    }
}
