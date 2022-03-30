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
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * change table status
 * <p>
 * will update [complex_task_outline]
 * will update [tables (version) ]
 */
@TaskName(name = "AlterComplexTaskUpdateJobStatusTask")
@Getter
public class AlterComplexTaskUpdateJobStatusTask extends BaseGmsTask {

    ComplexTaskMetaManager.ComplexTaskStatus beforeJobStatus;
    ComplexTaskMetaManager.ComplexTaskStatus afterJobStatus;
    ComplexTaskMetaManager.ComplexTaskStatus beforeTableStatus;
    ComplexTaskMetaManager.ComplexTaskStatus afterTableStatus;
    boolean subTask;
    List<String> relatedLogicalTables;
    String tableGroupName;
    String logicalTableName;

    @JSONCreator
    public AlterComplexTaskUpdateJobStatusTask(String schemaName,
                                               String logicalTableName,
                                               List<String> relatedLogicalTables,
                                               boolean subTask,
                                               ComplexTaskMetaManager.ComplexTaskStatus beforeJobStatus,
                                               ComplexTaskMetaManager.ComplexTaskStatus afterJobStatus,
                                               ComplexTaskMetaManager.ComplexTaskStatus beforeTableStatus,
                                               ComplexTaskMetaManager.ComplexTaskStatus afterTableStatus) {
        super(schemaName, "");
        this.relatedLogicalTables = relatedLogicalTables;
        this.beforeJobStatus = beforeJobStatus;
        this.afterJobStatus = afterJobStatus;
        this.beforeTableStatus = beforeTableStatus;
        this.afterTableStatus = afterTableStatus;
        this.subTask = subTask;
        this.logicalTableName = logicalTableName;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (subTask && beforeJobStatus != afterJobStatus) {
            ComplexTaskMetaManager
                .updateSubTasksStatusByJobIdAndObjName(getJobId(), schemaName, logicalTableName, beforeJobStatus,
                    afterJobStatus,
                    metaDbConnection);
        } else {
            ComplexTaskMetaManager
                .updateParentComplexTaskStatusByJobId(getJobId(), schemaName, beforeJobStatus, afterJobStatus,
                    metaDbConnection);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        if (!subTask && beforeTableStatus != afterTableStatus) {
            ComplexTaskMetaManager.updateAllSubTasksStatusByJobId(getJobId(), schemaName, beforeTableStatus,
                afterTableStatus, metaDbConnection);
        }

        for (String relatedTable : relatedLogicalTables) {
            tableInfoManager.updateVersionAndNotify(schemaName, relatedTable);
        }

        if (subTask) {
            LOGGER.info(
                String.format(
                    "Update table status[ schema:%s, table:%s, before state:%s, after state:%s]",
                    schemaName,
                    logicalTableName,
                    beforeJobStatus.name(),
                    afterJobStatus.name()));
        } else {
            LOGGER.info(String
                .format(
                    "Update table status[ schema:%s, job's status, before state:%s, after state:%s, subTask's "
                        + "status, before state:%s, after state:%s]",
                    schemaName,
                    beforeJobStatus.name(),
                    afterJobStatus.name(),
                    beforeTableStatus.name(),
                    afterTableStatus.name()));
        }

    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        // sync to restore the status of table meta
        for (String relatedTable : relatedLogicalTables) {
            SyncManagerHelper.sync(
                new TableMetaChangePreemptiveSyncAction(schemaName, relatedTable, 500L, 500L, TimeUnit.MICROSECONDS));
        }
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (subTask && beforeJobStatus != afterJobStatus) {
            ComplexTaskMetaManager
                .updateSubTasksStatusByJobIdAndObjName(getJobId(), schemaName, logicalTableName, afterJobStatus,
                    beforeJobStatus,
                    metaDbConnection);
        } else {
            ComplexTaskMetaManager
                .updateParentComplexTaskStatusByJobId(getJobId(), schemaName, afterJobStatus, beforeJobStatus,
                    metaDbConnection);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        if (!subTask && beforeTableStatus != afterTableStatus) {
            ComplexTaskMetaManager.updateAllSubTasksStatusByJobId(getJobId(), schemaName, afterTableStatus,
                beforeTableStatus, metaDbConnection);
        }

        for (String relatedTable : relatedLogicalTables) {
            tableInfoManager.updateVersionAndNotify(schemaName, relatedTable);
        }

        if (subTask) {
            LOGGER.info(String
                .format(
                    "Rollback table status[ schema:%s, table:%s, before state:%s, after state:%s]",
                    schemaName,
                    logicalTableName,
                    beforeJobStatus.name(),
                    afterJobStatus.name()));
        } else {
            LOGGER.info(String
                .format(
                    "Rollback table status[ schema:%s, job's status, before state:%s, after state:%s, subTask's "
                        + "status, before state:%s, after state:%s]",
                    schemaName,
                    beforeJobStatus.name(),
                    afterJobStatus.name(),
                    beforeTableStatus.name(),
                    afterTableStatus.name()));
        }
    }

    @Override
    protected String remark() {
        if (subTask) {
            return String.format("|%s to %s", beforeJobStatus.name(), afterJobStatus.name());
        } else {
            return String.format("|job: %s to %s, subTasks %s to %s", beforeJobStatus.name(),
                afterJobStatus.name(), beforeTableStatus.name(), afterTableStatus.name());
        }
    }
}
