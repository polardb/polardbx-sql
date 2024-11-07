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

package com.alibaba.polardbx.executor.ddl.job.task.changset;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@TaskName(name = "ChangeSetCopyBaselineTask")
@Getter
public class ChangeSetCopyBaselineTask extends BaseBackfillTask {
    final private String logicalTableName;
    final private String sourceGroupName;
    final private String targetGroupName;
    final private String sourcePhyTableName;
    final private String targetPhyTableName;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;

    @JSONCreator
    public ChangeSetCopyBaselineTask(String schemaName, String logicalTableName,
                                     String sourceGroupName, String targetGroupName,
                                     String sourcePhyTableName, String targetPhyTableName,
                                     ComplexTaskMetaManager.ComplexTaskType taskType
    ) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.sourceGroupName = sourceGroupName;
        this.targetGroupName = targetGroupName;
        this.sourcePhyTableName = sourcePhyTableName;
        this.targetPhyTableName = targetPhyTableName;
        this.taskType = taskType;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setTaskId(getTaskId());
        executionContext.setSchemaName(schemaName);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.migrateTableCopyBaseline(
            schemaName,
            logicalTableName,
            sourceGroupName,
            targetGroupName,
            sourcePhyTableName,
            targetPhyTableName,
            taskType,
            this.getTaskId(),
            executionContext
        );
    }

    @Override
    protected void rollbackImpl(ExecutionContext executionContext) {
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.migrateTableRollback(
            schemaName,
            logicalTableName,
            sourceGroupName,
            sourcePhyTableName,
            taskType,
            executionContext
        );
    }

    public static String getTaskName() {
        return "ChangeSetCopyBaselineTask";
    }

    @Override
    protected String remark() {
        return "|changeset copy baseline: " + logicalTableName;
    }

}
