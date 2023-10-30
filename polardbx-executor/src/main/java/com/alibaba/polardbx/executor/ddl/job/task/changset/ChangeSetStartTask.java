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
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@TaskName(name = "ChangeSetStartTask")
@Getter
public class ChangeSetStartTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long changeSetId;

    @JSONCreator
    public ChangeSetStartTask(String schemaName, String logicalTableName,
                              Map<String, Set<String>> sourcePhyTableNames,
                              ComplexTaskMetaManager.ComplexTaskType taskType,
                              Long changeSetId
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.taskType = taskType;
        this.changeSetId = changeSetId;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setSchemaName(schemaName);

        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);

        // should be idempotent
        changeSetManager.logicalTableChangeSetStart(
            logicalTableName,
            sourcePhyTableNames,
            taskType,
            changeSetId,
            executionContext
        );

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.logicalTableChangeSetRollBack(
            logicalTableName,
            sourcePhyTableNames,
            taskType,
            changeSetId,
            executionContext
        );
    }

    public static String getTaskName() {
        return "ChangeSetStartTask";
    }

    @Override
    public String remark() {
        return "|start changeset, tableName: " + logicalTableName;
    }
}
