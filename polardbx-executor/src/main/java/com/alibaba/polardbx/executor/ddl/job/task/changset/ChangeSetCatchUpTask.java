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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.changeset.ChangeSetApplyExecutor;
import com.alibaba.polardbx.executor.changeset.ChangeSetApplyExecutorMap;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.util.Map;
import java.util.Set;

@TaskName(name = "ChangeSetCatchUpTask")
@Getter
public class ChangeSetCatchUpTask extends BaseBackfillTask {
    final private String logicalTableName;
    final private String indexTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private Map<String, String> orderedTargetTableLocations;
    final private ChangeSetManager.ChangeSetCatchUpStatus catchUpStatus;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long changeSetId;
    final private Boolean loop;

    @JSONCreator
    public ChangeSetCatchUpTask(String schemaName, String logicalTableName, String indexTableName,
                                Map<String, Set<String>> sourcePhyTableNames,
                                Map<String, String> orderedTargetTableLocations,
                                ChangeSetManager.ChangeSetCatchUpStatus catchUpStatus,
                                ComplexTaskMetaManager.ComplexTaskType taskType,
                                Long changeSetId, Boolean loop
    ) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexTableName = indexTableName;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.orderedTargetTableLocations = orderedTargetTableLocations;
        this.catchUpStatus = catchUpStatus;
        this.changeSetId = changeSetId;
        this.taskType = taskType;
        this.loop = loop;
        onExceptionTryRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);

        DdlContext ddlContext = executionContext.getDdlContext();

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        FailPoint.injectSuspendFromHint("FP_CATCHUP_TASK_SUSPEND", executionContext);

        changeSetManager.logicalTableChangeSetCatchUp(
            logicalTableName,
            indexTableName,
            sourcePhyTableNames,
            orderedTargetTableLocations,
            taskType,
            catchUpStatus,
            changeSetId,
            executionContext
        );

        if (ddlContext.isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }

        if (loop) {
            ChangeSetApplyExecutor executor = ChangeSetApplyExecutorMap.get(schemaName, this.getJobId());
            if (executor == null) {
                SQLRecorderLogger.scaleOutTaskLogger.info(
                    "The job '" + jobId + "' can not find the changeset apply executor");
                return;
            }

            boolean res = executor.addTask(
                new ChangeSetApplyExecutor.ChangeSetApplyInfo(
                    logicalTableName, indexTableName,
                    sourcePhyTableNames, orderedTargetTableLocations,
                    catchUpStatus,
                    taskType,
                    changeSetId,
                    executionContext
                ));

            if (!res) {
                String msg = executor.getException().get().getMessage();
                String interruptMsg = "The job '" + jobId + "' has been cancelled";
                if (msg.contains(interruptMsg)) {
                    SQLRecorderLogger.scaleOutTaskLogger.info(
                        "The job '" + jobId
                            + "' has been cancelled, so apply task will not add to changeset apply executor");
                    return;
                }

                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' add changeset apply task failed, " + msg);
            }
        }
    }

    @Override
    protected void rollbackImpl(ExecutionContext executionContext) {
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
        return "ChangeSetCatchUpTask";
    }

    @Override
    protected String remark() {
        return String.format("|%s catch up with %s", logicalTableName, catchUpStatus.toString());
    }
}
