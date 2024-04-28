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

package com.alibaba.polardbx.executor.ddl.job.task.twophase;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@TaskName(name = "EmitPhysicalDdlTask")
@Getter
public class EmitPhysicalDdlTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long twoPhaseDdlId;

    final private String sqlTemplate;
    ConcurrentHashMap<String, Set<String>> sourcePhyTableEmitted;

    @JsonCreator
    public EmitPhysicalDdlTask(String schemaName, String logicalTableName,
                               Map<String, Set<String>> sourcePhyTableNames,
                               String sqlTemplate,
                               ComplexTaskMetaManager.ComplexTaskType taskType,
                               Long twoPhaseDdlId,
                               ConcurrentHashMap<String, Set<String>> sourcePhyTableEmitted
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.taskType = taskType;
        this.sqlTemplate = sqlTemplate;
        this.twoPhaseDdlId = twoPhaseDdlId;
        this.sourcePhyTableEmitted = sourcePhyTableEmitted;
        onExceptionTryRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "can't find twoPhaseDdlManager, HA may has happened! will rollback");
        }

        // should be idempotent
        try {
            twoPhaseDdlManager.twoPhaseDdlEmit(
                logicalTableName,
                sourcePhyTableEmitted,
                executionContext
            );
        } catch (Exception exception) {
            String cause = "unknown casue!";
            if (exception.getMessage() != null) {
                cause = exception.getMessage();
            }
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, cause);
        }

        if (!twoPhaseDdlManager.checkAllPhyDdlEmited(logicalTableName, executionContext)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The task ended but not all physical table emitted, maybe paused handly!");
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            twoPhaseDdlManager =
                new TwoPhaseDdlManager(schemaName, logicalTableName, sqlTemplate, sourcePhyTableNames, twoPhaseDdlId);
            twoPhaseDdlManager.setJobId(jobId);
        }
        try {
            twoPhaseDdlManager.twoPhaseDdlRollback(
                schemaName,
                logicalTableName,
                executionContext
            );
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

    }

    public static String getTaskName() {
        return "EmitPhysicalDdlTask";
    }

    @Override
    public String remark() {
        return "|emit TwoPhaseDdl, tableName: " + logicalTableName;
    }
}
