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
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@TaskName(name = "LogTwoPhaseDdlTask")
@Getter
public class LogTwoPhaseDdlTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long twoPhaseDdlId;

    final private String sqlTemplate;

    @JSONCreator
    public LogTwoPhaseDdlTask(String schemaName, String logicalTableName,
                              Map<String, Set<String>> sourcePhyTableNames,
                              String sqlTemplate,
                              ComplexTaskMetaManager.ComplexTaskType taskType,
                              Long twoPhaseDdlId
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sqlTemplate = sqlTemplate;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.taskType = taskType;
        this.twoPhaseDdlId = twoPhaseDdlId;
        onExceptionTryRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            twoPhaseDdlManager =
                new TwoPhaseDdlManager(schemaName, logicalTableName, sqlTemplate, sourcePhyTableNames, twoPhaseDdlId);
            twoPhaseDdlManager.setJobId(jobId);
        }

        // should be idempotent
        try {
            twoPhaseDdlManager.twoPhaseDdlLog(schemaName, logicalTableName, executionContext);
        } catch (RuntimeException | InterruptedException | ExecutionException exception) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_DDL_JOB_ERROR, exception.getMessage()
            );
            //TODO, process exception.
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
    }

    public static String getTaskName() {
        return "LogTwoPhaseDdlTask";
    }

    @Override
    public String remark() {
        return "|log TwoPhaseDdl, tableName: " + logicalTableName;
    }
}
