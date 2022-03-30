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
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineSchedulerManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "PauseCurrentJobTask")
// set the current running task to pause status for test purpose
public class PauseCurrentJobTask extends BaseDdlTask {

    @JSONCreator
    public PauseCurrentJobTask(String schemaName) {
        super(schemaName);
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final DdlEngineSchedulerManager schedulerManager = new DdlEngineSchedulerManager();
        if (schedulerManager.tryPauseDdl(
            getJobId(),
            DdlState.RUNNING,
            DdlState.PAUSED)) {
            DdlEngineDagExecutor ddlEngineDagExecutor = DdlEngineDagExecutorMap.get(schemaName, jobId);
            if (ddlEngineDagExecutor == null) {
                return;
            }
            ddlEngineDagExecutor.interrupt();
        }
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {

    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
        updateSupportedCommands(false, true, metaDbConnection);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected String remark() {
        return "|pause current job";
    }
}
