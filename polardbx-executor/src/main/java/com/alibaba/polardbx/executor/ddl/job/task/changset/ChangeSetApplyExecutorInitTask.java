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

import com.alibaba.polardbx.executor.changeset.ChangeSetApplyExecutor;
import com.alibaba.polardbx.executor.changeset.ChangeSetApplyExecutorMap;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

/**
 * @author wumu
 */
@TaskName(name = "ChangeSetApplyExecutorInitTask")
@Getter
public class ChangeSetApplyExecutorInitTask extends BaseBackfillTask {

    private int maxParallelism;

    public ChangeSetApplyExecutorInitTask(String schemaName, int maxParallelism) {
        super(schemaName);
        this.maxParallelism = maxParallelism;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        ChangeSetApplyExecutorMap.restore(schemaName, jobId, maxParallelism);
    }

    @Override
    protected void rollbackImpl(ExecutionContext executionContext) {
        ChangeSetApplyExecutor changeSetApplyExecutor = ChangeSetApplyExecutorMap.get(schemaName, jobId);
        if (changeSetApplyExecutor != null) {
            changeSetApplyExecutor.waitAndStop();
        }
        ChangeSetApplyExecutorMap.remove(schemaName, jobId);
    }
}
