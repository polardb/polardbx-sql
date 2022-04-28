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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.executor.ddl.newengine.job.AbstractDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Arrays;
import java.util.Objects;

/**
 * The action that execute a ddl task directly
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionTaskAdapter implements BalanceAction {

    private String schema;
    private AbstractDdlTask task;

    public ActionTaskAdapter(String schema, AbstractDdlTask task) {
        Objects.requireNonNull(task);
        this.schema = schema;
        this.task = task;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return task.getName();
    }

    @Override
    public String getStep() {
        return String.format("Execute %s: %s", task.getName(), task.getDescription());
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.addSequentialTasks(Arrays.asList(task));
        job.labelAsHead(task);
        job.labelAsTail(task);
        return job;
    }
}
