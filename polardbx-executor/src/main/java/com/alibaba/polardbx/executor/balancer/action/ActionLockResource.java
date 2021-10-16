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

import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Sets;

/**
 * Action that just lock some resource
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionLockResource implements BalanceAction {

    private String schema;
    private String resource;

    public ActionLockResource(String schema, String lockedResource) {
        this.schema = schema;
        this.resource = lockedResource;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return "LockResource";
    }

    @Override
    public String getStep() {
        return "Lock(" + resource + ")";
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        EmptyTask task = new EmptyTask(schema);
        job.addTask(task);
        job.addExcludeResources(Sets.newHashSet(resource));
        return job;
    }

}
