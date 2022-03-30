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
import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Action that just lock some resource
 *
 * @author moyi
 * @since 2021/10
 */
public class ActionLockResource implements BalanceAction {

    private String schema;
    private Set<String> exclusiveResourceSet;

    public ActionLockResource(String schema, Set<String> exclusiveResourceSet) {
        this.schema = schema;
        this.exclusiveResourceSet = new HashSet<>();
        if(exclusiveResourceSet != null){
            this.exclusiveResourceSet.addAll(exclusiveResourceSet);
        }
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
        if(CollectionUtils.isEmpty(exclusiveResourceSet)){
            return "Lock()";
        }
        return "Lock(" + Joiner.on(",").join(exclusiveResourceSet) + ")";
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        EmptyTask task = new EmptyTask(schema);
        job.addTask(task);
        job.labelAsHead(task);
        job.labelAsTail(task);
        job.addExcludeResources(exclusiveResourceSet);
        return job;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionLockResource)) {
            return false;
        }
        ActionLockResource that = (ActionLockResource) o;
        return Objects.equals(schema, that.schema) && Objects.equals(exclusiveResourceSet, that.exclusiveResourceSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, exclusiveResourceSet);
    }

    @Override
    public String toString() {
        return "ActionLockResource{" +
            "schema='" + schema + '\'' +
            ", exclusiveResourceSet='" + Joiner.on(",").join(exclusiveResourceSet) + '\'' +
            '}';
    }
}
