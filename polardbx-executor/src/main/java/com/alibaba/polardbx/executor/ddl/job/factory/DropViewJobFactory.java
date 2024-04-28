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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.DropViewRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropViewSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropViewMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateViewSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ValidateDropViewTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropView;
import com.google.common.collect.Lists;

import java.util.Set;

public class DropViewJobFactory extends DdlJobFactory {

    private final LogicalDropView logicalDropView;

    public DropViewJobFactory(LogicalDropView logicalDropView) {
        this.logicalDropView = logicalDropView;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = logicalDropView.getSchemaName();
        String viewName = logicalDropView.getViewName();
        boolean ifExists = logicalDropView.isIfExists();

        DdlTask validateTask = new ValidateDropViewTask(schemaName, viewName, ifExists);
        DdlTask removeMetaTask = new DropViewRemoveMetaTask(schemaName, viewName);
        DdlTask cdcMarkTask = new CdcDropViewMarkTask(schemaName, viewName);
        DdlTask syncTask = new DropViewSyncTask(schemaName, viewName);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(Lists.newArrayList(validateTask, removeMetaTask, cdcMarkTask, syncTask));
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(logicalDropView.getSchemaName(), logicalDropView.getViewName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}