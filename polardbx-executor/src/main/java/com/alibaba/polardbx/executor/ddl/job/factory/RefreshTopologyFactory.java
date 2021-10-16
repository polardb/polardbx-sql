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

import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.RefreshTopologyfinalTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshDbTopologyPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshTopologyPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.Map;
import java.util.Set;

/**
 * @author luoyanxin
 */
public class RefreshTopologyFactory extends DdlJobFactory {

    private final DDL ddl;
    private final RefreshTopologyPreparedData preparedData;
    private final ExecutionContext executionContext;

    public RefreshTopologyFactory(DDL ddl, RefreshTopologyPreparedData preparedData,
                                  ExecutionContext executionContext) {
        this.ddl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        String defaultSchemaName = preparedData.getAllRefreshTopologyPreparedData().keySet().iterator().next();
        EmptyTask emptyTask = new EmptyTask(defaultSchemaName);
        executableDdlJob.addTask(emptyTask);
        RefreshTopologyfinalTask refreshTopologyfinalTask =
            new RefreshTopologyfinalTask(defaultSchemaName, preparedData.getDbTableGroupAndInstGroupInfo());
        for (Map.Entry<String, RefreshDbTopologyPreparedData> entry : preparedData.getAllRefreshTopologyPreparedData()
            .entrySet()) {
            ExecutableDdlJob dbExecDdlJob = RefreshDbTopologyFactory.create(ddl, entry.getValue(), executionContext);
            executableDdlJob.combineTasks(dbExecDdlJob);
            executableDdlJob.addTaskRelationship(emptyTask, dbExecDdlJob.getHead());
            executableDdlJob.addTaskRelationship(dbExecDdlJob.getTail(), refreshTopologyfinalTask);
            executableDdlJob.getExcludeResources().addAll(dbExecDdlJob.getExcludeResources());
        }
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          RefreshTopologyPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new RefreshTopologyFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}