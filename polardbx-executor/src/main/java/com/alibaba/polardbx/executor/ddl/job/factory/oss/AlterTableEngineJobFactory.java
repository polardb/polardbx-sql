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

package com.alibaba.polardbx.executor.ddl.job.factory.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.ChangeTableEngineTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.MoveDataToInnodbTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AlterTableEngineJobFactory extends DdlJobFactory {
    private String schemaName;
    private String sourceTableName;
    private Engine sourceEngine;
    private Engine targetEngine;

    public AlterTableEngineJobFactory(String schemaName, String sourceTableName,
                                      Engine sourceEngine, Engine targetEngine) {
        this.schemaName = schemaName;
        this.sourceTableName = sourceTableName;
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();

        MoveDataToInnodbTask moveDataToInnodbTask
            = new MoveDataToInnodbTask(schemaName, sourceTableName,
            sourceTableName, sourceTableName,
            sourceEngine, targetEngine, ImmutableList.of());

        ChangeTableEngineTask changeTableEngineTask =
            new ChangeTableEngineTask(schemaName, sourceTableName, sourceEngine, targetEngine);

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, sourceTableName);

        taskList.add(moveDataToInnodbTask);
        taskList.add(changeTableEngineTask);
        taskList.add(tableSyncTask);

        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, sourceTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}
