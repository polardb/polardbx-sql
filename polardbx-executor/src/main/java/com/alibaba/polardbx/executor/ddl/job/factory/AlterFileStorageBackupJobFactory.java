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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.FileStorageBackupTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterFileStoragePreparedData;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AlterFileStorageBackupJobFactory extends DdlJobFactory {

    private static final Logger logger = LoggerFactory.getLogger("oss");

    private ExecutionContext executionContext;
    private AlterFileStoragePreparedData alterFileStoragePreparedData;

    public AlterFileStorageBackupJobFactory(
        AlterFileStoragePreparedData alterFileStoragePreparedData,
        ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.alterFileStoragePreparedData = alterFileStoragePreparedData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        Engine engine = Engine.of(alterFileStoragePreparedData.getFileStorageName());

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();

        taskList.add(new FileStorageBackupTask(engine.name()));

        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {

    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}

