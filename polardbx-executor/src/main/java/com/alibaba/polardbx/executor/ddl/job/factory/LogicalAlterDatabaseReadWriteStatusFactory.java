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

import com.alibaba.polardbx.executor.ddl.job.task.basic.ChangeDatabaseReadWriteStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.GlobalAcquireMdlLockInDbSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.GlobalReleaseMdlLockInDbSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterDatabaseReadWriteStatusFactory extends DdlJobFactory {
    protected final String schemaName;
    protected final int status;

    public LogicalAlterDatabaseReadWriteStatusFactory(String schemaName, int status) {
        this.schemaName = schemaName;
        this.status = status;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DdlTask globalAcquireMdlLockInDbTask = new GlobalAcquireMdlLockInDbSyncTask(
            schemaName,
            ImmutableSet.of(schemaName)
        );
        DdlTask globalReleaseMdlLockInDbTask = new GlobalReleaseMdlLockInDbSyncTask(
            schemaName,
            ImmutableSet.of(schemaName)
        );
        DdlTask changeDatabaseReadWriteStatus = new ChangeDatabaseReadWriteStatusTask(
            schemaName,
            ImmutableMap.of(schemaName, status)
        );

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(ImmutableList.of(
            globalAcquireMdlLockInDbTask,
            globalReleaseMdlLockInDbTask,
            changeDatabaseReadWriteStatus
        ));

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        //forbid all database's ddl
        resources.add(schemaName);
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
