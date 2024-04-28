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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.MoveDataToFileStoreTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.MoveDataToInnodbTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileCommitTsTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager.ID_GENERATOR;

public class MoveOSSDataJobFactory extends DdlJobFactory {
    private final String schemaName;
    private final String logicalTableName;

    private final String loadTableSchema;
    private final String loadTableName;

    private final String targetTableSchema;
    private final String targetTableName;

    private final Engine sourceEngine;
    private final Engine targetEngine;

    private final List<String> filterPartNames;

    public MoveOSSDataJobFactory(String loadTableSchema,
                                 String loadTableName,
                                 String targetTableSchema,
                                 String targetTableName, Engine sourceEngine,
                                 Engine targetEngine, List<String> filterPartNames) {
        this.schemaName = targetTableSchema;
        this.logicalTableName = targetTableName;
        this.loadTableSchema = loadTableSchema;
        this.loadTableName = loadTableName;
        this.targetTableSchema = targetTableSchema;

        this.targetTableName = targetTableName;
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
        this.filterPartNames = filterPartNames;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();
        if (Engine.isFileStore(targetEngine) && sourceEngine == Engine.INNODB) {
            MoveDataToFileStoreTask moveDataToFileStoreTask =
                new MoveDataToFileStoreTask(schemaName, logicalTableName, targetTableSchema, targetTableName,
                    loadTableSchema,
                    loadTableName, targetEngine, filterPartNames);
            moveDataToFileStoreTask.setTaskId(ID_GENERATOR.nextId());
            List<Long> taskIdList = new ArrayList<>();
            taskIdList.add(moveDataToFileStoreTask.getTaskId());
            UpdateFileCommitTsTask updateFileCommitTsTask =
                new UpdateFileCommitTsTask(targetEngine.name(), schemaName, logicalTableName, taskIdList);
            taskList.add(moveDataToFileStoreTask);
            taskList.add(updateFileCommitTsTask);
        } else if (Engine.isFileStore(sourceEngine) && targetEngine == Engine.INNODB) {
            MoveDataToInnodbTask moveDataToInnodbTask =
                new MoveDataToInnodbTask(schemaName, logicalTableName, loadTableName, targetTableName,
                    sourceEngine, targetEngine, filterPartNames);
            moveDataToInnodbTask.setTaskId(ID_GENERATOR.nextId());
            taskList.add(moveDataToInnodbTask);
        } else {
            throw GeneralUtil.nestedException(
                String.format("Unsupported data move: from %s to %s", sourceEngine, targetEngine));
        }

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        taskList.add(tableSyncTask);
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}
