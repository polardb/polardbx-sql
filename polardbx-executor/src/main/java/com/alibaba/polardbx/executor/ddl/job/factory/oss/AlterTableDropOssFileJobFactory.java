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
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileRemoveTsTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AlterTableDropOssFileJobFactory extends DdlJobFactory {
    private String schemaName;
    private String logicalTableName;
    private ExecutionContext executionContext;
    private AlterTablePreparedData alterTablePreparedData;

    public AlterTableDropOssFileJobFactory(String schemaName,
                                           String logicalTableName,
                                           AlterTablePreparedData alterTablePreparedData,
                                           ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.executionContext = executionContext;
        this.alterTablePreparedData = alterTablePreparedData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(this.logicalTableName);

        Engine engine = tableMeta.getEngine();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();

        // change file meta task
        final ITimestampOracle timestampOracle = executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
        if (null == timestampOracle) {
            throw new UnsupportedOperationException("Do not support timestamp oracle");
        }

        long ts = timestampOracle.nextTimestamp();
        UpdateFileRemoveTsTask updateFileRemoveTsTask = new UpdateFileRemoveTsTask(engine.name(),
            schemaName, logicalTableName, alterTablePreparedData.getDropFiles(), ts);

        // sync to other tables
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        taskList.add(updateFileRemoveTsTask);
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
