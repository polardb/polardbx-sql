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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.polardbx.executor.ddl.job.builder.gsi.DropGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.DropPartitionGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropGsiTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateGsiExistenceTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropGsi;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 1. drop index xxx on yyy
 * 2. alter table yyy drop index xxx
 * <p>
 * for drop table with [unique] gsi, see class: DropTableWithGlobalIndexJob
 *
 * @author guxu
 */
public class DropGsiJobFactory extends DdlJobFactory {

    protected final String schemaName;
    protected final String primaryTableName;
    protected final String indexTableName;
    protected final ExecutionContext executionContext;
    protected boolean skipSchemaChange = false;

    public static final String HIDE_TABLE_TASK = "HIDE_TABLE_TASK";

    /**
     * 1. validate
     * 2. online schema change
     * 3. drop gsi table
     * 4. clean up metadata
     */
    protected DropGsiJobFactory(String schemaName,
                                String primaryTableName,
                                String indexTableName,
                                ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.indexTableName = indexTableName;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        ValidateGsiExistenceTask validateTask =
            new ValidateGsiExistenceTask(schemaName, primaryTableName, indexTableName, null, null);
        validateTask.doValidate(schemaName, primaryTableName, indexTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ValidateGsiExistenceTask validateTask =
            new ValidateGsiExistenceTask(schemaName, primaryTableName, indexTableName, null, null);

        List<DdlTask> taskList = new ArrayList<>();
        //1. validate
        taskList.add(validateTask);

        //2. GSI status: public -> write_only -> delete_only -> absent
        if (!skipSchemaChange) {
            List<DdlTask> bringDownTasks =
                GsiTaskFactory.dropGlobalIndexTasks(
                    schemaName,
                    primaryTableName,
                    indexTableName);
            taskList.addAll(bringDownTasks);
        }

        // remove indexes meta for primary table
        GsiDropCleanUpTask gsiDropCleanUpTask = new GsiDropCleanUpTask(schemaName, primaryTableName, indexTableName);
        taskList.add(gsiDropCleanUpTask);
        TableSyncTask tableSyncTaskAfterCleanUpGsiIndexesMeta = new TableSyncTask(schemaName, primaryTableName);
        taskList.add(tableSyncTaskAfterCleanUpGsiIndexesMeta);

        //drop gsi physical table
        DropGsiPhyDdlTask dropGsiPhyDdlTask = new DropGsiPhyDdlTask(schemaName, primaryTableName, indexTableName);
        taskList.add(dropGsiPhyDdlTask);

        //table status: public -> absent
        DropGsiTableRemoveMetaTask dropGsiTableRemoveTableMetaTask =
            new DropGsiTableRemoveMetaTask(schemaName, primaryTableName, indexTableName);
        taskList.add(dropGsiTableRemoveTableMetaTask);

        //4. sync after drop table
        TablesSyncTask dropTableSyncTask = new TablesSyncTask(schemaName, Lists.newArrayList(primaryTableName, indexTableName));
        taskList.add(dropTableSyncTask);

        final ExecutableDdlJob4DropGsi executableDdlJob = new ExecutableDdlJob4DropGsi();
        executableDdlJob.addSequentialTasks(taskList);
        //todo delete me
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(dropTableSyncTask);
        executableDdlJob.labelTask(HIDE_TABLE_TASK, dropGsiTableRemoveTableMetaTask);

        executableDdlJob.setValidateTask(validateTask);
//        executableDdlJob.setBringDownTaskList();
        executableDdlJob.setGsiDropCleanUpTask(gsiDropCleanUpTask);
        executableDdlJob.setTableSyncTaskAfterCleanUpGsiIndexesMeta(tableSyncTaskAfterCleanUpGsiIndexesMeta);
        executableDdlJob.setDropGsiPhyDdlTask(dropGsiPhyDdlTask);
        executableDdlJob.setDropGsiTableRemoveMetaTask(dropGsiTableRemoveTableMetaTask);
        executableDdlJob.setFinalSyncTask(dropTableSyncTask);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        resources.add(concatWithDot(schemaName, indexTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    public static ExecutableDdlJob create(DropGlobalIndexPreparedData preparedData,
                                          ExecutionContext executionContext,
                                          boolean skipSchemaChange,
                                          boolean validate) {
        // TODO(moyi) merge the if-else path
        DropGsiJobFactory jobFactory;
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName());
        if (isNewPartDb) {
            DropGlobalIndexBuilder builder =
                DropPartitionGlobalIndexBuilder.createBuilder(
                    preparedData.getSchemaName(),
                    preparedData.getPrimaryTableName(),
                    preparedData.getIndexTableName(),
                    executionContext);
            if (preparedData.isRepartition()) {
                // add source partition for drop gsi
                builder.setPartitionInfo(OptimizerContext.getContext(preparedData.getSchemaName())
                    .getPartitionInfoManager()
                    .getPartitionInfo(preparedData.getPrimaryTableName()));
            }
            builder.build();
            PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();

            jobFactory = new DropPartitionGsiJobFactory(
                preparedData.getSchemaName(),
                preparedData.getPrimaryTableName(),
                preparedData.getIndexTableName(),
                physicalPlanData,
                executionContext
            );
        } else {
            jobFactory = new DropGsiJobFactory(
                preparedData.getSchemaName(),
                preparedData.getPrimaryTableName(),
                preparedData.getIndexTableName(),
                executionContext
            );
        }
        jobFactory.skipSchemaChange = skipSchemaChange;
        return jobFactory.create(validate);
    }

}
