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

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcGsiDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropGsiTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropPartitionGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateGsiExistenceTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionGsi;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 1. drop index xxx on yyy
 * 2. alter table yyy drop index xxx
 * <p>
 * for drop table with [unique] gsi, see class: DropTableWithGlobalIndexJob
 *
 * @author guxu
 */
public class DropPartitionGsiJobFactory extends DropGsiJobFactory {

    /**
     * 1. validate
     * 2. online schema change
     * 3. drop gsi table
     * 4. clean up metadata
     */
    final private PhysicalPlanData physicalPlanData;
    private List<Long> tableGroupIds = new ArrayList<>();

    public DropPartitionGsiJobFactory(String schemaName,
                                      String primaryTableName,
                                      String indexTableName,
                                      PhysicalPlanData physicalPlanData,
                                      ExecutionContext executionContext) {
        super(schemaName, primaryTableName, indexTableName, physicalPlanData, executionContext);
        this.physicalPlanData = physicalPlanData;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, indexTableName));

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(indexTableName);
        if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
            tableGroupIds.add(partitionInfo.getTableGroupId());
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(indexTableName);
        Long tableGroupId = -1L;
        TableGroupConfig tableGroupConfig = null;
        if (partitionInfo != null) {
            tableGroupId = partitionInfo.getTableGroupId();
            tableGroupConfig = physicalPlanData.getTableGroupConfig();
        }

        ValidateGsiExistenceTask validateTask =
            new ValidateGsiExistenceTask(schemaName, primaryTableName, indexTableName, tableGroupIds, tableGroupConfig);

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

        //3.1 table status: public -> absent
        DropGsiTableHideTableMetaTask dropGsiTableHideTableMetaTask =
            new DropGsiTableHideTableMetaTask(schemaName, primaryTableName, indexTableName);
        taskList.add(dropGsiTableHideTableMetaTask);

        //3.2 drop gsi physical table
        DropPartitionGsiPhyDdlTask dropPartitionGsiPhyDdlTask =
            new DropPartitionGsiPhyDdlTask(schemaName, primaryTableName, indexTableName, physicalPlanData);
        taskList.add(dropPartitionGsiPhyDdlTask);

        //3.3 remove indexes meta for primary table
        GsiDropCleanUpTask gsiDropCleanUpTask = new GsiDropCleanUpTask(schemaName, primaryTableName, indexTableName);
        taskList.add(gsiDropCleanUpTask);

        TableSyncTask tableSyncTaskAfterCleanUpGsiIndexesMeta = new TableSyncTask(schemaName, primaryTableName);
        taskList.add(tableSyncTaskAfterCleanUpGsiIndexesMeta);

        //4. remove table meta for gsi table
        DropTableRemoveMetaTask dropGsiTableRemoveMetaTask = new DropTableRemoveMetaTask(schemaName, indexTableName);
        taskList.add(dropGsiTableRemoveMetaTask);

        if (!skipSchemaChange && !repartition) {
            //mark gsi task
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
            if (!tableMeta.isAutoPartition()) {
                CdcGsiDdlMarkTask cdcDdlMarkTask = new CdcGsiDdlMarkTask(schemaName, physicalPlanData,
                    primaryTableName, executionContext.getOriginSql());
                taskList.add(cdcDdlMarkTask);
            }
        }

        if (tableGroupId != -1) {
            //tableGroupConfig from physicalPlanData is not set tableGroup record
            tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigById(tableGroupId);
            DdlTask syncTableGroup =
                new TableGroupSyncTask(schemaName, tableGroupConfig.getTableGroupRecord().getTg_name());
            taskList.add(syncTableGroup);
        }

        //5. sync after drop table
        TablesSyncTask dropTableSyncTask =
            new TablesSyncTask(schemaName, Lists.newArrayList(indexTableName, primaryTableName));
        taskList.add(dropTableSyncTask);

//        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
//        executableDdlJob.addSequentialTasks(taskList);
//        executableDdlJob.labelAsHead(validateTask);
//        executableDdlJob.labelAsTail(dropTableSyncTask);

        final ExecutableDdlJob4DropPartitionGsi executableDdlJob = new ExecutableDdlJob4DropPartitionGsi();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(dropTableSyncTask);
        executableDdlJob.labelTask(HIDE_TABLE_TASK, dropGsiTableHideTableMetaTask);

        executableDdlJob.setValidateTask(validateTask);
        executableDdlJob.setDropGsiTableHideTableMetaTask(dropGsiTableHideTableMetaTask);
        executableDdlJob.setDropGsiPhyDdlTask(dropPartitionGsiPhyDdlTask);
        executableDdlJob.setGsiDropCleanUpTask(gsiDropCleanUpTask);
        executableDdlJob.setTableSyncTaskAfterCleanUpGsiIndexesMeta(tableSyncTaskAfterCleanUpGsiIndexesMeta);
        executableDdlJob.setDropGsiTableRemoveMetaTask(dropGsiTableRemoveMetaTask);
        executableDdlJob.setFinalSyncTask(dropTableSyncTask);

        return executableDdlJob;
    }

    public static ExecutableDdlJob create(DropGlobalIndexPreparedData preparedData,
                                          PhysicalPlanData physicalPlanData,
                                          ExecutionContext executionContext,
                                          boolean validate) {
        return new DropPartitionGsiJobFactory(
            preparedData.getSchemaName(),
            preparedData.getPrimaryTableName(),
            preparedData.getIndexTableName(),
            physicalPlanData,
            executionContext
        ).create(validate);
    }
}
