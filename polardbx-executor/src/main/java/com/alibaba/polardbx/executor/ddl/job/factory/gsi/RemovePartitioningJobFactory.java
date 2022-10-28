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
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcRepartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.AlterTableRemovePartitioningValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RepartitionCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RepartitionSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4AlterTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionGsi;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.operator.Sub;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wumu
 */
public class RemovePartitioningJobFactory extends DdlJobFactory {
    private final String schemaName;
    private final String primaryTableName;
    private final String indexTableName;
    private final Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData;
    private final ExecutionContext executionContext;
    private final Map<String, List<String>> dropColumns;

    public RemovePartitioningJobFactory(String schemaName, String primaryTableName, String indexTableName,
                                         Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData,
                                         Map<String, List<String>> dropColumns,
                                         ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.indexTableName = indexTableName;
        this.globalIndexPrepareData = globalIndexPrepareData;
        this.dropColumns = dropColumns;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, primaryTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
        GsiValidator.validateGsiSupport(schemaName, executionContext);

        for (CreateGlobalIndexPreparedData item : globalIndexPrepareData.keySet()) {
            GsiValidator.validateCreateOnGsi(schemaName, item.getIndexTableName(), executionContext);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob alterRemovePartitioningJob = new ExecutableDdlJob();

        Map<String, Long> tableVersions = new HashMap<>();
        assert !globalIndexPrepareData.isEmpty();
        tableVersions.put(primaryTableName,
            globalIndexPrepareData.keySet().stream().findFirst().get().getTableVersion());
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, tableVersions);

        // alter table remove partitioning validate
        List<TableGroupConfig> tableGroupConfigs = globalIndexPrepareData.values().stream()
            .map(PhysicalPlanData::getTableGroupConfig).collect(Collectors.toList());

        AlterTableRemovePartitioningValidateTask removePartitioningValidateTask =
            new AlterTableRemovePartitioningValidateTask(schemaName, primaryTableName, indexTableName,
                Collections.singletonList(indexTableName), tableGroupConfigs);

        // create gsi
        List<ExecutableDdlJob4CreatePartitionGsi> createGsiJobs = new ArrayList<>();
        globalIndexPrepareData.forEach((createGlobalIndexPreparedData, physicalPlanData) -> {
            CreateGsiJobFactory createGsiJobFactory =
                CreateGsiJobFactory.create(createGlobalIndexPreparedData, physicalPlanData, executionContext);
            if (StringUtils.equalsIgnoreCase(createGlobalIndexPreparedData.getIndexTableName(), indexTableName)) {
                createGsiJobFactory.stayAtBackFill = true;
            }
            createGsiJobs.add((ExecutableDdlJob4CreatePartitionGsi) createGsiJobFactory.create());
        });

        RepartitionCutOverTask cutOverTask =
            new RepartitionCutOverTask(schemaName, primaryTableName, indexTableName, false, false, true);
        RepartitionSyncTask repartitionSyncTask = new RepartitionSyncTask(schemaName, primaryTableName, indexTableName);

        // cdc
        DdlTask cdcDdlMarkTask = new CdcRepartitionMarkTask(schemaName, primaryTableName, SqlKind.ALTER_TABLE);

        // drop gsi
        DropGlobalIndexPreparedData dropGlobalIndexPreparedData =
            new DropGlobalIndexPreparedData(schemaName, primaryTableName, indexTableName, false);
        dropGlobalIndexPreparedData.setRepartition(true);
        ExecutableDdlJob dropGsiJob =
            DropGsiJobFactory.create(dropGlobalIndexPreparedData, executionContext, false, false);
        // rollback is not supported after CutOver
        dropGsiJob.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);

        // reload table group
        List<TableGroupConfig> tableGroupConfigList =
            FactoryUtils.getTableGroupConfigByTableName(schemaName, Collections.singletonList(primaryTableName));
        assert tableGroupConfigList.size() == 1;
        TableGroupSyncTask tableGroupSyncTask =
            new TableGroupSyncTask(schemaName, tableGroupConfigList.get(0).getTableGroupRecord().getTg_name());

        List<DdlTask> subJobTasks = genDropGsiColumns();

        alterRemovePartitioningJob.addTask(validateTableVersionTask);
        alterRemovePartitioningJob.addTaskRelationship(validateTableVersionTask, removePartitioningValidateTask);
        createGsiJobs.forEach(alterRemovePartitioningJob::appendJob2);

        alterRemovePartitioningJob.addTaskRelationship(createGsiJobs.get(createGsiJobs.size() - 1).getLastTask(),
            cutOverTask);
        alterRemovePartitioningJob.addTaskRelationship(cutOverTask, repartitionSyncTask);
        alterRemovePartitioningJob.addTaskRelationship(repartitionSyncTask, cdcDdlMarkTask);

        alterRemovePartitioningJob.appendJob2(dropGsiJob);

        alterRemovePartitioningJob.addTaskRelationship(
            ((ExecutableDdlJob4DropPartitionGsi) dropGsiJob).getFinalSyncTask(), tableGroupSyncTask);

        if (!subJobTasks.isEmpty()) {
            alterRemovePartitioningJob.addSequentialTasks(subJobTasks);
            alterRemovePartitioningJob.addTaskRelationship(tableGroupSyncTask, subJobTasks.get(0));
        }

        return alterRemovePartitioningJob;
    }


    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        for (CreateGlobalIndexPreparedData item : globalIndexPrepareData.keySet()) {
            resources.add(concatWithDot(schemaName, item.getIndexTableName()));
        }

        // lock table group of primary table
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(primaryTableName);
        if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

    private List<DdlTask> genDropGsiColumns() {
        if (dropColumns == null || dropColumns.isEmpty()) {
            return new ArrayList<>();
        }

        List<DdlTask> tasks = new ArrayList<>();
        dropColumns.forEach((k, v) -> {
            String sql = GsiTaskFactory.genAlterGlobalIndexDropColumnsSql(k, v);
            SubJobTask subJobTask = new SubJobTask(schemaName, sql, null);
            subJobTask.setParentAcquireResource(true);
            tasks.add(subJobTask);
        });

        return tasks;
    }
}
