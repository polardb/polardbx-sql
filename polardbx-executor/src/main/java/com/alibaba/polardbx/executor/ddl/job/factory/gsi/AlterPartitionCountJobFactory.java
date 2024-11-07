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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcRepartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.AlterPartitionCountCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.AlterPartitionCountSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.AlterPartitionCountValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupsSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionGsi;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author wumu
 */
public class AlterPartitionCountJobFactory extends DdlJobFactory {
    private final String schemaName;
    private final String primaryTableName;
    private final Map<String, String> tableNameMap;
    private final Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData;
    private final ExecutionContext executionContext;

    public AlterPartitionCountJobFactory(String schemaName, String primaryTableName, Map<String, String> tableNameMap,
                                         Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData,
                                         ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.tableNameMap = tableNameMap;
        this.globalIndexPrepareData = globalIndexPrepareData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, primaryTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
        GsiValidator.validateGsiSupport(schemaName, executionContext);

        for (String indexTableName : tableNameMap.values()) {
            GsiValidator.validateCreateOnGsi(schemaName, indexTableName, executionContext);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob alterPartitionCountJob = new ExecutableDdlJob();

        Map<String, Long> tableVersions = new HashMap<>();
        assert !globalIndexPrepareData.isEmpty();
        tableVersions.put(primaryTableName,
            globalIndexPrepareData.keySet().stream().findFirst().get().getTableVersion());
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, tableVersions);

        // alter table partitions validate
        // get old table groups
        List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
        List<TableGroupConfig> oldTableGroupConfigs =
            FactoryUtils.getTableGroupConfigByTableName(schemaName, new ArrayList<>(tableNameMap.keySet()));
        tableGroupConfigs.addAll(oldTableGroupConfigs);
        // get new table groups
        tableGroupConfigs.addAll(
            globalIndexPrepareData.values().stream()
                .map(PhysicalPlanData::getTableGroupConfig).collect(Collectors.toList())
        );
        AlterPartitionCountValidateTask validateTask =
            new AlterPartitionCountValidateTask(schemaName, primaryTableName, tableNameMap, tableGroupConfigs);

        // create gsi
        List<ExecutableDdlJob4CreatePartitionGsi> createGsiJobs = new ArrayList<>();
        globalIndexPrepareData.forEach((createGlobalIndexPreparedData, physicalPlanData) -> {
            CreateGsiJobFactory createGsiJobFactory =
                CreateGsiJobFactory.create(createGlobalIndexPreparedData, physicalPlanData, null, executionContext);
            createGsiJobFactory.stayAtBackFill = true;
            createGsiJobs.add((ExecutableDdlJob4CreatePartitionGsi) createGsiJobFactory.create());
        });

        // cut over
        AlterPartitionCountCutOverTask cutOverTask =
            new AlterPartitionCountCutOverTask(schemaName, primaryTableName, tableNameMap);
        AlterPartitionCountSyncTask
            alterPartitionCountSyncTask = new AlterPartitionCountSyncTask(schemaName, primaryTableName, tableNameMap);

        // cdc
        if (executionContext.getDdlContext().isSubJob()) {
            throw new RuntimeException("unexpected parent ddl job");
        }
        DdlTask cdcDdlMarkTask = new CdcRepartitionMarkTask(
            schemaName, primaryTableName, SqlKind.ALTER_TABLE, CdcDdlMarkVisibility.Protected);

        // drop gsi
        List<ExecutableDdlJob> dropGsiJobs = new ArrayList<>();
        for (Map.Entry<String, String> entries : tableNameMap.entrySet()) {
            String newIndexTableName = entries.getValue();
            DropGlobalIndexPreparedData dropGlobalIndexPreparedData =
                new DropGlobalIndexPreparedData(schemaName, primaryTableName, newIndexTableName, false);
            dropGlobalIndexPreparedData.setRepartition(true);
            dropGlobalIndexPreparedData.setRepartitionTableName(entries.getKey());
            ExecutableDdlJob dropGsiJob =
                DropGsiJobFactory.create(dropGlobalIndexPreparedData, executionContext, false, false);
            // rollback is not supported after CutOver
            dropGsiJob.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
            dropGsiJobs.add(dropGsiJob);
        }

        // table groups sync task
        TableGroupsSyncTask tableGroupsSyncTask = new TableGroupsSyncTask(schemaName,
            oldTableGroupConfigs.stream()
                .map(e -> e.getTableGroupRecord().getTg_name())
                .collect(Collectors.toList())
        );

        alterPartitionCountJob.addTask(validateTableVersionTask);
        alterPartitionCountJob.addTaskRelationship(validateTableVersionTask, validateTask);
        createGsiJobs.forEach(alterPartitionCountJob::appendJob2);

        alterPartitionCountJob.addTaskRelationship(createGsiJobs.get(createGsiJobs.size() - 1).getLastTask(),
            cutOverTask);
        alterPartitionCountJob.addTaskRelationship(cutOverTask, alterPartitionCountSyncTask);
        alterPartitionCountJob.addTaskRelationship(alterPartitionCountSyncTask, cdcDdlMarkTask);

        dropGsiJobs.forEach(alterPartitionCountJob::appendJob2);

        alterPartitionCountJob.addTaskRelationship(
            ((ExecutableDdlJob4DropPartitionGsi) dropGsiJobs.get(dropGsiJobs.size() - 1)).getFinalSyncTask(),
            tableGroupsSyncTask);

        return alterPartitionCountJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        for (String indexTableName : tableNameMap.values()) {
            resources.add(concatWithDot(schemaName, indexTableName));
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
}
