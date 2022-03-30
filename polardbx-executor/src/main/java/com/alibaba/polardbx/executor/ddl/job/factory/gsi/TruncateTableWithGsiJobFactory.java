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

import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreatePartitionTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateTableWithGsiBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.CreatePartitionTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTruncateTmpPrimaryTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTruncateTableWithGsiMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.TruncateCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.TruncateSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.TruncateTableWithGsiValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateTable;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateTableWithGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropTableWithGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4TruncateTableWithGsiCutOver;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TruncateUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TruncateTableWithGsiJobFactory extends DdlJobFactory {
    private final String schemaName;
    private final String logicalTableName;
    private final String tmpPrimaryTableName;
    private final Map<String, String> tmpIndexTableMap;
    private final TruncateTableWithGsiPreparedData preparedData;
    private final ExecutionContext executionContext;
    private final boolean isNewPartDb;

    public TruncateTableWithGsiJobFactory(TruncateTableWithGsiPreparedData preparedData,
                                          ExecutionContext executionContext) {
        this.schemaName = preparedData.getSchemaName();
        this.logicalTableName = preparedData.getPrimaryTableName();
        this.tmpIndexTableMap = preparedData.getTmpIndexTableMap();
        this.preparedData = preparedData;
        this.executionContext = executionContext;
        this.isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        String tmpTableSuffix = preparedData.getTmpTableSuffix();
        this.tmpPrimaryTableName = TruncateUtil.generateTmpTableName(logicalTableName, tmpTableSuffix);
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);
        GsiValidator.validateAllowTruncateOnTable(schemaName, logicalTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob result = new ExecutableDdlJob();

        // validate task
        List<String> tableNames = new ArrayList<>();
        tableNames.add(logicalTableName);
        tableNames.addAll(tmpIndexTableMap.keySet());
        List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
        TruncateTableWithGsiValidateTask truncateTableWithGsiValidateTask =
            new TruncateTableWithGsiValidateTask(schemaName, logicalTableName,
                new ArrayList<>(tmpIndexTableMap.keySet()), tableGroupConfigs);

        // Some tasks are different from Create/DropTableWithGsiJobFactory because no one will use tmp table
        // For example, we remove physical table without hiding table meta first
        ExecutableDdlJob4CreateTableWithGsi createTmpTableJob =
            isNewPartDb ? generateCreateTmpPartitionTableJob() : generateCreateTmpTableJob();
        ExecutableDdlJob4TruncateTableWithGsiCutOver cutOverJob = generateCutOverJob();
        ExecutableDdlJob4DropTableWithGsi dropTmpTableJob =
            isNewPartDb ? generateDropTmpPartitionTableJob() : generateDropTmpTableJob();

        result.combineTasks(createTmpTableJob);
        result.combineTasks(cutOverJob);
        result.combineTasks(dropTmpTableJob);

        result.addTaskRelationship(createTmpTableJob.getTableSyncTask(), cutOverJob.getCdcMarkTask());
        result.addTaskRelationship(cutOverJob.getTruncateSyncTask(), dropTmpTableJob.getValidateTask());

        // DdlTask validateTask = new TruncateTableValidateTask(schemaName, logicalTableName, null);
        // result.addTaskRelationship(validateTask, createTmpTableJob.getTableValidateTask());
        result.addTaskRelationship(truncateTableWithGsiValidateTask, createTmpTableJob.getTableValidateTask());

        result
            .setExceptionActionForAllSuccessor(cutOverJob.getCdcMarkTask(), DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);

        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
        resources.add(concatWithDot(schemaName, tmpPrimaryTableName));

        String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, logicalTableName);
        if (tgName != null) {
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    private ExecutableDdlJob4TruncateTableWithGsiCutOver generateCutOverJob() {
        ExecutableDdlJob4TruncateTableWithGsiCutOver cutOverJob = new ExecutableDdlJob4TruncateTableWithGsiCutOver();
        CdcTruncateTableWithGsiMarkTask cdcTask =
            new CdcTruncateTableWithGsiMarkTask(schemaName, logicalTableName, tmpPrimaryTableName);
        TruncateCutOverTask cutOverTask =
            new TruncateCutOverTask(schemaName, logicalTableName, tmpIndexTableMap, tmpPrimaryTableName);
        TruncateSyncTask syncTask =
            new TruncateSyncTask(schemaName, logicalTableName, tmpPrimaryTableName, tmpIndexTableMap.keySet());

        cutOverJob.addSequentialTasks(Lists.newArrayList(
            cdcTask,
            cutOverTask,
            syncTask
        ));

        cutOverJob.setCdcMarkTask(cdcTask);
        cutOverJob.setTruncateSyncTask(syncTask);
        return cutOverJob;
    }

    private ExecutableDdlJob4CreateTableWithGsi generateCreateTmpTableJob() {
        ExecutableDdlJob4CreateTableWithGsi result = new ExecutableDdlJob4CreateTableWithGsi();

        LogicalCreateTable logicalCreateTable = preparedData.getLogicalCreateTable();
        CreateTableWithGsiPreparedData createTablePreparedData = logicalCreateTable.getCreateTableWithGsiPreparedData();

        CreateTableWithGsiBuilder createTableWithGsiBuilder =
            new CreateTableWithGsiBuilder(logicalCreateTable.relDdl, createTablePreparedData, executionContext);
        createTableWithGsiBuilder.build();

        Map<String, List<List<String>>> primaryTableTopology = createTableWithGsiBuilder.getPrimaryTableTopology();
        List<PhyDdlTableOperation> primaryTablePhysicalPlans = createTableWithGsiBuilder.getPrimaryTablePhysicalPlans();
        boolean isAutoPartition = createTablePreparedData.getPrimaryTablePreparedData().isAutoPartition();
        boolean hasTimestampColumnDefault =
            createTablePreparedData.getPrimaryTablePreparedData().isTimestampColumnDefault();
        Map<String, String> binaryColumnDefaultValues =
            createTablePreparedData.getPrimaryTablePreparedData().getBinaryColumnDefaultValues();
        PhysicalPlanData physicalPlanData = DdlJobDataConverter
            .convertToPhysicalPlanData(primaryTableTopology, primaryTablePhysicalPlans, false, isAutoPartition);

        // Create Primary Table
        ExecutableDdlJob4CreateTable createTableJob = (ExecutableDdlJob4CreateTable) new CreateTableJobFactory(
            false,
            hasTimestampColumnDefault,
            binaryColumnDefaultValues,
            physicalPlanData,
            executionContext).create();

        result.addSequentialTasks(Lists.newArrayList(
                createTableJob.getCreateTableValidateTask(),
                createTableJob.getCreateTableAddTablesExtMetaTask(),
                createTableJob.getCreateTablePhyDdlTask(),
                createTableJob.getCreateTableAddTablesMetaTask(),
                createTableJob.getCreateTableShowTableMetaTask(),
                createTableJob.getTableSyncTask()
            )
        );
        result.removeTaskRelationship(
            createTableJob.getCreateTableShowTableMetaTask(),
            createTableJob.getTableSyncTask()
        );
        result.addExcludeResources(createTableJob.getExcludeResources());

        // Create Index Table
        Map<String, CreateGlobalIndexPreparedData> gsiPreparedDataMap =
            createTablePreparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, CreateGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final CreateGlobalIndexPreparedData gsiPreparedData = entry.getValue();
            ExecutableDdlJob4CreateGsi gsiJob = (ExecutableDdlJob4CreateGsi)
                CreateGsiJobFactory
                    .create4CreateTableWithGsi(logicalCreateTable.relDdl, gsiPreparedData, executionContext);
            result.addSequentialTasks(Lists.newArrayList(
                    gsiJob.getCreateGsiValidateTask(),
                    gsiJob.getCreateTableAddTablesExtMetaTask(),
                    gsiJob.getCreateGsiPhyDdlTask(),
                    gsiJob.getCreateTableAddTablesMetaTask(),
                    gsiJob.getCreateTableShowTableMetaTask(),
                    gsiJob.getGsiInsertIndexMetaTask(),
                    gsiJob.getLastUpdateGsiStatusTask()
                )
            );
            result.addTaskRelationship(
                createTableJob.getCreateTableShowTableMetaTask(), gsiJob.getCreateGsiValidateTask());
            result.addTaskRelationship(
                gsiJob.getLastUpdateGsiStatusTask(), createTableJob.getTableSyncTask());

            result.addExcludeResources(gsiJob.getExcludeResources());
        }

        result.setExceptionActionForAllSuccessor(createTableJob.getCreateTableAddTablesMetaTask(),
            DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);

        result.setTableValidateTask(createTableJob.getCreateTableValidateTask());
        result.setTableSyncTask(createTableJob.getTableSyncTask());

        return result;
    }

    private ExecutableDdlJob4CreateTableWithGsi generateCreateTmpPartitionTableJob() {
        ExecutableDdlJob4CreateTableWithGsi result = new ExecutableDdlJob4CreateTableWithGsi();

        LogicalCreateTable logicalCreateTable = preparedData.getLogicalCreateTable();
        CreateTableWithGsiPreparedData createTablePreparedData = logicalCreateTable.getCreateTableWithGsiPreparedData();

        CreatePartitionTableWithGsiBuilder createTableWithGsiBuilder =
            new CreatePartitionTableWithGsiBuilder(logicalCreateTable.relDdl, createTablePreparedData,
                executionContext);
        createTableWithGsiBuilder.build();

        Map<String, List<List<String>>> primaryTableTopology = createTableWithGsiBuilder.getPrimaryTableTopology();
        List<PhyDdlTableOperation> primaryTablePhysicalPlans = createTableWithGsiBuilder.getPrimaryTablePhysicalPlans();
        boolean isAutoPartition = createTablePreparedData.getPrimaryTablePreparedData().isAutoPartition();
        boolean hasTimestampColumnDefault =
            createTablePreparedData.getPrimaryTablePreparedData().isTimestampColumnDefault();
        Map<String, String> binaryColumnDefaultValues =
            createTablePreparedData.getPrimaryTablePreparedData().getBinaryColumnDefaultValues();
        PhysicalPlanData physicalPlanData = DdlJobDataConverter
            .convertToPhysicalPlanData(primaryTableTopology, primaryTablePhysicalPlans, false, isAutoPartition);

        // Create Primary Table
        ExecutableDdlJob4CreatePartitionTable createTableJob = (ExecutableDdlJob4CreatePartitionTable)
            new CreatePartitionTableJobFactory(isAutoPartition, hasTimestampColumnDefault, binaryColumnDefaultValues,
                physicalPlanData, executionContext, createTablePreparedData.getPrimaryTablePreparedData(), null).create();

        result.addSequentialTasks(Lists.newArrayList(
                createTableJob.getCreatePartitionTableValidateTask(),
                createTableJob.getCreateTableAddTablesPartitionInfoMetaTask(),
                createTableJob.getCreateTablePhyDdlTask(),
                createTableJob.getCreateTableAddTablesMetaTask(),
                createTableJob.getCreateTableShowTableMetaTask(),
                createTableJob.getTableSyncTask()
            )
        );
        result.removeTaskRelationship(
            createTableJob.getCreateTableShowTableMetaTask(),
            createTableJob.getTableSyncTask()
        );

        // Create Index Table
        Map<String, CreateGlobalIndexPreparedData> gsiPreparedDataMap =
            createTablePreparedData.getIndexTablePreparedDataMap();
        for (Map.Entry<String, CreateGlobalIndexPreparedData> entry : gsiPreparedDataMap.entrySet()) {
            final CreateGlobalIndexPreparedData gsiPreparedData = entry.getValue();
            ExecutableDdlJob4CreatePartitionGsi gsiJob =
                (ExecutableDdlJob4CreatePartitionGsi) CreatePartitionGsiJobFactory
                    .create4CreateTableWithGsi(logicalCreateTable.relDdl, gsiPreparedData, executionContext);
            result.addSequentialTasks(Lists.newArrayList(
                    gsiJob.getCreateGsiValidateTask(),
                    gsiJob.getCreateTableAddTablesPartitionInfoMetaTask(),
                    gsiJob.getCreateGsiPhyDdlTask(),
                    gsiJob.getCreateTableAddTablesMetaTask(),
                    gsiJob.getCreateTableShowTableMetaTask(),
                    gsiJob.getGsiInsertIndexMetaTask(),
                    gsiJob.getLastUpdateGsiStatusTask()
                )
            );
            result.addTaskRelationship(
                createTableJob.getCreateTableShowTableMetaTask(), gsiJob.getCreateGsiValidateTask());
            result.addTaskRelationship(
                gsiJob.getLastUpdateGsiStatusTask(), createTableJob.getTableSyncTask());
        }

        result.setExceptionActionForAllSuccessor(createTableJob.getCreateTableAddTablesPartitionInfoMetaTask(),
            DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);

        result.setTableValidateTask(createTableJob.getCreatePartitionTableValidateTask());
        result.setTableSyncTask(createTableJob.getTableSyncTask());

        return result;
    }

    private ExecutableDdlJob4DropTableWithGsi generateDropTmpTableJob() {
        // Drop primary Table
        ExecutableDdlJob dropTmpPrimaryTableJob = new ExecutableDdlJob();
        DropTableValidateTask validateTask = new DropTableValidateTask(schemaName, tmpPrimaryTableName);
        DdlTask phyDdlTask = new DropTruncateTmpPrimaryTablePhyDdlTask(schemaName, tmpPrimaryTableName);
        DdlTask removeMetaTask = new DropTableRemoveMetaTask(schemaName, tmpPrimaryTableName);
        DdlTask tableSyncTask = new TableSyncTask(schemaName, tmpPrimaryTableName);
        dropTmpPrimaryTableJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            phyDdlTask,
            removeMetaTask,
            tableSyncTask
        ));
        dropTmpPrimaryTableJob.removeTaskRelationship(validateTask, phyDdlTask);

        // Drop GSI Tables
        ExecutableDdlJob4DropTableWithGsi dropTmpTableJob = new ExecutableDdlJob4DropTableWithGsi();
        dropTmpTableJob.appendJob(dropTmpPrimaryTableJob);

        Map<String, String> tmpIndexTableMap = preparedData.getTmpIndexTableMap();

        for (String tmpIndexTableName : tmpIndexTableMap.values()) {
            DropGsiJobFactory jobFactory =
                new DropGsiJobFactory(schemaName, tmpPrimaryTableName, tmpIndexTableName, executionContext);
            jobFactory.skipSchemaChange = true;
            ExecutableDdlJob4DropGsi dropGsiJob = (ExecutableDdlJob4DropGsi) jobFactory.create(false);

            dropTmpTableJob.addTaskRelationship(dropGsiJob.getValidateTask(), phyDdlTask);
            dropTmpTableJob.addTaskRelationship(validateTask, dropGsiJob.getValidateTask());

            dropTmpTableJob.addTaskRelationship(tableSyncTask, dropGsiJob.getDropGsiPhyDdlTask());
            dropTmpTableJob.addTaskRelationship(
                dropGsiJob.getDropGsiPhyDdlTask(), dropGsiJob.getGsiDropCleanUpTask());
            dropTmpTableJob.addTaskRelationship(
                dropGsiJob.getGsiDropCleanUpTask(), dropGsiJob.getDropGsiTableRemoveMetaTask());
            dropTmpTableJob
                .addTaskRelationship(dropGsiJob.getDropGsiTableRemoveMetaTask(), dropGsiJob.getFinalSyncTask());
        }
        dropTmpTableJob.setValidateTask(validateTask);
        return dropTmpTableJob;
    }

    private ExecutableDdlJob4DropTableWithGsi generateDropTmpPartitionTableJob() {
        // Drop primary Table

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tmpPrimaryTableName);
        Long tableGroupId = -1L;
        if (partitionInfo != null) {
            tableGroupId = partitionInfo.getTableGroupId();
        }
        List<DdlTask> tasks = new ArrayList<>();
        ExecutableDdlJob dropTmpPrimaryTableJob = new ExecutableDdlJob();
        DropTableValidateTask validateTask = new DropTableValidateTask(schemaName, tmpPrimaryTableName);
        DdlTask phyDdlTask = new DropTruncateTmpPrimaryTablePhyDdlTask(schemaName, tmpPrimaryTableName);
        DdlTask removeMetaTask = new DropPartitionTableRemoveMetaTask(schemaName, tmpPrimaryTableName);
        DdlTask syncTableGroup = null;
        if (tableGroupId != -1) {
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
            TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
            syncTableGroup =
                new TableGroupSyncTask(schemaName, tableGroupConfig.getTableGroupRecord().getTg_name());
        }
        DdlTask tableSyncTask = new TableSyncTask(schemaName, tmpPrimaryTableName);

        tasks.add(validateTask);
        tasks.add(phyDdlTask);
        tasks.add(removeMetaTask);
        if (syncTableGroup != null) {
            tasks.add(syncTableGroup);
        }
        tasks.add(tableSyncTask);

        dropTmpPrimaryTableJob.addSequentialTasks(tasks);
        dropTmpPrimaryTableJob.removeTaskRelationship(validateTask, phyDdlTask);

        // Drop GSI Tables
        ExecutableDdlJob4DropTableWithGsi dropTmpTableJob = new ExecutableDdlJob4DropTableWithGsi();
        dropTmpTableJob.appendJob(dropTmpPrimaryTableJob);

        Map<String, String> tmpIndexTableMap = preparedData.getTmpIndexTableMap();

        for (String tmpIndexTableName : tmpIndexTableMap.values()) {
            DropGsiJobFactory jobFactory =
                new DropPartitionGsiJobFactory(schemaName, tmpPrimaryTableName, tmpIndexTableName, null,
                    executionContext);
            jobFactory.skipSchemaChange = true;
            ExecutableDdlJob4DropPartitionGsi dropGsiJob =
                (ExecutableDdlJob4DropPartitionGsi) jobFactory.create(false);

            dropTmpTableJob.addTaskRelationship(dropGsiJob.getValidateTask(), phyDdlTask);
            dropTmpTableJob.addTaskRelationship(validateTask, dropGsiJob.getValidateTask());

            dropTmpTableJob.addTaskRelationship(tableSyncTask, dropGsiJob.getDropGsiPhyDdlTask());
            dropTmpTableJob.addTaskRelationship(
                dropGsiJob.getDropGsiPhyDdlTask(), dropGsiJob.getGsiDropCleanUpTask());
            dropTmpTableJob.addTaskRelationship(
                dropGsiJob.getGsiDropCleanUpTask(), dropGsiJob.getDropGsiTableRemoveMetaTask());
            dropTmpTableJob
                .addTaskRelationship(dropGsiJob.getDropGsiTableRemoveMetaTask(), dropGsiJob.getFinalSyncTask());
        }
        dropTmpTableJob.setValidateTask(validateTask);
        return dropTmpTableJob;
    }
}
