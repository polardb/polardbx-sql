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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterColumnDefaultTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterMultiTableChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterMultiTablesHideMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AtomicTablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RemoveAbsentColumnMappingTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4AlterTable;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;

/**
 * alter table which binding to an oss table
 */
public class AlterTableWithFileStoreJobFactory extends AlterTableJobFactory {

    private final PhysicalPlanData physicalPlanFileStoreData;

    private final AlterTablePreparedData preparedFileStoreData;

    private final String fileStoreSchema;

    private final String fileStoreTable;

    public AlterTableWithFileStoreJobFactory(PhysicalPlanData physicalPlanData,
                                             AlterTablePreparedData preparedData,
                                             PhysicalPlanData physicalPlanFileStoreData,
                                             AlterTablePreparedData preparedFileStoreData,
                                             LogicalAlterTable logicalAlterTable,
                                             ExecutionContext executionContext) {
        super(physicalPlanData, preparedData, logicalAlterTable, executionContext);
        this.physicalPlanFileStoreData = physicalPlanFileStoreData;
        this.preparedFileStoreData = preparedFileStoreData;
        this.fileStoreSchema = preparedFileStoreData.getSchemaName();
        this.fileStoreTable = preparedFileStoreData.getTableName();
    }

    @Override
    protected ExecutableDdlJob doCreate() {

        boolean crossSchema = !StringUtils.equalsIgnoreCase(schemaName, fileStoreSchema);
        DdlTask validateTask =
            this.validateExistence ?
                new AlterTableValidateTask(schemaName, logicalTableName,
                    logicalAlterTable.getSqlAlterTable().getSourceSql(), prepareData.getTableVersion(),
                    physicalPlanData.getTableGroupConfig()) :
                new EmptyTask(schemaName);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(fileStoreTable, preparedFileStoreData.getTableVersion());

        DdlTask validateFileStoreTask =
            this.validateExistence ?
                new ValidateTableVersionTask(fileStoreSchema, tableVersions)
                : new EmptyTask(fileStoreSchema);

        final boolean isDropColumnOrDropIndex =
            CollectionUtils.isNotEmpty(prepareData.getDroppedColumns())
                || CollectionUtils.isNotEmpty(prepareData.getDroppedIndexes());

        DdlTask beginAlterColumnDefault = null;
        DdlTask beginAlterColumnDefaultSyncTask = null;

        if (CollectionUtils.isNotEmpty(prepareData.getAlterDefaultColumns())) {
            beginAlterColumnDefault =
                new AlterColumnDefaultTask(schemaName, logicalTableName, prepareData.getAlterDefaultColumns(), true);
            beginAlterColumnDefaultSyncTask = new TableSyncTask(schemaName, logicalTableName);
        }

        DdlTask phyDdlTask = new AlterTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);
        DdlTask phyDdlFileStoreTask =
            new AlterTablePhyDdlTask(fileStoreSchema, fileStoreTable, physicalPlanFileStoreData);

        physicalPlanData.setAlterTablePreparedData(prepareData);
        physicalPlanFileStoreData.setAlterTablePreparedData(preparedFileStoreData);
        DdlTask cdcDdlMarkTask = this.prepareData.isOnlineModifyColumnIndexTask() ? null :
            new CdcDdlMarkTask(schemaName, physicalPlanData, false, false, DEFAULT_DDL_VERSION_ID);

        List<String> schemas = Lists.newArrayList(schemaName, fileStoreSchema);
        List<String> tables = Lists.newArrayList(logicalTableName, fileStoreTable);
        List<AlterMultiTableChangeMetaTask.TableChange> changes = Lists.newArrayList(
            AlterMultiTableChangeMetaTask.TableChange.build(schemaName, logicalTableName,
                physicalPlanData, prepareData,
                false),
            AlterMultiTableChangeMetaTask.TableChange.build(fileStoreSchema, fileStoreTable,
                physicalPlanFileStoreData, preparedFileStoreData,
                true)
        );
        DdlTask updateMetaTask = new AlterMultiTableChangeMetaTask(schemas, tables, changes);

        DdlTask tableSyncTaskAfterShowing = buildAtomicSyncTask(crossSchema);

        ExecutableDdlJob4AlterTable executableDdlJob = new ExecutableDdlJob4AlterTable();

        List<DdlTask> taskList;
        if (isDropColumnOrDropIndex) {
            // 1. sync table(for rollback)
            // 2. hide meta
            // 3. sync table
            // 4. physical DDL
            // 5. physical DDL on archive table
            // 6. update meta
            // 7. sync table
            // 8. remove hidden column mapping
            DdlTask tableSyncTaskBeforeHiding = buildAtomicSyncTask(crossSchema);

            DdlTask hideMetaTask =
                new AlterMultiTablesHideMetaTask(schemas, tables,
                    Lists.newArrayList(prepareData.getDroppedColumns(), preparedFileStoreData.getDroppedColumns()),
                    Lists.newArrayList(prepareData.getDroppedIndexes(), preparedFileStoreData.getDroppedIndexes()));
            DdlTask removeColumnMappingTask = new RemoveAbsentColumnMappingTask(fileStoreSchema, fileStoreTable);
            DdlTask tableSyncTaskAfterHiding = buildAtomicSyncTask(crossSchema);
            taskList = Lists.newArrayList(
                validateTask,
                validateFileStoreTask,
                tableSyncTaskBeforeHiding,
                hideMetaTask,
                tableSyncTaskAfterHiding,
                phyDdlTask,
                phyDdlFileStoreTask,
                cdcDdlMarkTask,
                updateMetaTask,
                tableSyncTaskAfterShowing,
                removeColumnMappingTask
            ).stream().filter(Objects::nonNull).collect(Collectors.toList());
            executableDdlJob.labelAsTail(removeColumnMappingTask);
        } else {
            // 1. physical DDL
            // 2. physical DDL on archive table
            // 3. update meta
            // 4. sync table
            String originDdl = executionContext.getDdlContext().getDdlStmt();
            if (AlterTableRollbacker.checkIfRollbackable(originDdl)) {
                phyDdlTask = phyDdlTask.onExceptionTryRecoveryThenRollback();
            }
            taskList = Lists.newArrayList(
                validateTask,
                validateFileStoreTask,
                beginAlterColumnDefault,
                beginAlterColumnDefaultSyncTask,
                phyDdlTask,
                phyDdlFileStoreTask,
                cdcDdlMarkTask,
                updateMetaTask,
                tableSyncTaskAfterShowing
            ).stream().filter(Objects::nonNull).collect(Collectors.toList());
            executableDdlJob.labelAsTail(tableSyncTaskAfterShowing);
        }

        executableDdlJob.labelAsHead(validateTask);

        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    private DdlTask buildAtomicSyncTask(boolean crossSchema) {
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
        return crossSchema ?
            new AtomicTablesSyncTask(
                Lists.newArrayList(schemaName, fileStoreSchema),
                Lists.newArrayList(Lists.newArrayList(logicalTableName), Lists.newArrayList(fileStoreTable)),
                initWait, interval, TimeUnit.MILLISECONDS
            ) :
            new AtomicTablesSyncTask(
                Lists.newArrayList(schemaName),
                compressTables(),
                initWait, interval, TimeUnit.MILLISECONDS
            );

    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));

        String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, logicalTableName);
        if (tgName != null) {
            resources.add(concatWithDot(schemaName, tgName));
        }
        resources.add(concatWithDot(fileStoreSchema, fileStoreTable));

        String fileStoreTgName = FactoryUtils.getTableGroupNameByTableName(fileStoreSchema, fileStoreTable);
        if (tgName != null) {
            resources.add(concatWithDot(fileStoreSchema, fileStoreTgName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        // add forbid drop read lock if the 'expire' ddl is cross schema
        if (!StringUtils.equalsIgnoreCase(schemaName, fileStoreSchema)) {
            resources.add(LockUtil.genForbidDropResourceName(fileStoreSchema));
        }
    }

    public void validateExistence(boolean validateExistence) {
        this.validateExistence = validateExistence;
    }

    private List<List<String>> compressTables() {
        List<List<String>> compressedTables = Lists.newArrayList();
        compressedTables.add(Lists.newArrayList(logicalTableName, fileStoreTable));
        return compressedTables;
    }
}
