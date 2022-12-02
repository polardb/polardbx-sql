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

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterColumnDefaultTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableHideMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableInsertColumnsMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4AlterTable;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterTableJobFactory extends DdlJobFactory {

    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;
    private final AlterTablePreparedData prepareData;
    private final LogicalAlterTable logicalAlterTable;

    /**
     * Whether altering a gsi table.
     */
    private boolean alterGsiTable = false;
    private String primaryTableName;

    /**
     * Whether altering a gsi table for repartition
     */
    private boolean repartition = false;

    /**
     * Whether generate validate table task
     */
    private boolean validateExistence = true;

    private ExecutionContext executionContext;

    public AlterTableJobFactory(PhysicalPlanData physicalPlanData,
                                AlterTablePreparedData preparedData,
                                LogicalAlterTable logicalAlterTable,
                                ExecutionContext executionContext) {
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.physicalPlanData = physicalPlanData;
        this.prepareData = preparedData;
        this.logicalAlterTable = logicalAlterTable;
        this.executionContext = executionContext;
    }

    public void withAlterGsi(boolean alterGsi, String primaryTableName) {
        this.alterGsiTable = alterGsi;
        this.primaryTableName = primaryTableName;
    }

    public void withAlterGsi4Repartition(boolean alterGsi, boolean repartition, String primaryTableName) {
        withAlterGsi(alterGsi, primaryTableName);
        this.repartition = repartition;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        TableGroupConfig tableGroupConfig = isNewPart ? physicalPlanData.getTableGroupConfig() : null;
        DdlTask validateTask =
            this.validateExistence ?
                new AlterTableValidateTask(schemaName, logicalTableName,
                    logicalAlterTable.getSqlAlterTable().getSourceSql(), prepareData.getTableVersion(),
                    tableGroupConfig) :
                new EmptyTask(schemaName);

        final boolean isDropColumnOrDropIndex =
            CollectionUtils.isNotEmpty(prepareData.getDroppedColumns())
                || CollectionUtils.isNotEmpty(prepareData.getDroppedIndexes());

        List<DdlTask> alterGsiMetaTasks = new ArrayList<>();
        if (this.alterGsiTable) {
            // TODO(moyi) simplify these tasks, which could be executed batched
            if (CollectionUtils.isNotEmpty(prepareData.getDroppedColumns())) {
                alterGsiMetaTasks.addAll(GsiTaskFactory.alterGlobalIndexDropColumnTasks(
                    schemaName,
                    primaryTableName,
                    logicalTableName,
                    prepareData.getDroppedColumns()));
            }

            if (CollectionUtils.isNotEmpty(prepareData.getAddedColumns())) {
                alterGsiMetaTasks.addAll(GsiTaskFactory.alterGlobalIndexAddColumnsStatusTasks(
                    schemaName,
                    primaryTableName,
                    logicalTableName,
                    prepareData.getAddedColumns(),
                    prepareData.getBackfillColumns()));
            }
        }

        DdlTask beginAlterColumnDefault = null;
        DdlTask endAlterColumnDefault = null;
        if (!this.alterGsiTable && CollectionUtils.isNotEmpty(prepareData.getAlterDefaultColumns())) {
            beginAlterColumnDefault =
                new AlterColumnDefaultTask(schemaName, logicalTableName, prepareData.getAlterDefaultColumns(), true);
            endAlterColumnDefault =
                new AlterColumnDefaultTask(schemaName, logicalTableName, prepareData.getAlterDefaultColumns(), false);
        }

        DdlTask phyDdlTask = new AlterTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);
        if (this.repartition) {
            ((AlterTablePhyDdlTask) phyDdlTask).setSourceSql(logicalAlterTable.getNativeSql());
        }

        physicalPlanData.setAlterTablePreparedData(prepareData);
        DdlTask cdcDdlMarkTask = this.alterGsiTable || this.prepareData.isOnlineModifyColumnIndexTask() ? null :
            new CdcDdlMarkTask(schemaName, physicalPlanData);

        DdlTask updateMetaTask = null;
        if (!this.repartition) {
            updateMetaTask = new AlterTableChangeMetaTask(
                schemaName,
                logicalTableName,
                physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(),
                physicalPlanData.getKind(),
                physicalPlanData.isPartitioned(),
                prepareData.getDroppedColumns(),
                prepareData.getAddedColumns(),
                prepareData.getUpdatedColumns(),
                prepareData.getChangedColumns(),
                prepareData.isTimestampColumnDefault(),
                prepareData.getBinaryColumnDefaultValues(),
                prepareData.getDroppedIndexes(),
                prepareData.getAddedIndexes(),
                prepareData.getAddedIndexesWithoutNames(),
                prepareData.getRenamedIndexes(),
                prepareData.isPrimaryKeyDropped(),
                prepareData.getAddedPrimaryKeyColumns(),
                prepareData.getColumnAfterAnother(),
                prepareData.isLogicalColumnOrder(),
                prepareData.getTableComment(),
                prepareData.getTableRowFormat(),
                physicalPlanData.getSequence(),
                prepareData.isOnlineModifyColumnIndexTask()
            );
        } else {
            // only add columns
            updateMetaTask = new AlterTableInsertColumnsMetaTask(
                schemaName,
                logicalTableName,
                physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(),
                prepareData.getAddedColumns()
            );
        }

        DdlTask tableSyncTaskAfterShowing = new TableSyncTask(schemaName, logicalTableName);

        ExecutableDdlJob4AlterTable executableDdlJob = new ExecutableDdlJob4AlterTable();

        List<DdlTask> taskList = null;
        if (isDropColumnOrDropIndex) {
            DdlTask hideMetaTask =
                new AlterTableHideMetaTask(schemaName, logicalTableName,
                    prepareData.getDroppedColumns(),
                    prepareData.getDroppedIndexes());
            DdlTask tableSyncTaskAfterHiding = new TableSyncTask(schemaName, logicalTableName);
            taskList = Lists.newArrayList(
                validateTask,
                hideMetaTask,
                tableSyncTaskAfterHiding,
                phyDdlTask,
                cdcDdlMarkTask,
                updateMetaTask
            ).stream().filter(Objects::nonNull).collect(Collectors.toList());
        } else {
            // 1. physical DDL
            // 2. alter GSI meta if necessary
            // 3. update meta
            // 4. sync table
            String originDdl = executionContext.getDdlContext().getDdlStmt();
            if (AlterTableRollbacker.checkIfRollbackable(originDdl)) {
                phyDdlTask = phyDdlTask.onExceptionTryRecoveryThenRollback();
            }
            taskList = Lists.newArrayList(
                validateTask,
                beginAlterColumnDefault,
                phyDdlTask,
                cdcDdlMarkTask,
                updateMetaTask,
                endAlterColumnDefault
            ).stream().filter(Objects::nonNull).collect(Collectors.toList());
        }

        taskList.addAll(alterGsiMetaTasks);
        taskList.add(tableSyncTaskAfterShowing);

        executableDdlJob.addSequentialTasks(taskList);

        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(tableSyncTaskAfterShowing);

        executableDdlJob.setTableValidateTask((BaseValidateTask) validateTask);
        executableDdlJob.setTableSyncTask((TableSyncTask) tableSyncTaskAfterShowing);

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));

        String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, logicalTableName);
        if (tgName != null) {
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    public void validateExistence(boolean validateExistence) {
        this.validateExistence = validateExistence;
    }
}
