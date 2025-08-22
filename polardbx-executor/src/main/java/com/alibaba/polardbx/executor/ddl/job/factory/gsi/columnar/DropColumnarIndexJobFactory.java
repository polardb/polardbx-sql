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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropColumnarIndexTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CciSchemaEvolutionTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.DropColumnarTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.DropMockColumnarIndexTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropColumnarTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateGsiExistenceTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TtlValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropColumnarIndex;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * 1. drop index xxx on yyy
 * 2. alter table yyy drop index xxx
 * <p>
 * for drop table with [unique] gsi, see class: DropTableWithGlobalIndexJob
 *
 * @author guxu
 */
public class DropColumnarIndexJobFactory extends DdlJobFactory {

    protected final String schemaName;
    protected final String primaryTableName;
    protected final String indexTableName;
    protected final String originalIndexName;
    protected final ExecutionContext executionContext;
    protected final Long versionId;

    public static final String HIDE_TABLE_TASK = "HIDE_TABLE_TASK";

    private boolean skipSchemaChange = false;

    public DropColumnarIndexJobFactory(String schemaName,
                                       String primaryTableName,
                                       String indexTableName,
                                       String originalIndexName,
                                       Long versionId,
                                       boolean skipSchemaChange,
                                       ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.indexTableName = indexTableName;
        this.originalIndexName = originalIndexName;
        this.skipSchemaChange = skipSchemaChange;
        this.versionId = versionId;
        this.executionContext = executionContext;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        resources.add(concatWithDot(schemaName, indexTableName));

        final TableGroupConfig indexTgConfig = getIndexTableGroupConfig();
        if (null != indexTgConfig) {
            resources.add(concatWithDot(schemaName, indexTgConfig.getTableGroupRecord().getTg_name()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

    @Override
    protected void validate() {
        TtlValidator.validateIfDroppingCciOfArcTableOfTtlTable(schemaName, primaryTableName, indexTableName,
            executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        final TableGroupConfig indexTgConfig = getIndexTableGroupConfig();
        final List<Long> tableGroupIds = new ArrayList<>();
        if (null != indexTgConfig) {
            tableGroupIds.add(indexTgConfig.getTableGroupRecord().getId());
        }

        List<DdlTask> taskList = new ArrayList<>();
        // 1. validate
        ValidateGsiExistenceTask validateTask =
            new ValidateGsiExistenceTask(schemaName, primaryTableName, indexTableName, tableGroupIds, indexTgConfig);
        taskList.add(validateTask);

        // 2. GSI status: public -> absent
        if (!skipSchemaChange) {
            List<DdlTask> bringDownTasks =
                GsiTaskFactory.dropColumnarIndexTasks(schemaName, primaryTableName, indexTableName);
            taskList.addAll(bringDownTasks);
        }

        // 3.1 table status: public -> absent
        DropColumnarTableHideTableMetaTask dropColumnarTableHideTableMetaTask =
            new DropColumnarTableHideTableMetaTask(schemaName, primaryTableName, indexTableName);
        taskList.add(dropColumnarTableHideTableMetaTask);

        // 3.2 remove table meta for columnar index
        CciSchemaEvolutionTask cciSchemaEvolutionTask =
            CciSchemaEvolutionTask.dropCci(schemaName, primaryTableName, indexTableName, versionId);
        taskList.add(cciSchemaEvolutionTask);

        // 3.3 drop columnar table
        CdcDropColumnarIndexTask cdcDropColumnarTableTask = null;
        DropMockColumnarIndexTask dropMockColumnarIndexTask = null;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.MOCK_COLUMNAR_INDEX)) {
            dropMockColumnarIndexTask = new DropMockColumnarIndexTask(schemaName, primaryTableName, indexTableName);
            taskList.add(dropMockColumnarIndexTask);
        } else {
            cdcDropColumnarTableTask =
                new CdcDropColumnarIndexTask(schemaName, primaryTableName, originalIndexName, versionId);
            taskList.add(cdcDropColumnarTableTask);
        }

        // 3.4 remove indexes meta for primary table
        GsiDropCleanUpTask gsiDropCleanUpTask = new GsiDropCleanUpTask(schemaName, primaryTableName, indexTableName);
        taskList.add(gsiDropCleanUpTask);
        TableSyncTask tableSyncTaskAfterCleanUpGsiIndexesMeta = new TableSyncTask(schemaName, primaryTableName);
        taskList.add(tableSyncTaskAfterCleanUpGsiIndexesMeta);

        // 4.1 remove table meta for columnar index
        DropColumnarTableRemoveMetaTask dropColumnarTableRemoveMetaTask =
            new DropColumnarTableRemoveMetaTask(schemaName, primaryTableName, indexTableName);
        taskList.add(dropColumnarTableRemoveMetaTask);

        // 4.2 clear table group cache if necessary
        if (null != indexTgConfig) {
            final DdlTask syncTableGroup = new TableGroupSyncTask(
                schemaName,
                indexTgConfig.getTableGroupRecord().getTg_name());
            taskList.add(syncTableGroup);
        }

        // 5.1 sync columnar index table & primary table
        TablesSyncTask finalSyncTask =
            new TablesSyncTask(schemaName, Lists.newArrayList(primaryTableName, indexTableName));
        taskList.add(finalSyncTask);

        final ExecutableDdlJob4DropColumnarIndex executableDdlJob = new ExecutableDdlJob4DropColumnarIndex();
        executableDdlJob.addSequentialTasks(taskList);
        //todo delete me
        // USED IN
        // com.alibaba.polardbx.executor.handler.ddl.LogicalDropIndexHandler.buildDropColumnarIndexJob
        // com.alibaba.polardbx.executor.handler.ddl.LogicalAlterTableHandler.buildDropCciJob
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(finalSyncTask);
        executableDdlJob.labelTask(HIDE_TABLE_TASK, dropColumnarTableHideTableMetaTask);

        executableDdlJob.setValidateTask(validateTask);
        executableDdlJob.setDropColumnarTableHideTableMetaTask(dropColumnarTableHideTableMetaTask);
        executableDdlJob.setGsiDropCleanUpTask(gsiDropCleanUpTask);
        executableDdlJob.setTableSyncTaskAfterCleanUpGsiIndexesMeta(tableSyncTaskAfterCleanUpGsiIndexesMeta);
        executableDdlJob.setCdcDropColumnarIndexTask(cdcDropColumnarTableTask);
        executableDdlJob.setDropMockColumnarIndexTask(dropMockColumnarIndexTask);
        executableDdlJob.setDropColumnarTableRemoveMetaTask(dropColumnarTableRemoveMetaTask);
        executableDdlJob.setCciSchemaEvolutionTask(cciSchemaEvolutionTask);
        executableDdlJob.setFinalSyncTask(finalSyncTask);

        return executableDdlJob;
    }

    @Nullable
    private TableGroupConfig getIndexTableGroupConfig() {
        final OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        final PartitionInfo partitionInfo = oc
            .getPartitionInfoManager()
            .getPartitionInfo(indexTableName);

        final Long tableGroupId = Optional.ofNullable(partitionInfo)
            .map(PartitionInfo::getTableGroupId)
            .orElse(TableGroupRecord.INVALID_TABLE_GROUP_ID);

        TableGroupConfig result = null;
        if (tableGroupId != TableGroupRecord.INVALID_TABLE_GROUP_ID) {
            result = oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
        }

        return result;
    }

    public static ExecutableDdlJob create(DropGlobalIndexPreparedData preparedData,
                                          ExecutionContext executionContext,
                                          boolean skipSchemaChange,
                                          boolean validate) {
        return new DropColumnarIndexJobFactory(
            preparedData.getSchemaName(),
            preparedData.getPrimaryTableName(),
            preparedData.getIndexTableName(),
            preparedData.getOriginalIndexName(),
            preparedData.getDdlVersionId(),
            skipSchemaChange,
            executionContext
        ).create(validate);
    }
}
