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

import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.BindingArchiveTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateOssTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateOssTableFormatTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateOssTableGenerateDataTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileCommitTsTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesPartitionInfoMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateOssPartitionTable;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.utils.RelUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager.ID_GENERATOR;

public class CreatePartitionOssTableJobFactory extends CreateTableJobFactory {
    public static final String CREATE_TABLE_ADD_TABLES_META_TASK = "CREATE_TABLE_ADD_TABLES_META_TASK";

    private CreateTablePreparedData preparedData;
    private Engine tableEngine;
    private ArchiveMode archiveMode;

    public CreatePartitionOssTableJobFactory(boolean autoPartition, boolean hasTimestampColumnDefault,
                                             Map<String, String> binaryColumnDefaultValues,
                                             PhysicalPlanData physicalPlanData, ExecutionContext executionContext,
                                             CreateTablePreparedData preparedData, Engine tableEngine,
                                             ArchiveMode archiveMode) {
        super(autoPartition, hasTimestampColumnDefault, binaryColumnDefaultValues, physicalPlanData, executionContext);
        this.preparedData = preparedData;
        this.tableEngine = tableEngine;
        this.archiveMode = archiveMode;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        super.excludeResources(resources);

        TableGroupConfig tgConfig = physicalPlanData.getTableGroupConfig();
        TableGroupRecord record = tgConfig.getTableGroupRecord();
        if (record != null) {
            String tgName = record.getTg_name();
            resources.add(concatWithDot(schemaName, tgName));
        }
        if (preparedData != null && preparedData.getTableGroupName() != null) {
            String tgName = RelUtils.stringValue(preparedData.getTableGroupName());
            if (TStringUtil.isNotBlank(tgName)) {
                resources.add(concatWithDot(schemaName, tgName));
            }
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = physicalPlanData.getSchemaName();

        List<DdlTask> taskList = new ArrayList<>();

        // table info validator
        CreatePartitionTableValidateTask validateTask =
            new CreatePartitionTableValidateTask(schemaName, logicalTableName,
                physicalPlanData.isIfNotExists(), physicalPlanData.getTableGroupConfig(), new ArrayList<>(), false,
                false);
        taskList.add(validateTask);

        // table partition info
        CreateTableAddTablesPartitionInfoMetaTask addPartitionInfoTask =
            new CreateTableAddTablesPartitionInfoMetaTask(schemaName, logicalTableName, physicalPlanData.isTemporary(),
                physicalPlanData.getTableGroupConfig(), null, false, null);
        taskList.add(addPartitionInfoTask);

        // mysql physical ddl task
        CreateTablePhyDdlTask phyDdlTask = new CreateTablePhyDdlTask(schemaName, logicalTableName, physicalPlanData);
        taskList.add(phyDdlTask);

        // oss table metas
        CreateOssTableAddTablesMetaTask createOssTableAddTablesMetaTask =
            new CreateOssTableAddTablesMetaTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(), physicalPlanData.getSequence(),
                physicalPlanData.getTablesExtRecord(), physicalPlanData.isPartitioned(),
                physicalPlanData.isIfNotExists(), physicalPlanData.getKind(), this.tableEngine);
        taskList.add(createOssTableAddTablesMetaTask);

        // oss table format
        CreateOssTableFormatTask createOssTableFormatTask =
            new CreateOssTableFormatTask(schemaName, logicalTableName, physicalPlanData, this.tableEngine);
        taskList.add(createOssTableFormatTask);

        // oss data loading
        CreateOssTableGenerateDataTask createOssTableGenerateDataTask
            = new CreateOssTableGenerateDataTask(schemaName, logicalTableName, physicalPlanData,
            preparedData.getLoadTableSchema(), preparedData.getLoadTableName(), tableEngine, archiveMode);
        taskList.add(createOssTableGenerateDataTask);

        // binding archive table to source table
        if (archiveMode == ArchiveMode.TTL
            && preparedData.getLoadTableSchema() != null
            && preparedData.getLoadTableName() != null) {
            BindingArchiveTableMetaTask bindingArchiveTableMetaTask = new BindingArchiveTableMetaTask(
                schemaName, logicalTableName,
                preparedData.getLoadTableSchema(), preparedData.getLoadTableName(), // load table as source table
                schemaName, logicalTableName, // target table as archive table
                archiveMode
            );
            taskList.add(bindingArchiveTableMetaTask);
        }

        // handle task id
        createOssTableFormatTask.setTaskId(ID_GENERATOR.nextId());
        createOssTableGenerateDataTask.setTaskId(ID_GENERATOR.nextId());
        List<Long> taskIdList = new ArrayList<>();
        taskIdList.add(createOssTableFormatTask.getTaskId());
        taskIdList.add(createOssTableGenerateDataTask.getTaskId());

        // update file timestamp
        UpdateFileCommitTsTask updateFileCommitTsTask =
            new UpdateFileCommitTsTask(tableEngine.name(), schemaName, logicalTableName, taskIdList);
        taskList.add(updateFileCommitTsTask);

        // show table meta
        CreateTableShowTableMetaTask showTableMetaTask = new CreateTableShowTableMetaTask(schemaName, logicalTableName);
        taskList.add(showTableMetaTask);

        // sync source table
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        taskList.add(tableSyncTask);

        // sync ttl table
        if (archiveMode == ArchiveMode.TTL
            && preparedData.getLoadTableSchema() != null
            && preparedData.getLoadTableName() != null) {
            CreateTableShowTableMetaTask showLoadTableMetaTask =
                new CreateTableShowTableMetaTask(preparedData.getLoadTableSchema(), preparedData.getLoadTableName());
            taskList.add(showLoadTableMetaTask);

            TableSyncTask loadTableSyncTask = new TableSyncTask(preparedData.getLoadTableSchema(), preparedData.getLoadTableName());
            taskList.add(loadTableSyncTask);
        }

        ExecutableDdlJob4CreateOssPartitionTable result = new ExecutableDdlJob4CreateOssPartitionTable();
        result.addSequentialTasks(taskList);

        result.setCreatePartitionTableValidateTask(validateTask);
        result.setCreateTableAddTablesPartitionInfoMetaTask(addPartitionInfoTask);
        result.setCreateTablePhyDdlTask(phyDdlTask);
        result.setCreateOssTableAddTablesMetaTask(createOssTableAddTablesMetaTask);
        result.setCreateOssTableFormatTask(createOssTableFormatTask);
        result.setCreateOssTableGenerateDataTask(createOssTableGenerateDataTask);
        result.setCreateTableShowTableMetaTask(showTableMetaTask);
        result.setTableSyncTask(tableSyncTask);

        return result;
    }
}
