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
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateArchiveTableEventLogTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateEntitySecurityAttrTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesPartitionInfoMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.BindingArchiveTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateOssTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateOssTableFormatTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateOssTableGenerateDataMppTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CreateOssTableGenerateDataTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileCommitTsTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.engine.ColdDataStatus;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.eclipse.jetty.util.StringUtil;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager.ID_GENERATOR;

public class CreatePartitionOssTableJobFactory extends CreateTableJobFactory {
    private final CreateTablePreparedData preparedData;
    private final Engine tableEngine;
    private final ArchiveMode archiveMode;
    private final List<String> dictColumns;

    public CreatePartitionOssTableJobFactory(boolean autoPartition, boolean hasTimestampColumnDefault,
                                             Map<String, String> specialDefaultValues,
                                             Map<String, Long> specialDefaultValueFlags,
                                             PhysicalPlanData physicalPlanData, ExecutionContext executionContext,
                                             CreateTablePreparedData preparedData, Engine tableEngine,
                                             ArchiveMode archiveMode, List<String> dictColumns) {
        super(autoPartition, hasTimestampColumnDefault, specialDefaultValues, specialDefaultValueFlags, null,
            physicalPlanData, preparedData.getDdlVersionId(), executionContext, null);
        this.preparedData = preparedData;
        this.tableEngine = tableEngine;
        this.archiveMode = archiveMode;
        this.dictColumns = dictColumns;
    }

    @Override
    protected void validate() {
        if (FileSystemUtils.getColdDataStatus().getStatus() == ColdDataStatus.OFF.getStatus()) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_ARCHIVE_NOT_ENABLED,
                "Data archiving feature is not enabled. Please enable this feature on the console."
            );
        }
        if (archiveMode == ArchiveMode.TTL
            && preparedData.getLoadTableSchema() != null
            && preparedData.getLoadTableName() != null) {
            try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                String ttlTableSchema = preparedData.getLoadTableSchema();
                String ttlTableName = preparedData.getLoadTableName();
                TableInfoManager tableInfoManager = executionContext.getTableInfoManager();
                tableInfoManager.setConnection(conn);
                TableLocalPartitionRecord localPartRecord =
                    tableInfoManager.getLocalPartitionRecord(ttlTableSchema, ttlTableName);
                TtlInfoRecord ttlInfoRecord = tableInfoManager.getTtlInfoRecord(ttlTableSchema, ttlTableName);

                String oldArchiveTableSchema = null;
                String oldArchiveTableName = null;
                if (ttlInfoRecord == null) {
                    // not a local partition table
                    if (localPartRecord == null) {
                        throw GeneralUtil.nestedException(
                            MessageFormat.format("{0}.{1} is not a local partition table.",
                                ttlTableSchema, ttlTableName));
                    }
                    oldArchiveTableSchema = localPartRecord.getArchiveTableSchema();
                    oldArchiveTableName = localPartRecord.getArchiveTableName();
                } else {
                    /**
                     * To validate
                     */
                    oldArchiveTableSchema = ttlInfoRecord.getArcTblSchema();
                    oldArchiveTableName = ttlInfoRecord.getArcTblName();
                }

                // already has archive table but don't allow replace it.

                if (oldArchiveTableSchema != null || oldArchiveTableName != null) {
                    boolean allowReplace =
                        executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_REPLACE_ARCHIVE_TABLE);

                    if (!allowReplace) {
                        throw new TddlRuntimeException(
                            ErrorCode.ERR_ARCHIVE_TABLE_EXISTS,
                            MessageFormat.format(
                                "The table {0}.{1} already has archive table {2}.{3}, please use connection param: ALLOW_REPLACE_ARCHIVE_TABLE=true to allow replace archive table.",
                                ttlTableSchema, ttlTableName, oldArchiveTableSchema, oldArchiveTableName));
                    }
                }

            } catch (Throwable t) {
                throw new TddlNestableRuntimeException(t);
            }
        }
        if (selectSql != null) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_CREATE_SELECT_WITH_OSS, "Create table select for archive table is not supported."
            );
        }
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
        if (preparedData != null && !StringUtil.isEmpty(preparedData.getLoadTableSchema()) &&
            !StringUtil.isEmpty(preparedData.getLoadTableName())) {
            resources.add(concatWithDot(preparedData.getLoadTableSchema(), preparedData.getLoadTableName()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        // lock load schema if the ddl is cross-schema
        if (preparedData != null) {
            if (!StringUtils.isEmpty(preparedData.getLoadTableSchema())) {
                if (!preparedData.getLoadTableName().equalsIgnoreCase(schemaName)) {
                    resources.add(LockUtil.genForbidDropResourceName(preparedData.getLoadTableName()));
                }
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
                physicalPlanData.isIfNotExists(), physicalPlanData.getTableGroupConfig(), null, new ArrayList<>(), null,false,
                false);
        taskList.add(validateTask);

        boolean autoCreateTg =
            executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_AUTO_CREATE_TABLEGROUP);
        // table partition info
        CreateTableAddTablesPartitionInfoMetaTask addPartitionInfoTask =
            new CreateTableAddTablesPartitionInfoMetaTask(schemaName, logicalTableName, physicalPlanData.isTemporary(),
                physicalPlanData.getTableGroupConfig(), null, null,
                null, false, true, null, null, false, autoCreateTg);
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

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        List<Long> taskIdList = new ArrayList<>();

        // handle task id
        createOssTableFormatTask.setTaskId(ID_GENERATOR.nextId());
        taskIdList.add(createOssTableFormatTask.getTaskId());

        DdlTask tailTask;
        // oss data loading
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MPP_FILE_STORE_BACKFILL)
            && !StringUtils.isEmpty(preparedData.getLoadTableName())) {
            EmptyTask emptyTask = new EmptyTask(schemaName);
            tailTask = emptyTask;
            int totalNum = OSSTaskUtils.getMppParallelism(executionContext,
                executionContext.getSchemaManager(preparedData.getLoadTableSchema())
                    .getTable(preparedData.getLoadTableName()));
            for (int serialNum = 0; serialNum < totalNum; serialNum++) {
                CreateOssTableGenerateDataMppTask createOssTableGenerateDataMppTask
                    = new CreateOssTableGenerateDataMppTask(schemaName, logicalTableName, physicalPlanData,
                    preparedData.getLoadTableSchema(), preparedData.getLoadTableName(), tableEngine, archiveMode,
                    dictColumns, totalNum, serialNum);

                createOssTableGenerateDataMppTask.setTaskId(ID_GENERATOR.nextId());
                taskIdList.add(createOssTableGenerateDataMppTask.getTaskId());

                executableDdlJob.addTask(createOssTableGenerateDataMppTask);
                executableDdlJob.addTaskRelationship(createOssTableFormatTask, createOssTableGenerateDataMppTask);
                executableDdlJob.addTaskRelationship(createOssTableGenerateDataMppTask, emptyTask);
            }
            executableDdlJob.setMaxParallelism(OSSTaskUtils.getArchiveParallelism(executionContext));
        } else {
            CreateOssTableGenerateDataTask createOssTableGenerateDataTask
                = new CreateOssTableGenerateDataTask(schemaName, logicalTableName, physicalPlanData,
                preparedData.getLoadTableSchema(), preparedData.getLoadTableName(), tableEngine, archiveMode,
                dictColumns);
            createOssTableGenerateDataTask.setTaskId(ID_GENERATOR.nextId());
            taskIdList.add(createOssTableGenerateDataTask.getTaskId());

            executableDdlJob.addTask(createOssTableGenerateDataTask);
            executableDdlJob.addTaskRelationship(createOssTableFormatTask, createOssTableGenerateDataTask);
            tailTask = createOssTableGenerateDataTask;
        }

        taskList.clear();

        // binding archive table to source table
        if (archiveMode == ArchiveMode.TTL
            && preparedData.getLoadTableSchema() != null
            && preparedData.getLoadTableName() != null) {
            BindingArchiveTableMetaTask bindingArchiveTableMetaTask = new BindingArchiveTableMetaTask(
                schemaName,
                logicalTableName,
                preparedData.getLoadTableSchema(),
                preparedData.getLoadTableName(), // load table as source table
                schemaName,
                logicalTableName,// target table as archive table
                tableEngine.name(),
                archiveMode
            );
            taskList.add(bindingArchiveTableMetaTask);
        }

        // update file timestamp
        UpdateFileCommitTsTask updateFileCommitTsTask =
            new UpdateFileCommitTsTask(tableEngine.name(), schemaName, logicalTableName, taskIdList);
        taskList.add(updateFileCommitTsTask);

        // show table meta
        CreateTableShowTableMetaTask showTableMetaTask = new CreateTableShowTableMetaTask(schemaName, logicalTableName);
        taskList.add(showTableMetaTask);

        // record event log
        CreateArchiveTableEventLogTask createArchiveTableEventLogTask =
            new CreateArchiveTableEventLogTask(schemaName, logicalTableName, preparedData.getLoadTableSchema(),
                preparedData.getLoadTableName(), archiveMode, tableEngine);
        taskList.add(createArchiveTableEventLogTask);

        CreateEntitySecurityAttrTask cesaTask = createCESATask();
        if (cesaTask != null) {
            taskList.add(cesaTask);
        }

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

            TableSyncTask loadTableSyncTask =
                new TableSyncTask(preparedData.getLoadTableSchema(), preparedData.getLoadTableName());
            taskList.add(loadTableSyncTask);
        }

        executableDdlJob.addSequentialTasksAfter(tailTask, taskList);
        if (selectSql != null) {
            throw new TddlNestableRuntimeException(
                "Don't support create table select in oss.");
        }
        return executableDdlJob;
    }
}
