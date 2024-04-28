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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillExecutor;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillTimer;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillWriterTask;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.FileStorageBackFillAccessor;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.FileStorageAccessorDelegate;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "ArchiveOSSTableDataTask")
public class ArchiveOSSTableDataTask extends BaseGmsTask {
    protected final String loadTableSchema;
    protected final String loadTableName;
    protected final String physicalPartitionName;

    protected final Engine targetTableEngine;

    @JSONCreator
    public ArchiveOSSTableDataTask(String schemaName, String logicalTableName,
                                   String loadTableSchema, String loadTableName,
                                   String physicalPartitionName, Engine targetTableEngine) {
        super(schemaName, logicalTableName);
        this.loadTableSchema = loadTableSchema;
        this.loadTableName = loadTableName;
        this.physicalPartitionName = physicalPartitionName;
        this.targetTableEngine = targetTableEngine;
        onExceptionTryRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateSupportedCommands(true, true, null);
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        executionContext.setBackfillId(getTaskId());
        new FileStorageAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                // don't continue the ddl if it was paused
                List<FilesRecord> files = filesAccessor.queryByIdAndSchemaAndTable(getTaskId(), schemaName, logicalTableName);
                if (files != null && files.size() > 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CANT_CONTINUE_DDL);
                }
                return 0;
            }
        }.execute();

        loadTable(executionContext);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackFileStorage(metaDbConnection, executionContext);
    }

    protected void rollbackFileStorage(Connection metaDbConnection, ExecutionContext executionContext) {

        new FileStorageAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<FilesRecord> files = filesAccessor.queryByIdAndSchemaAndTable(getTaskId(), schemaName, logicalTableName);
                List<ColumnMetasRecord> columnMetas = columnMetaAccessor.queryByIdAndSchemaAndTable(getTaskId(), schemaName, logicalTableName);
                deleteUncommitted(files, columnMetas);
                filesAccessor.delete(getTaskId(), schemaName, logicalTableName);
                columnMetaAccessor.delete(getTaskId(), schemaName, logicalTableName);
                return 0;
            }
        }.execute();

        // clear back-fill states
        // if checkpoint resume is supported, this code block should be removed.
        GsiBackfillManager manager = new GsiBackfillManager(schemaName);
        manager.deleteByBackfillId(getTaskId());
    }

    protected void deleteUncommitted(List<FilesRecord> files, List<ColumnMetasRecord> columnMetas) {
        // delete remote oss files and local tmp files
        for (FilesRecord record : files) {
            FileSystemUtils.deleteIfExistsFile(record.getFileName(), this.targetTableEngine, false);
            File tmpFile = new File(record.getLocalPath());
            if (tmpFile.exists()) {
                if (!tmpFile.delete()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_BACK_FILL_FAIL,
                        "can't delete file " + record.getLocalPath());
                }
            }
        }
        CommonMetaChanger.clearOSSFileSystemCache(
            files.stream().map(FilesRecord::getLocalPath).collect(Collectors.toList()), schemaName);

        // delete column meta and bf files
        for (ColumnMetasRecord record : columnMetas) {
            FileSystemUtils.deleteIfExistsFile(record.tableFileName, this.targetTableEngine, false);
        }
    }

    private void loadTable(ExecutionContext executionContext) {
        loadTable(executionContext, false);
    }

    protected void loadTable(ExecutionContext executionContext, boolean supportPause) {
        Map<Pair<String, String>, OSSBackFillWriterTask> tasks = null;
        try {
            String sourceLogicalSchema = this.loadTableSchema;
            String sourceLogicalTable = this.loadTableName;
            String targetLogicalSchema = this.schemaName;
            String targetLogicalTable = this.logicalTableName;

            ExecutionContext sourceDbContext = executionContext.copy();
            sourceDbContext.setSchemaName(sourceLogicalSchema);
            sourceDbContext.setBackfillId(getTaskId());

            TableMeta sourceTableMeta =
                executionContext.getSchemaManager(sourceLogicalSchema).getTable(sourceLogicalTable);
            if (!sourceTableMeta.isHasPrimaryKey()) {
                throw new AssertionError("Table must have primary key");
            }

            TableMeta targetTableMeta =
                executionContext.getSchemaManager(targetLogicalSchema).getTable(targetLogicalTable);
            // build orc schema
            PolarDBXOrcSchema orcSchema = OrcMetaUtils.buildPolarDBXOrcSchema(targetTableMeta);

            // data config
            Configuration conf = OrcMetaUtils.getConfiguration(executionContext, orcSchema);

            tasks = buildOssBackFillLoaderTasks(
                executionContext,
                sourceLogicalSchema,
                sourceLogicalTable,
                targetLogicalSchema,
                targetLogicalTable,
                sourceTableMeta,
                targetTableMeta,
                orcSchema,
                conf,
                supportPause);
            if (GeneralUtil.isEmpty(tasks)) {
                return;
            }

            Map<String, Set<String>> sourcePhyTables = OSSTaskUtils.genSourcePhyTables(tasks);
            final int parallelism =
                executionContext.getParamManager().getInt(ConnectionParams.OSS_BACKFILL_PARALLELISM);
            final long indexStride =
                executionContext.getParamManager().getLong(ConnectionParams.OSS_ORC_INDEX_STRIDE);

            // do backfill: select source table -> fill target orc file
            OSSBackFillExecutor backFillExecutor = new OSSBackFillExecutor(Engine.INNODB, this.targetTableEngine);
            backFillExecutor
                .backFill2FileStore(sourceLogicalSchema, sourceLogicalTable, targetLogicalTable, sourceDbContext, sourcePhyTables,
                    (int) indexStride,
                    parallelism, tasks, physicalPartitionName, supportPause);

            // flush all
            tasks.forEach((pair, task) -> task.flush(sourceDbContext));

            // wait all async task done.
            tasks.forEach((pair, task) -> task.waitAsync());
        } catch (Exception e) {
            // cancel all tasks
            if (tasks != null) {
                tasks.forEach((pair, task) -> task.cancelAsync());
            }
            // pause ddl if support
            if (supportPause) {
                OSSBackFillTimer.pauseDDL(executionContext);
            }
            throw GeneralUtil.nestedException(e);
        }
    }

    @NotNull
    protected Map<Pair<String, String>, OSSBackFillWriterTask> buildOssBackFillLoaderTasks(
        ExecutionContext executionContext,
        String sourceLogicalSchema,
        String sourceLogicalTable,
        String targetLogicalSchema,
        String targetLogicalTable,
        TableMeta sourceTableMeta,
        TableMeta targetTableMeta,
        PolarDBXOrcSchema orcSchema,
        Configuration conf,
        Boolean supportPause) {
        final long maxRowsPerFile = executionContext.getParamManager().getLong(ConnectionParams.OSS_MAX_ROWS_PER_FILE);
        final boolean removeTmpFiles =
            executionContext.getParamManager().getBoolean(ConnectionParams.OSS_REMOVE_TMP_FILES);

        Map<Pair<String, String>, OSSBackFillWriterTask> tasks = new HashMap<>();

        // handle single table.
        Pair<String, String> singleTopology = OSSTaskUtils.getSingleTopology(sourceLogicalSchema, sourceLogicalTable, sourceTableMeta);

        PartitionInfo sourceTablePartitionInfo =
            OSSTaskUtils.getSourcePartitionInfo(executionContext, sourceLogicalSchema, sourceLogicalTable);

        // traverse each physical partition (phy table)
        for (PhysicalPartitionInfo physicalPartitionInfo :
            getFlattenedPartitionInfo(targetLogicalSchema, targetLogicalTable)) {

            String targetPhySchema = physicalPartitionInfo.getGroupKey();
            String targetPhyTable = physicalPartitionInfo.getPhyTable();

            String partName = physicalPartitionInfo.getPartName();
            Pair<String, String> sourcePhySchemaAndTable = Optional
                .ofNullable(singleTopology)
                .orElseGet(() -> OSSTaskUtils.getSourcePhyTable(sourceTablePartitionInfo, partName));
            String sourcePhySchema = sourcePhySchemaAndTable.getKey();
            String sourcePhyTable = sourcePhySchemaAndTable.getValue();

            // for each physical table, add orc write task.
            OSSBackFillWriterTask task = new OSSBackFillWriterTask(
                // for target table
                targetLogicalSchema,
                targetLogicalTable,
                targetPhySchema,
                targetPhyTable,

                // for source table
                sourcePhySchema,
                sourcePhyTable,
                sourceTableMeta,
                targetTableEngine,
                getTaskId(),

                // for orc file
                conf,
                physicalPartitionName,
                orcSchema,
                targetTableMeta,
                maxRowsPerFile,
                removeTmpFiles,
                executionContext,
                supportPause
            );
            tasks.put(sourcePhySchemaAndTable, task);

        }
        return tasks;
    }

    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table) {
        return OSSTaskUtils.getFlattenedPartitionInfo(schema, table);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        updateSupportedCommands(true, false, null);
    }
}
