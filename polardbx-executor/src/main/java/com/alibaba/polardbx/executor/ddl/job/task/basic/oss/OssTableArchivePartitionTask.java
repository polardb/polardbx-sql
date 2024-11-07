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
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.FileStorageAccessorDelegate;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
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

/**
 * @author wumu
 */
@Getter
@TaskName(name = "OssTableArchivePartitionTask")
public class OssTableArchivePartitionTask extends BaseBackfillTask {
    protected final String ossTableName;
    protected final String tmpTableSchema;
    protected final String tmpTableName;
    protected final List<String> partitionNames;

    protected final Engine targetTableEngine;

    @JSONCreator
    public OssTableArchivePartitionTask(String ossSchemaName, String ossTableName,
                                        String tmpTableSchema, String tmpTableName,
                                        List<String> partitionNames, Engine targetTableEngine) {
        super(ossSchemaName);
        this.ossTableName = ossTableName;
        this.tmpTableSchema = tmpTableSchema;
        this.tmpTableName = tmpTableName;
        this.partitionNames = partitionNames;
        this.targetTableEngine = targetTableEngine;
        onExceptionTryRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateSupportedCommands(true, true, null);
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        executeImpl(executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        updateSupportedCommands(true, true, null);
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        executionContext.setBackfillId(getTaskId());

        new FileStorageAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                // rollback unfinished files
                List<FilesRecord> files = filesAccessor.queryUncommitted(getTaskId(), schemaName, ossTableName);
                List<ColumnMetasRecord> columnMetas =
                    columnMetaAccessor.queryUncommitted(getTaskId(), schemaName, ossTableName);
                deleteUncommitted(files, columnMetas);
                filesAccessor.deleteUncommitted(getTaskId(), schemaName, ossTableName);
                columnMetaAccessor.deleteUncommitted(getTaskId(), schemaName, ossTableName);
                return 0;
            }
        }.execute();

        // do real backfill
        loadTable(executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        new FileStorageAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                List<FilesRecord> files =
                    filesAccessor.queryByIdAndSchemaAndTable(getTaskId(), schemaName, ossTableName);
                List<ColumnMetasRecord> columnMetas =
                    columnMetaAccessor.queryByIdAndSchemaAndTable(getTaskId(), schemaName, ossTableName);
                deleteUncommitted(files, columnMetas);
                filesAccessor.delete(getTaskId(), schemaName, ossTableName);
                columnMetaAccessor.delete(getTaskId(), schemaName, ossTableName);
                return 0;
            }
        }.execute();

        // clear back-fill states
        // if checkpoint resume is supported, this code block should be removed.
        GsiBackfillManager manager = new GsiBackfillManager(schemaName);
        manager.deleteByBackfillId(getTaskId());

        // delete file_storage_backfill_object
        FileStorageBackFillAccessor fileStorageBackFillAccessor = new FileStorageBackFillAccessor();
        fileStorageBackFillAccessor.setConnection(metaDbConnection);
        fileStorageBackFillAccessor.deleteFileBackfillMeta(getTaskId());
    }

    protected void loadTable(ExecutionContext executionContext) {
        Map<Pair<String, String>, OSSBackFillWriterTask> tasks = null;
        try {
            String sourceLogicalSchema = this.tmpTableSchema;
            String sourceLogicalTable = this.tmpTableName;
            String targetLogicalSchema = this.schemaName;
            String targetLogicalTable = this.ossTableName;

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
                conf);
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
                .backFill2FileStore(sourceLogicalSchema, sourceLogicalTable, targetLogicalTable, sourceDbContext,
                    sourcePhyTables,
                    (int) indexStride,
                    parallelism, tasks, null, true);

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
            OSSBackFillTimer.pauseDDL(executionContext);
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
        Configuration conf) {
        final long maxRowsPerFile = executionContext.getParamManager().getLong(ConnectionParams.OSS_MAX_ROWS_PER_FILE);
        final boolean removeTmpFiles =
            executionContext.getParamManager().getBoolean(ConnectionParams.OSS_REMOVE_TMP_FILES);

        Map<Pair<String, String>, OSSBackFillWriterTask> tasks = new HashMap<>();

        // handle single table.
        Pair<String, String> singleTopology =
            OSSTaskUtils.getSingleTopology(sourceLogicalSchema, sourceLogicalTable, sourceTableMeta);

        PartitionInfo sourceTablePartitionInfo =
            OSSTaskUtils.getSourcePartitionInfo(executionContext, sourceLogicalSchema, sourceLogicalTable);

        // traverse each physical partition (phy table)
        for (PhysicalPartitionInfo physicalPartitionInfo :
            getFlattenedPartitionInfo(targetLogicalSchema, targetLogicalTable, partitionNames)) {

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
                null,
                orcSchema,
                targetTableMeta,
                maxRowsPerFile,
                removeTmpFiles,
                executionContext,
                true
            );
            tasks.put(sourcePhySchemaAndTable, task);
        }
        return tasks;
    }

    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table,
                                                                    List<String> partitionNames) {
        return OSSTaskUtils.getFlattenedPartitionInfo(schema, table, partitionNames);
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

    @Override
    protected String remark() {
        return String.format("|%s.%s.%s", schemaName, ossTableName, partitionNames);
    }
}
