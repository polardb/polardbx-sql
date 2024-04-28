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
import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillExecutor;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillWriterTask;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.FileStorageAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.meta.SchemaEvolutionAccessorDelegate;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import lombok.Getter;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "CreateOssTableGenerateDataTask")
public class CreateOssTableGenerateDataTask extends BaseGmsTask {

    protected final PhysicalPlanData physicalPlanData;

    protected final String loadTableSchema;

    protected final String loadTableName;

    protected final Engine tableEngine;

    protected final ArchiveMode archiveMode;

    protected final List<String> dictColumns;

    @JSONCreator
    public CreateOssTableGenerateDataTask(String schemaName, String logicalTableName, PhysicalPlanData physicalPlanData,
                                          String loadTableSchema, String loadTableName, Engine tableEngine,
                                          ArchiveMode archiveMode, List<String> dictColumns) {
        super(schemaName, logicalTableName);
        this.physicalPlanData = physicalPlanData;
        this.loadTableSchema = loadTableSchema;
        this.loadTableName = loadTableName;
        this.tableEngine = tableEngine;
        this.archiveMode = archiveMode;
        this.dictColumns = dictColumns;
        onExceptionTryRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        new FileStorageAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                // don't continue the ddl if it was paused
                List<FilesRecord> files =
                    filesAccessor.queryByIdAndSchemaAndTable(getTaskId(), schemaName, logicalTableName);
                if (files != null && files.size() > 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CANT_CONTINUE_DDL);
                }
                return 0;
            }
        }.execute();

        if (isLoadTable()) {
            loadTable(executionContext);
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        List<FilesRecord> files = TableMetaChanger.lockOssFileMeta(metaDbConnection, getTaskId(), schemaName, logicalTableName);
        for (FilesRecord record : files) {
            FileSystemUtils.deleteIfExistsFile(record.getFileName(), this.tableEngine, false);
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

        //delete table meta
        TableMetaChanger.deleteOssFileMeta(metaDbConnection, getTaskId(), schemaName, logicalTableName);

        List<ColumnMetasRecord> columnMetas =
            TableMetaChanger.lockOssColumnMeta(metaDbConnection, getTaskId(), schemaName, logicalTableName);
        for (ColumnMetasRecord record : columnMetas) {
            FileSystemUtils.deleteIfExistsFile(record.tableFileName, this.tableEngine, false);
        }
        TableMetaChanger.deleteOssColumnMeta(metaDbConnection, getTaskId(), schemaName, logicalTableName);

        // clear back-fill states
        // if checkpoint resume is supported, this code block should be removed.
        GsiBackfillManager manager = new GsiBackfillManager(schemaName);
        manager.deleteByBackfillId(getTaskId());
    }

    protected void loadTable(ExecutionContext executionContext) {

        Map<Pair<String, String>, OSSBackFillWriterTask> tasks = null;
        try {

            String sourceLogicalSchema = this.loadTableSchema;
            String sourceLogicalTable = this.loadTableName;
            String targetLogicalSchema = physicalPlanData.getSchemaName();
            String targetLogicalTable = physicalPlanData.getLogicalTableName();

            // for loading table, we should read field_id from gms
            Map<String, String> columnToFieldIdMap = new SchemaEvolutionAccessorDelegate<Map<String, String>>() {
                @Override
                protected Map<String, String> invoke() {
                    Map<String, String> map = TreeMaps.caseInsensitiveMap();
                    for (ColumnMappingRecord record :
                        columnMappingAccessor.querySchemaTable(targetLogicalSchema, targetLogicalTable)) {
                        map.put(record.getColumnName(), record.getFieldIdString());
                    }
                    return map;
                }
            }.execute();

            ExecutionContext sourceDbContext = executionContext.copy();
            sourceDbContext.setSchemaName(sourceLogicalSchema);
            sourceDbContext.setBackfillId(getTaskId());

            TableMeta sourceTableMeta =
                executionContext.getSchemaManager(sourceLogicalSchema).getTable(sourceLogicalTable);
            if (!sourceTableMeta.isHasPrimaryKey()) {
                throw GeneralUtil.nestedException("Table must have primary key");
            }
            Engine sourceEngine = sourceTableMeta.getEngine();

            // build orc schema
            PolarDBXOrcSchema orcSchema =
                OrcMetaUtils.buildPolarDBXOrcSchema(sourceTableMeta, Optional.of(columnToFieldIdMap), false,
                    dictColumns);

            // data config
            Configuration conf = OrcMetaUtils.getConfiguration(executionContext, orcSchema);

            tasks = buildOssBackFillLoaderTasks(
                executionContext,
                sourceLogicalSchema,
                sourceLogicalTable,
                targetLogicalSchema,
                targetLogicalTable,
                sourceTableMeta,
                orcSchema,
                conf);

            Map<String, Set<String>> sourcePhyTables = OSSTaskUtils.genSourcePhyTables(tasks);
            final int parallelism =
                executionContext.getParamManager().getInt(ConnectionParams.OSS_BACKFILL_PARALLELISM);
            final long indexStride =
                executionContext.getParamManager().getLong(ConnectionParams.OSS_ORC_INDEX_STRIDE);

            // do back fill: select source table -> fill target orc file
            OSSBackFillExecutor backFillExecutor = new OSSBackFillExecutor(sourceEngine, this.tableEngine);
            backFillExecutor
                .backFill2FileStore(sourceLogicalSchema, sourceLogicalTable, targetLogicalTable, sourceDbContext, sourcePhyTables,
                    (int) indexStride, parallelism, tasks, null);

            // flush all
            tasks.forEach((pair, task) -> task.flush(sourceDbContext));
            // wait all async task done.
            tasks.forEach((pair, task) -> task.waitAsync());

//            new FileStorageAccessorDelegate<Integer>() {
//                @Override
//                protected Integer invoke() {
//                    // valid the meta files and column metas
//                    filesAccessor.ready(getTaskId(), schemaName, logicalTableName);
//                    columnMetaAccessor.ready(getTaskId(), schemaName, logicalTableName);
//                    return 0;
//                }
//            }.execute();
        } catch (Exception e) {
            if (tasks != null) {
                tasks.forEach((pair, task) -> task.cancelAsync());
            }
            throw GeneralUtil.nestedException(e);
        }
    }

    @NotNull
    private Map<Pair<String, String>, OSSBackFillWriterTask> buildOssBackFillLoaderTasks(
        ExecutionContext executionContext,
        String sourceLogicalSchema,
        String sourceLogicalTable,
        String targetLogicalSchema,
        String targetLogicalTable,
        TableMeta sourceTableMeta,
        PolarDBXOrcSchema orcSchema,
        Configuration conf) {

        final long maxRowsPerFile = executionContext.getParamManager().getLong(ConnectionParams.OSS_MAX_ROWS_PER_FILE);
        final boolean removeTmpFiles =
            executionContext.getParamManager().getBoolean(ConnectionParams.OSS_REMOVE_TMP_FILES);

        Map<Pair<String, String>, OSSBackFillWriterTask> tasks = new HashMap<>();

        // handle single table.
        Pair<String, String> singleTopology = OSSTaskUtils.getSingleTopology(sourceLogicalSchema, sourceLogicalTable, sourceTableMeta);

        PartitionInfo sourceTablePartitionInfo =
            OSSTaskUtils.getSourcePartitionInfo(executionContext, sourceLogicalSchema, sourceLogicalTable);

        ;
        // traverse each physical partition (phy table)
        for (PhysicalPartitionInfo physicalPartitionInfo :
            getFlattenedPartitionInfo(physicalPlanData.getPhysicalPartitionTopology())) {

            String targetPhySchema = physicalPartitionInfo.getGroupKey();
            String targetPhyTable = physicalPartitionInfo.getPhyTable();

            String partName = physicalPartitionInfo.getPartName();
            Pair<String, String> sourcePhySchemaAndTable = Optional
                .ofNullable(singleTopology)
                .orElseGet(() -> OSSTaskUtils.getSourcePhyTable(sourceTablePartitionInfo, partName));

            String sourcePhyTable = sourcePhySchemaAndTable.getValue();

            // for each physical table, add orc write task.
            OSSBackFillWriterTask task = new OSSBackFillWriterTask(
                // for target table
                targetLogicalSchema,
                targetLogicalTable,
                targetPhySchema,
                targetPhyTable,

                // for source table
                sourcePhySchemaAndTable.getKey(),
                sourcePhyTable,
                sourceTableMeta,
                null,
                tableEngine,
                getTaskId(),

                // for orc file conf
                conf,
                "",

                // for orc schema
                orcSchema,
                maxRowsPerFile,
                removeTmpFiles
            );
            tasks.put(sourcePhySchemaAndTable, task);
        }
        return tasks;
    }

    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(
        Map<String, List<PhysicalPartitionInfo>> partitionInfoMap) {
        List<PhysicalPartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfoMap.values().forEach(partitionInfos::addAll);
        return partitionInfos;
    }

    private boolean isLoadTable() {
        return loadTableName != null && archiveMode == ArchiveMode.LOADING;
    }
}
