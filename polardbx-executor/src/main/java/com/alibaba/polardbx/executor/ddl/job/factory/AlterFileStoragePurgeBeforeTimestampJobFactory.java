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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DoNothingTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.DeleteOssFilesTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.DeleteRecycleBinTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.DropOssFilesTask;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalRenameTableHandler;
import com.alibaba.polardbx.gms.engine.FileStorageFilesMetaRecord;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterFileStoragePreparedData;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.common.RecycleBin.FILE_STORAGE_PREFIX;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.common.RecycleBin.FILE_STORAGE_PREFIX;
import static com.alibaba.polardbx.optimizer.utils.ITimestampOracle.BITS_LOGICAL_TIME;

public class AlterFileStoragePurgeBeforeTimestampJobFactory extends DdlJobFactory {

    private static final Logger logger = LoggerFactory.getLogger("oss");

    private ExecutionContext executionContext;
    private AlterFileStoragePreparedData alterFileStoragePreparedData;

    public AlterFileStoragePurgeBeforeTimestampJobFactory(
        AlterFileStoragePreparedData alterFileStoragePreparedData,
        ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.alterFileStoragePreparedData = alterFileStoragePreparedData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        Engine engine = Engine.of(alterFileStoragePreparedData.getFileStorageName());
        Timestamp timestamp = Timestamp.valueOf(alterFileStoragePreparedData.getTimestamp());

        TimeZone fromTimeZone;
        if (executionContext.getTimeZone() != null) {
            fromTimeZone = executionContext.getTimeZone().getTimeZone();
        } else {
            fromTimeZone = TimeZone.getDefault();
        }

        long ts =
            OSSTaskUtils.getTsFromTimestampWithTimeZone(alterFileStoragePreparedData.getTimestamp(), fromTimeZone);

        // ensure purge do not affect backup
        int backupOssPeriodInDay = executionContext.getParamManager().getInt(ConnectionParams.BACKUP_OSS_PERIOD);
        final ITimestampOracle timestampOracle =
            executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
        if (null == timestampOracle) {
            throw new UnsupportedOperationException("Do not support timestamp oracle");
        }
        long nowTs = timestampOracle.nextTimestamp();

        int day = (int) (((nowTs - ts) >>> ITimestampOracle.BITS_LOGICAL_TIME) / (24 * 60 * 60 * 1000));
        if (day < backupOssPeriodInDay) {
            throw new IllegalArgumentException("Can't purge file storage within backup period");
        }

        List<FileStorageFilesMetaRecord> toDeleteFileMeta = new ArrayList<>();
        List<FilesRecord> toDeleteFileRecordList = new ArrayList<>();

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            FileStorageMetaStore fileStorageMetaStore = new FileStorageMetaStore(engine);
            fileStorageMetaStore.setConnection(conn);

            toDeleteFileMeta = fileStorageMetaStore.queryTableNeedToPurge(ts);

            logger.info("alter filestorage purge before timestamp " + alterFileStoragePreparedData.getTimestamp());
            logger.info(String.format("to purge %s files with remove_ts < %s : ", toDeleteFileMeta.size(), ts));
            logger.info(toDeleteFileMeta.stream().map(x -> x.fileName).collect(Collectors.joining("\n")));

            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(conn);
            for (FileStorageFilesMetaRecord ossFileMeta : toDeleteFileMeta) {
                // NOTE: ensure fileName is unique
                List<FilesRecord> filesRecords = tableInfoManager.queryFilesByFileName(ossFileMeta.fileName);
                if (!filesRecords.isEmpty()) {
                    toDeleteFileRecordList.addAll(filesRecords);
                }
            }
        } catch (Throwable throwable) {
            throw new TddlNestableRuntimeException(throwable);
        }

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();

        Comparator<? super FilesRecord> comparator = (a, b) -> {
            int cmp1 = a.getLogicalSchemaName().compareTo(b.getLogicalSchemaName());
            if (cmp1 != 0) {
                return cmp1;
            }
            int cmp2 = a.getLogicalTableName().compareTo(b.getLogicalTableName());
            return cmp2;
        };

        toDeleteFileRecordList = toDeleteFileRecordList.stream().sorted(comparator).collect(Collectors.toList());

        Set<Pair<String, String>> schemaTablePair = new HashSet<>();

        if (!toDeleteFileRecordList.isEmpty()) {
            String schemaName = toDeleteFileRecordList.get(0).getLogicalSchemaName();
            String logicalTableName = toDeleteFileRecordList.get(0).getLogicalTableName();
            schemaTablePair.add(Pair.of(schemaName, logicalTableName));
            List<FilesRecord> currentFilesRecordList = new ArrayList<>();

            for (int i = 0; i < toDeleteFileRecordList.size(); i++) {
                FilesRecord filesRecord = toDeleteFileRecordList.get(i);
                if (schemaName.equals(filesRecord.getLogicalSchemaName()) && logicalTableName.equals(
                    filesRecord.getLogicalTableName())) {
                    currentFilesRecordList.add(filesRecord);
                } else {
                    DropOssFilesTask dropOssFilesTask =
                        new DropOssFilesTask(engine.name(), schemaName, logicalTableName,
                            currentFilesRecordList.stream().map(x -> x.getFileName()).collect(Collectors.toSet()));
                    taskList.add(dropOssFilesTask);
                    schemaName = filesRecord.getLogicalSchemaName();
                    logicalTableName = filesRecord.getLogicalTableName();
                    currentFilesRecordList.clear();
                    currentFilesRecordList.add(filesRecord);
                    schemaTablePair.add(Pair.of(schemaName, logicalTableName));
                }
            }

            if (!currentFilesRecordList.isEmpty()) {
                DropOssFilesTask dropOssFilesTask = new DropOssFilesTask(engine.name(), schemaName, logicalTableName,
                    currentFilesRecordList.stream().map(x -> x.getFileName()).collect(Collectors.toSet()));
                taskList.add(dropOssFilesTask);
            }
        }

        // purge table if table drop (must in recycle_bin) before given ts
        taskList.addAll(buildRecycleBinPurgeBeforeTimestamp(engine, timestamp, executionContext));

        if (!toDeleteFileMeta.isEmpty()) {
            taskList.add(new DeleteOssFilesTask(engine.name(), DefaultDbSchema.NAME,
                toDeleteFileMeta.stream().map(x -> x.fileName).collect(Collectors.toSet())));
        }

        for (Pair<String, String> pair : schemaTablePair) {
            // sync to other tables
            TableSyncTask tableSyncTask = new TableSyncTask(pair.getKey(), pair.getValue());
            taskList.add(tableSyncTask);
        }

        if (taskList.isEmpty()) {
            taskList.add(new DoNothingTask(executionContext.getSchemaName()));
        }
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    public static List<DdlTask> buildRecycleBinPurgeBeforeTimestamp(Engine engine, Timestamp ts,
                                                                    ExecutionContext executionContext) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            List<DdlTask> ddlTasks = new ArrayList<>();
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(conn);

            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(String.format(
                "select `schema_name`, `name`, `original_name`, `gmt_create` from %s where name like '%s%%'",
                GmsSystemTables.RECYCLE_BIN,
                FILE_STORAGE_PREFIX));

            while (resultSet.next()) {
                String schemaName = resultSet.getString(1);
                String binName = resultSet.getString(2);
                String originalName = resultSet.getString(3);
                Timestamp binCreateTimestamp = resultSet.getTimestamp(4);
                if (!SystemDbHelper.isDBBuildIn(schemaName)) {
                    if (ts.compareTo(binCreateTimestamp) > 0) {
                        TablesRecord tablesRecord = tableInfoManager.queryTable(schemaName, binName, false);
                        if (tablesRecord != null
                            && engine.name().equalsIgnoreCase(tablesRecord.engine)) {
                            // purge
                            ddlTasks.addAll(
                                buildPurgeOssRecycleBin(engine, schemaName, binName, executionContext.copy()));
                        }
                    }
                }
            }
            return ddlTasks;
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    private static List<DdlTask> buildPurgeOssRecycleBin(Engine engine, String schemaName, String binName,
                                                         ExecutionContext executionContext) {
        // TODO: improve makeTableVisible
        LogicalRenameTableHandler.makeTableVisible(schemaName, binName, executionContext);
        List<DdlTask> taskList = new ArrayList<>();
        taskList.addAll(OSSTaskUtils.dropTableTasks(engine, schemaName, binName, true, executionContext));
        DeleteRecycleBinTask deleteRecycleBinTask = new DeleteRecycleBinTask(schemaName, binName);
        taskList.add(deleteRecycleBinTask);
        return taskList;
    }

    @Override
    protected void excludeResources(Set<String> resources) {

    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}


