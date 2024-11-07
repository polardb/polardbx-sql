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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.TimestampUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DoNothingTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.DeleteOssFilesTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.DropOssFilesTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.FileStorageBackupTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileRemoveTsTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterFileStoragePreparedData;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.utils.ITimestampOracle.BITS_LOGICAL_TIME;

/**
 * @author chenzilin
 */
public class AlterFileStorageAsOfTimestampJobFactory extends DdlJobFactory {

    private static final Logger logger = LoggerFactory.getLogger("oss");

    private ExecutionContext executionContext;
    private AlterFileStoragePreparedData alterFileStoragePreparedData;

    public AlterFileStorageAsOfTimestampJobFactory(
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
        TimeZone fromTimeZone;
        if (executionContext.getTimeZone() != null) {
            fromTimeZone = executionContext.getTimeZone().getTimeZone();
        } else {
            fromTimeZone = TimeZone.getDefault();
        }

        long ts =
            TimestampUtils.getTsFromTimestampWithTimeZone(alterFileStoragePreparedData.getTimestamp(), fromTimeZone);

        List<FilesRecord> toDeleteFileRecordList = new ArrayList<>();
        List<FilesRecord> toUpdateFileRecordList = new ArrayList<>();

        List<FileStorageMetaStore.OssFileMeta> toDeleteFileMeta = new ArrayList<>();
        List<FileStorageMetaStore.OssFileMeta> toUpdateFileMeta = new ArrayList<>();

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            FileStorageMetaStore fileStorageMetaStore = new FileStorageMetaStore(engine);
            fileStorageMetaStore.setConnection(conn);

            // TODO(siyun): prevent columnar file being purged
            List<FileStorageMetaStore.OssFileMeta> fileMetaList = fileStorageMetaStore.queryFromFileStorage();

            for (FileStorageMetaStore.OssFileMeta ossFileMeta : fileMetaList) {
                if (ossFileMeta.getCommitTs() == null) {
                    // commitTs == null means files was not committed
                    // ignore
                } else if (ts < ossFileMeta.getCommitTs()) {
                    // delete
                    toDeleteFileMeta.add(ossFileMeta);
                } else if (ts >= ossFileMeta.getCommitTs() && ossFileMeta.getRemoveTs() == null) {
                    // ignore
                } else if (ts >= ossFileMeta.getCommitTs() && ts <= ossFileMeta.getRemoveTs()) {
                    // update removeTs = null
                    toUpdateFileMeta.add(ossFileMeta);
                } else if (ts > ossFileMeta.getRemoveTs()) {
                    // ignore
                } else {
                    throw new AssertionError("impossible case");
                }
            }

            logger.info("alter filestorage as of timestamp " + alterFileStoragePreparedData.getTimestamp());
            logger.info(String.format("to delete %s files with commit_ts > %s : ", toDeleteFileMeta.size(), ts));
            logger.info(toDeleteFileMeta.stream().map(x -> x.getDataPath()).collect(Collectors.joining("\n")));
            logger.info(
                String.format("to update %s files with commit_ts <= %s <= remove_ts : ", toUpdateFileMeta.size(), ts));
            logger.info(toUpdateFileMeta.stream().map(x -> x.getDataPath()).collect(Collectors.joining("\n")));

            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(conn);
            for (FileStorageMetaStore.OssFileMeta ossFileMeta : toDeleteFileMeta) {
                // NOTE: ensure fileName is unique
                List<FilesRecord> filesRecords = tableInfoManager.queryFilesByFileName(ossFileMeta.getDataPath());
                if (!filesRecords.isEmpty()) {
                    toDeleteFileRecordList.addAll(filesRecords);
                }
            }

            for (FileStorageMetaStore.OssFileMeta ossFileMeta : toUpdateFileMeta) {
                List<FilesRecord> filesRecords = tableInfoManager.queryFilesByFileName(ossFileMeta.getDataPath());
                if (!filesRecords.isEmpty()) {
                    toUpdateFileRecordList.addAll(filesRecords);
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

        toUpdateFileRecordList = toUpdateFileRecordList.stream().sorted(comparator).collect(Collectors.toList());
        toDeleteFileRecordList = toDeleteFileRecordList.stream().sorted(comparator).collect(Collectors.toList());

        Set<Pair<String, String>> schemaTablePair = new HashSet<>();

        if (!toUpdateFileRecordList.isEmpty()) {
            String schemaName = toUpdateFileRecordList.get(0).getLogicalSchemaName();
            String logicalTableName = toUpdateFileRecordList.get(0).getLogicalTableName();
            schemaTablePair.add(Pair.of(schemaName, logicalTableName));
            List<FilesRecord> currentFilesRecordList = new ArrayList<>();

            for (int i = 0; i < toUpdateFileRecordList.size(); i++) {
                FilesRecord filesRecord = toUpdateFileRecordList.get(i);
                if (schemaName.equals(filesRecord.getLogicalSchemaName()) && logicalTableName.equals(
                    filesRecord.getLogicalTableName())) {
                    currentFilesRecordList.add(filesRecord);
                } else {
                    UpdateFileRemoveTsTask updateFileRemoveTsTask = new UpdateFileRemoveTsTask(engine.name(),
                        schemaName, logicalTableName,
                        currentFilesRecordList.stream().map(x -> x.getFileName()).collect(Collectors.toList()), null);
                    taskList.add(updateFileRemoveTsTask);
                    schemaName = filesRecord.getLogicalSchemaName();
                    logicalTableName = filesRecord.getLogicalTableName();
                    currentFilesRecordList.clear();
                    currentFilesRecordList.add(filesRecord);
                    schemaTablePair.add(Pair.of(schemaName, logicalTableName));
                }

                if (!currentFilesRecordList.isEmpty()) {
                    UpdateFileRemoveTsTask updateFileRemoveTsTask = new UpdateFileRemoveTsTask(engine.name(),
                        schemaName, logicalTableName,
                        currentFilesRecordList.stream().map(x -> x.getFileName()).collect(Collectors.toList()), null);
                    taskList.add(updateFileRemoveTsTask);
                }
            }
        }

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

        if (!toDeleteFileMeta.isEmpty()) {
            taskList.add(new DeleteOssFilesTask(engine.name(), DefaultDbSchema.NAME,
                toDeleteFileMeta.stream().map(x -> x.getDataPath()).collect(Collectors.toSet())));
        }

        for (Pair<String, String> pair : schemaTablePair) {
            // sync to other tables
            TableSyncTask tableSyncTask = new TableSyncTask(pair.getKey(), pair.getValue());
            taskList.add(tableSyncTask);
        }

        if (taskList.isEmpty()) {
            taskList.add(new DoNothingTask(executionContext.getSchemaName()));
        } else {
            taskList.add(new FileStorageBackupTask(engine.name()));
        }
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {

    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}


