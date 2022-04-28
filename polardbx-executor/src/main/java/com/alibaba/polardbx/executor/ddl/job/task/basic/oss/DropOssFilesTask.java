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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

/**
 * @author chenzilin
 * @date 2022/1/13 17:42
 */
@Getter
@TaskName(name = "DropOssFilesTask")
public class DropOssFilesTask extends BaseGmsTask {

    // files == null means delete all
    private Set<String> files;

    private String engine;

    @JSONCreator
    public DropOssFilesTask(String engine, String schemaName, String logicalTableName, Set<String> files) {
        super(schemaName, logicalTableName);
        this.files = files;
        this.engine = engine;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        Engine fileEngine = Engine.of(engine);
        long stamp = FileSystemManager.readLockWithTimeOut(fileEngine);
        try (Connection connection = MetaDbUtil.getConnection()) {
            FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(fileEngine);
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);

            ColumnMetaAccessor accessor = new ColumnMetaAccessor();
            accessor.setConnection(connection);

            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);

            FileStorageMetaStore fileStorageMetaStore = new FileStorageMetaStore(fileEngine);
            fileStorageMetaStore.setConnection(connection);

            List<ColumnsRecord> columnsRecords = tableInfoManager.queryColumns(schemaName, logicalTableName);
            List<FilesRecord> filesRecordList = tableInfoManager.queryFilesByLogicalSchemaTable(schemaName, logicalTableName);

            for (FilesRecord filesRecord : filesRecordList) {
                if (files == null) {
                    // means delete all
                } else if (!files.contains(filesRecord.getFileName())) {
                    continue;
                }
                for (ColumnsRecord columnsRecord : columnsRecords) {

                    List<ColumnMetasRecord> records = accessor.query(filesRecord.getFileName(), columnsRecord.columnName);
                    for (ColumnMetasRecord record : records) {
                        if (record.bloomFilterPath != null && !record.bloomFilterPath.isEmpty()) {
                            String dataFilePath = record.bloomFilterPath;
                            if (fileSystemGroup.exists(dataFilePath)) {
                                fileSystemGroup.delete(dataFilePath, false);
                            }
                            fileStorageMetaStore.deleteFile(dataFilePath);
                            accessor.delete(record.columnMetaId);
                        }
                    }
                }

                String dataFilePath = filesRecord.getFileName();
                if (fileSystemGroup.exists(dataFilePath)) {
                    fileSystemGroup.delete(dataFilePath, false);
                }
                fileStorageMetaStore.deleteFile(dataFilePath);
                filesAccessor.delete(filesRecord.fileId);

            }
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            FileSystemManager.unlockRead(fileEngine, stamp);
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }
}
