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
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "UpdateFileRemoveTsTask")
public class UpdateFileRemoveTsTask extends BaseGmsTask {

    private List<String> files;

    private String engine;

    private Long ts;

    @JSONCreator
    public UpdateFileRemoveTsTask(String engine, String schemaName, String logicalTableName, List<String> files,
                                  Long ts) {
        super(schemaName, logicalTableName);
        this.engine = engine;
        this.files = files;
        this.ts = ts;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        Engine fileEngine = Engine.of(engine);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        FileStorageMetaStore fileStorageMetaStore = new FileStorageMetaStore(fileEngine);
        fileStorageMetaStore.setConnection(metaDbConnection);

        if (files != null && !files.isEmpty()) {
            List<FilesRecord> filesRecords =
                tableInfoManager.queryFilesByLogicalSchemaTable(schemaName, logicalTableName);
            for (FilesRecord filesRecord : filesRecords) {
                if (files.contains(filesRecord.getFileName())) {
                    if (ts == null) {
                        fileStorageMetaStore.deleteFileRemoveTs(filesRecord.fileName);
                    } else if (filesRecord.getCommitTs() != null && filesRecord.getRemoveTs() == null) {
                        fileStorageMetaStore.updateFileRemoveTs(filesRecord.fileName, ts);
                    }
                }
            }
            tableInfoManager.updateFilesRemoveTs(ts, schemaName, logicalTableName, files);
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

}
