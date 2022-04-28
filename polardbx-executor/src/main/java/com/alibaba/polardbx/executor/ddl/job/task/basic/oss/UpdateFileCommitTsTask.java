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
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "UpdateFileCommitTsTask")
public class UpdateFileCommitTsTask extends BaseGmsTask {

    private List<Long> fileTaskIdList;

    private String engine;

    @JSONCreator
    public UpdateFileCommitTsTask(String engine, String schemaName, String logicalTableName, List<Long> fileTaskIdList) {
        super(schemaName, logicalTableName);
        this.engine = engine;
        this.fileTaskIdList = fileTaskIdList;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        Engine fileEngine = Engine.of(engine);
        final ITimestampOracle timestampOracle = executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
        if (null == timestampOracle) {
            throw new UnsupportedOperationException("Do not support timestamp oracle");
        }

        long ts = timestampOracle.nextTimestamp();

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        FileStorageMetaStore fileStorageMetaStore = new FileStorageMetaStore(fileEngine);
        fileStorageMetaStore.setConnection(metaDbConnection);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        for (Long fileTaskId : fileTaskIdList) {
            List<FilesRecord> filesRecords = tableInfoManager.lockOssFile(fileTaskId, schemaName, logicalTableName);
            for (FilesRecord filesRecord : filesRecords) {
                if (filesRecord.getCommitTs() == null && filesRecord.getRemoveTs() == null) {
                    fileStorageMetaStore.updateFileCommitTs(filesRecord.fileName, ts);
                }
            }
            tableInfoManager.updateFilesCommitTs(ts, schemaName, logicalTableName, fileTaskId);
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        Engine fileEngine = Engine.of(engine);

        FileStorageMetaStore fileStorageMetaStore = new FileStorageMetaStore(fileEngine);
        fileStorageMetaStore.setConnection(metaDbConnection);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        for (Long fileTaskId : fileTaskIdList) {
            tableInfoManager.updateFilesCommitTs(null, schemaName, logicalTableName, fileTaskId);
            List<FilesRecord> filesRecords = tableInfoManager.lockOssFile(fileTaskId, schemaName, logicalTableName);
            for (FilesRecord filesRecord : filesRecords) {
                fileStorageMetaStore.deleteFile(filesRecord.fileName);
            }
        }
    }

}
