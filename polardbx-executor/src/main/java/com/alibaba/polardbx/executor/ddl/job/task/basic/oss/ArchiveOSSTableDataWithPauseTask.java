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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.FileStorageBackFillAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.FileStorageAccessorDelegate;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "ArchiveOSSTableDataWithPauseTask")
public class ArchiveOSSTableDataWithPauseTask extends ArchiveOSSTableDataTask {

    @JSONCreator
    public ArchiveOSSTableDataWithPauseTask(String schemaName, String logicalTableName, String loadTableSchema,
                                            String loadTableName, String physicalPartitionName,
                                            Engine targetTableEngine) {
        super(schemaName, logicalTableName, loadTableSchema, loadTableName, physicalPartitionName, targetTableEngine);
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateSupportedCommands(true, true, null);
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        updateSupportedCommands(true, true, null);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        executionContext.setBackfillId(getTaskId());

        new FileStorageAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                // rollback unfinished files
                List<FilesRecord> files = filesAccessor.queryUncommitted(getTaskId(), schemaName, logicalTableName);
                List<ColumnMetasRecord> columnMetas =
                    columnMetaAccessor.queryUncommitted(getTaskId(), schemaName, logicalTableName);
                deleteUncommitted(files, columnMetas);
                filesAccessor.deleteUncommitted(getTaskId(), schemaName, logicalTableName);
                columnMetaAccessor.deleteUncommitted(getTaskId(), schemaName, logicalTableName);
                return 0;
            }
        }.execute();

        // do real backfill
        loadTable(executionContext, true);
    }

    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackFileStorage(metaDbConnection, executionContext);

        // delete file_storage_backfill_object
        FileStorageBackFillAccessor fileStorageBackFillAccessor = new FileStorageBackFillAccessor();
        fileStorageBackFillAccessor.setConnection(metaDbConnection);
        fileStorageBackFillAccessor.deleteFileBackfillMeta(getTaskId());
    }

    protected String remark() {
        return String.format("|%s.%s.%s", loadTableSchema, loadTableName, physicalPartitionName);
    }
}
