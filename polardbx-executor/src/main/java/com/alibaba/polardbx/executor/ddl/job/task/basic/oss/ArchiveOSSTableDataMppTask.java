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
import com.alibaba.polardbx.executor.ddl.job.meta.FileStorageBackFillAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.FileStorageAccessorDelegate;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "ArchiveOSSTableDataMPPTask")
public class ArchiveOSSTableDataMppTask extends ArchiveOSSTableDataTask implements RemoteExecutableDdlTask {

    /**
     * the total number ofArchiveOSSTableDataMPPTask for a table's local partition
     */
    private int totalTaskNumber;

    /**
     * the serial id of ArchiveOSSTableDataMPPTask for a table's local partition
     */
    private int serialNumber;

    /**
     * in this case, the archive backfill task of table A is split into 4 tasks,
     * each task responsible for some physical tables.
     * totalTaskNumber is 4, and serialNumber is 0
     * -------A
     * ----/ / \ \
     * --/  /   \  \
     * /   /     \   \
     * 0^  1    2    3
     */

    @JSONCreator
    public ArchiveOSSTableDataMppTask(String schemaName, String logicalTableName, String loadTableSchema,
                                      String loadTableName, String physicalPartitionName,
                                      Engine targetTableEngine, int totalTaskNumber, int serialNumber) {
        super(schemaName, logicalTableName, loadTableSchema, loadTableName, physicalPartitionName, targetTableEngine);
        this.totalTaskNumber = totalTaskNumber;
        this.serialNumber = serialNumber;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateSupportedCommands(true, true, null);
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
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

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        updateSupportedCommands(true, true, null);
    }

    @Override
    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table) {
        return OSSTaskUtils.getOrderedPartitionInfo(schema, table, totalTaskNumber, serialNumber);
    }

    @Override
    public Optional<String> chooseServer() {
        if (forbidRemoteDdlTask()) {
            return Optional.empty();
        }
        return OSSTaskUtils.chooseRemoteNode(taskId);
    }

    protected String remark() {
        String partitions =
            OSSTaskUtils.getOrderedPartitionInfo(loadTableSchema, loadTableName, totalTaskNumber, serialNumber).stream()
                .map(x -> x.getPartName()).collect(
                    Collectors.joining(","));
        if (StringUtils.isEmpty(partitions)) {
            partitions = "empty";
        }
        return String.format("|%s.%s.%s, task SN: %d/%d, partitions: %s", loadTableSchema, loadTableName,
            physicalPartitionName,
            serialNumber, totalTaskNumber, partitions);
    }
}
