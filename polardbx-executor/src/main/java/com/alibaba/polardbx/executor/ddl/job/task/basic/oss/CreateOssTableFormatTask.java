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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.oss.access.OSSKey;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.alibaba.polardbx.gms.metadb.table.FilesRecord.TABLE_FORMAT_TYPE;

@Getter
@TaskName(name = "CreateOssTableFormatTask")
public class CreateOssTableFormatTask extends BaseGmsTask {
    private final PhysicalPlanData physicalPlanData;
    private final Engine tableEngine;

    public CreateOssTableFormatTask(String schemaName, String logicalTableName, PhysicalPlanData physicalPlanData,
                                    Engine tableEngine) {
        super(schemaName, logicalTableName);
        this.physicalPlanData = physicalPlanData;
        this.tableEngine = tableEngine;

        onExceptionTryRollback();
    }

    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                // 0. don't continue the ddl if it was paused
                MetaDbUtil.beginTransaction(metaDbConn);
                List<FilesRecord> files =
                    TableMetaChanger.lockOssFileMeta(metaDbConn, getTaskId(), schemaName, logicalTableName);
                if (files != null && files.size() > 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CANT_CONTINUE_DDL);
                }
                MetaDbUtil.commit(metaDbConn);

                final String uniqueId = UUID.randomUUID().toString();
                // 1. get table format path
                OSSKey ossKey = OSSKey.createFormatFileOSSKey(schemaName, logicalTableName, uniqueId);
                String fileName = ossKey.localPath();
                File localFormatFile = FileSystemUtils.createLocalFile(fileName);

                // 2. write to metaDB files table.
                FilesRecord filesRecord = buildFilesRecord(localFormatFile, ossKey);
                TableMetaChanger.addOssFileMeta(metaDbConn, schemaName, logicalTableName, filesRecord);

                // 3. write table format info to *.format
                try (FileWriter fileWriter = new FileWriter(localFormatFile)) {
                    Map<String, List<List<String>>> tableTopology = physicalPlanData.getTableTopology();
                    String nativeCreateTableSql = physicalPlanData.getSqlTemplate();
                    fileWriter.write(schemaName);
                    fileWriter.write("\n");
                    fileWriter.write(logicalTableName);
                    fileWriter.write("\n");
                    fileWriter.write(nativeCreateTableSql);
                    fileWriter.write("\n");
                    fileWriter.write(tableTopology.toString());
                }

                // 4. upload to oss
                FileSystemUtils.writeFile(localFormatFile, ossKey.toString(), tableEngine);

                // 5. delete tmp file
                localFormatFile.delete();

                // valid table meta
                MetaDbUtil.beginTransaction(metaDbConn);
                TableMetaChanger.lockOssFileMeta(metaDbConn, getTaskId(), schemaName, logicalTableName);
                TableMetaChanger.validOssFileMeta(metaDbConn, getTaskId(), schemaName, logicalTableName);
                MetaDbUtil.commit(metaDbConn);

            } catch (Exception e) {
                MetaDbUtil.rollback(metaDbConn, e, null, null);
                // don't interrupt!
                e.printStackTrace();
                throw GeneralUtil.nestedException(e);
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, null);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        List<FilesRecord> files =
            TableMetaChanger.lockOssFileMeta(metaDbConnection, getTaskId(), schemaName, logicalTableName);
        for (FilesRecord record : files) {
            FileSystemUtils.deleteIfExistsFile(record.getFileName(), this.tableEngine);
            File tmpFile = new File(record.getLocalPath());
            if (tmpFile.exists()) {
                if (!tmpFile.delete()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OSS_FORMAT,
                        "can't delete file " + record.getLocalPath());
                }
            }
        }
        //delete table meta
        TableMetaChanger.deleteOssFileMeta(metaDbConnection, getTaskId(), schemaName, logicalTableName);
    }

    @NotNull
    private FilesRecord buildFilesRecord(File localFormatFile, OSSKey ossKey) {
        FilesRecord filesRecord = new FilesRecord();
        filesRecord.fileName = ossKey.toString();
        filesRecord.fileType = TABLE_FORMAT_TYPE;
        filesRecord.fileMeta = new byte[] {};
        filesRecord.tablespaceName = null;
        filesRecord.tableCatalog = "";
        filesRecord.tableSchema = schemaName;
        filesRecord.tableName = logicalTableName;
        filesRecord.logfileGroupName = null;
        filesRecord.logfileGroupNumber = 0;
        filesRecord.engine = this.tableEngine.name();
        filesRecord.fulltextKeys = null;
        filesRecord.deletedRows = 0;
        filesRecord.updateCount = 0;
        filesRecord.freeExtents = 0;
        filesRecord.totalExtents = 0;
        filesRecord.extentSize = 0;
        filesRecord.initialSize = 0;
        filesRecord.maximumSize = 0;
        filesRecord.autoextendSize = 0;
        filesRecord.creationTime = null;
        filesRecord.lastUpdateTime = null;
        filesRecord.lastAccessTime = null;
        filesRecord.recoverTime = 0;
        filesRecord.transactionCounter = 0;
        filesRecord.version = 0;
        filesRecord.rowFormat = null;
        filesRecord.tableRows = 0;
        filesRecord.avgRowLength = 0;
        filesRecord.dataLength = 0;
        filesRecord.maxDataLength = 0;
        filesRecord.indexLength = 0;
        filesRecord.dataFree = 0;
        filesRecord.createTime = null;
        filesRecord.updateTime = null;
        filesRecord.checkTime = null;
        filesRecord.checksum = 0;
        filesRecord.status = "VISIBLE";
        filesRecord.extra = null;
        filesRecord.taskId = getTaskId();
        filesRecord.lifeCycle = OSSMetaLifeCycle.CREATING.ordinal();
        filesRecord.localPath = localFormatFile.toString();
        filesRecord.logicalSchemaName = schemaName;
        filesRecord.logicalTableName = logicalTableName;
        return filesRecord;
    }
}
