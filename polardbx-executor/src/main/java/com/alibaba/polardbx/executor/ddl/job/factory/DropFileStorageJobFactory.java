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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CloseFileStorageTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.DeleteFileStorageDirectoryTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UnBindingArchiveTableMetaByArchiveTableTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropFileStoragePreparedData;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DropFileStorageJobFactory extends DdlJobFactory {

    private static final Logger logger = LoggerFactory.getLogger("oss");

    private ExecutionContext executionContext;
    private DropFileStoragePreparedData dropFileStoragePreparedData;

    public DropFileStorageJobFactory(
        DropFileStoragePreparedData dropFileStoragePreparedData,
        ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.dropFileStoragePreparedData = dropFileStoragePreparedData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        Engine engine = Engine.of(dropFileStoragePreparedData.getFileStorageName());

        if (!Engine.isFileStore(engine)) {
            throw new TddlNestableRuntimeException(engine.name() + " is not file storage ");
        }

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();

        // validate file storage exists
        FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine, false);
        if (fileSystemGroup == null) {
            throw new TddlNestableRuntimeException("file storage " + engine.name() + " is not exists ");
        }

        // purge file storage recycle bin
        taskList.addAll(AlterFileStoragePurgeBeforeTimestampJobFactory.buildRecycleBinPurgeBeforeTimestamp(engine,
            new Timestamp(System.currentTimeMillis()), executionContext));

        List<TablesRecord> tablesRecordList;
        try (Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaConn);
            tablesRecordList = tableInfoManager.queryTablesByEngine(engine.name());
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }

        // drop file storage table
        for (TablesRecord tablesRecord : tablesRecordList) {
            String logicalSchemaName = tablesRecord.tableSchema;
            String logicalTableName = tablesRecord.tableName;

            if (logicalTableName.startsWith(RecycleBin.FILE_STORAGE_PREFIX)) {
                continue;
            }

            UnBindingArchiveTableMetaByArchiveTableTask unBindingArchiveTask
                = new UnBindingArchiveTableMetaByArchiveTableTask(logicalSchemaName, logicalTableName);

            taskList.add(unBindingArchiveTask);

            taskList.addAll(
                OSSTaskUtils.dropTableTasks(engine, logicalSchemaName, logicalTableName, true, executionContext));
        }

        // delete whole file storage directory
        taskList.add(new DeleteFileStorageDirectoryTask(engine.name(), "/"));

        // close file storage
        taskList.add(new CloseFileStorageTask(engine.name(), false));

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



