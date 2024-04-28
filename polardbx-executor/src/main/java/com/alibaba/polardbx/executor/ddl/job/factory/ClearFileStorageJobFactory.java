package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CloseFileStorageTask;
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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ClearFileStoragePreparedData;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ClearFileStorageJobFactory extends DdlJobFactory {

    final private ExecutionContext executionContext;
    final private ClearFileStoragePreparedData clearFileStoragePreparedData;

    public ClearFileStorageJobFactory(ClearFileStoragePreparedData clearFileStoragePreparedData,
                                      ExecutionContext executionContext) {
        this.executionContext = executionContext;
        this.clearFileStoragePreparedData = clearFileStoragePreparedData;
    }

    @Override
    protected void validate() {
        Engine engine = Engine.of(clearFileStoragePreparedData.getFileStorageName());

        if (!Engine.isFileStore(engine)) {
            throw new TddlNestableRuntimeException(engine.name() + " is not file storage ");
        }

        // validate file storage exists
        FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine, false);
        if (fileSystemGroup == null) {
            throw new TddlNestableRuntimeException("file storage " + engine.name() + " is not exists ");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        Engine engine = Engine.of(clearFileStoragePreparedData.getFileStorageName());

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();

        taskList.addAll(AlterFileStoragePurgeBeforeTimestampJobFactory.buildRecycleBinPurgeBeforeTimestamp(engine,
            new Timestamp(System.currentTimeMillis()), executionContext));

        List<TablesRecord> tablesRecordList;
        try (Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaConn);
            tablesRecordList = tableInfoManager.queryTablesByEngineAndTableType(engine.name(), "ORC TABLE");
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }

        // drop cold data table
        for (TablesRecord tablesRecord : tablesRecordList) {
            String logicalSchemaName = tablesRecord.tableSchema;
            String logicalTableName = tablesRecord.tableName;

            if (logicalTableName.startsWith(RecycleBin.FILE_STORAGE_PREFIX)) {
                continue;
            }

            taskList.add(new UnBindingArchiveTableMetaByArchiveTableTask(logicalSchemaName, logicalTableName));

            taskList.addAll(
                OSSTaskUtils.dropTableTasks(engine, logicalSchemaName, logicalTableName, true, executionContext));
        }

        // only clean cold data metas
        taskList.add(new CloseFileStorageTask(engine.name(), true));

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
