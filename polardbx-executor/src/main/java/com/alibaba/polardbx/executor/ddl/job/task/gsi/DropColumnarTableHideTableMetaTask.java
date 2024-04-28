package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "DropColumnarTableHideTableMetaTask")
public class DropColumnarTableHideTableMetaTask extends BaseGmsTask {

    protected final String indexName;

    @JSONCreator
    public DropColumnarTableHideTableMetaTask(String schemaName, String logicalTableName, String indexName) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        TableMetaChanger.hideTableMeta(metaDbConnection, schemaName, indexName);
        TableMetaChanger.notifyDropColumnarIndex(metaDbConnection, schemaName, logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.showTableMeta(metaDbConnection, schemaName, indexName);
        TableMetaChanger.notifyCreateColumnarIndex(metaDbConnection, schemaName, logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, indexName), SyncScope.ALL);
    }

}
