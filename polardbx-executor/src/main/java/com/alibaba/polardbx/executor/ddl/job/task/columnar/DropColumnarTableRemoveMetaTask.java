package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "DropColumnarTableRemoveMetaTask")
public class DropColumnarTableRemoveMetaTask extends BaseGmsTask {

    private final String columnarTableName;

    @JSONCreator
    public DropColumnarTableRemoveMetaTask(String schemaName, String logicalTableName, String columnarTableName) {
        super(schemaName, logicalTableName);
        this.columnarTableName = columnarTableName;
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.removeColumnarTableMeta(metaDbConnection, schemaName, columnarTableName);
        CommonMetaChanger.finalOperationsOnSuccess(schemaName, columnarTableName);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        TableMetaChanger.afterRemovingTableMeta(schemaName, logicalTableName);
    }
}
