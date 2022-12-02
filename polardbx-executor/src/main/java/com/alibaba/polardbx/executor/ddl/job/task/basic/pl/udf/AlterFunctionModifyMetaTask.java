package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.ProcedureAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AlterFunctionModifyMetaTask")
public class AlterFunctionModifyMetaTask extends BaseGmsTask {
    protected final String functionName;
    private final String alterFunctionContent;

    @JSONCreator
    public AlterFunctionModifyMetaTask(String schemaName, String logicalTableName, String functionName,
                              String alterFunctionContent) {
        super(schemaName, logicalTableName);
        this.functionName = functionName;
        this.alterFunctionContent = alterFunctionContent;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomSuspendFromHint(executionContext);

        FunctionAccessor functionAccessor = new FunctionAccessor();
        functionAccessor.setConnection(metaDbConnection);
        functionAccessor.alterFunction(functionName, alterFunctionContent);
    }
}
