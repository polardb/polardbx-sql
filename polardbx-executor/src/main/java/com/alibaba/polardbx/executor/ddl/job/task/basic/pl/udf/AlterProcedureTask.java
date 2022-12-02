package com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.ProcedureAccessor;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AlterProcedureTask")
public class AlterProcedureTask extends BaseGmsTask {
    protected final String procedureName;
    protected final String procedureSchema;
    private final String alterProcedureContent;

    @JSONCreator
    public AlterProcedureTask(String schemaName, String logicalTableName, String procedureSchema,
                              String procedureName, String alterProcedureContent) {
        super(schemaName, logicalTableName);
        this.procedureSchema = procedureSchema;
        this.procedureName = procedureName;
        this.alterProcedureContent = alterProcedureContent;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomSuspendFromHint(executionContext);

        ProcedureAccessor procedureAccessor = new ProcedureAccessor();
        procedureAccessor.setConnection(metaDbConnection);
        procedureAccessor.alterProcedure(procedureSchema, procedureName, alterProcedureContent);
    }
}
