package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.udf.AlterProcedureTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterProcedure;
import org.apache.calcite.sql.SqlAlterProcedure;

import java.util.ArrayList;
import java.util.List;

public class LogicalAlterProcedureHandler extends LogicalCommonDdlHandler{
    public LogicalAlterProcedureHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext){
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        List<DdlTask> taskList = new ArrayList<>();
        taskList.add(getAlterProcedureTask(logicalDdlPlan, executionContext));
        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    private AlterProcedureTask getAlterProcedureTask(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlAlterProcedure alterProcedure = ((LogicalAlterProcedure) logicalDdlPlan).getSqlAlterProcedure();
        SQLName procedureName = alterProcedure.getProcedureName();
        String procedureSchema = PLUtils.getProcedureSchema(procedureName, executionContext.getSchemaName());

        return new AlterProcedureTask(logicalDdlPlan.getSchemaName(), null,
            procedureSchema, SQLUtils.normalize(procedureName.getSimpleName()),
            alterProcedure.getText());
    }
}
