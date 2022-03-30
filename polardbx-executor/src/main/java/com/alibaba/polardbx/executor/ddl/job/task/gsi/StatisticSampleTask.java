package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@TaskName(name = "StatisticSampleTask")
@Getter
public class StatisticSampleTask extends BaseDdlTask {
    private String logicalTableName;

    @JSONCreator
    public StatisticSampleTask(String schemaName,
                               String logicalTableName) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        OptimizerContext.getContext(schemaName).getStatisticManager().sampleTable(logicalTableName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(String.format("sample table task. schema:%s, table:%s", schemaName, logicalTableName));
    }
}
