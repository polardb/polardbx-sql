package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.util.TtlEventLogUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

/**
 * @author chenghui.lch
 */
@Getter
@TaskName(name = "FinishCleaningUpAndLogTask")
public class FinishCleaningUpAndLogTask extends AbstractTtlJobTask {

    @JSONCreator
    public FinishCleaningUpAndLogTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
    }

    protected void fetchTtlJobContextFromPreviousTask() {
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(),
            PrepareCleanupIntervalTask.class,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        executeImpl(executionContext);
    }

    protected void executeImpl(ExecutionContext executionContext) {
        TtlJobUtil.updateJobStage(this.jobContext, "Finished");
        TtlEventLogUtil.logCleanupExpiredDataEvent(schemaName, logicalTableName);
    }
}
