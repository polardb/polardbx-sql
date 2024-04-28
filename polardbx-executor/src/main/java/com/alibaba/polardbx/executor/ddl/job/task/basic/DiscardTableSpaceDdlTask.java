package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "DiscardTableSpaceDdlTask")
public class DiscardTableSpaceDdlTask extends BasePhyDdlTask {

    private String logicalTableName;

    @JSONCreator
    public DiscardTableSpaceDdlTask(String schemaName, String logicalTableName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        this.logicalTableName = logicalTableName;
        onExceptionTryRecoveryThenRollback();
    }

    public void executeImpl(ExecutionContext executionContext) {
        super.executeImpl(executionContext);
    }
}
