package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@Getter
@TaskName(name = "EmptyTableGroupValidateTask")
public class EmptyTableGroupValidateTask extends BaseValidateTask {

    private String tableGroupName;

    @JSONCreator
    public EmptyTableGroupValidateTask(String schemaName,
                                       String tableGroupName) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableGroupValidator.validateTableGroupIsEmpty(schemaName, tableGroupName);
    }

    @Override
    protected String remark() {
        return "|tableGroupName: " + tableGroupName;
    }

}
