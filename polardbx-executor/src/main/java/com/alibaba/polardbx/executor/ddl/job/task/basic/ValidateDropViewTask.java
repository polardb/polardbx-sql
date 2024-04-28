package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import lombok.Getter;

@TaskName(name = "ValidateDropViewTask")
@Getter
public class ValidateDropViewTask extends BaseValidateTask {

    final private String viewName;
    protected Boolean ifExists;

    @JSONCreator
    public ValidateDropViewTask(String schemaName, String viewName, boolean ifExists) {
        super(schemaName);
        this.viewName = viewName;
        this.ifExists = ifExists;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        if (!ifExists) {
            SystemTableView.Row row = OptimizerContext.getContext(schemaName).getViewManager().select(viewName);
            if (row == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "Unknown view " + viewName);
            }
        }
    }

    @Override
    protected String remark() {
        return "|schema: " + schemaName + " viewName: " + viewName + " ifExists: " + ifExists;
    }
}
