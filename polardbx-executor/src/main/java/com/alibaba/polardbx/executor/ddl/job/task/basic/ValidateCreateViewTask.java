package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateViewHandler;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import lombok.Getter;

@TaskName(name = "ValidateViewTask")
@Getter
public class ValidateCreateViewTask extends BaseValidateTask {

    final private String viewName;
    final private Boolean isReplace;

    @JSONCreator
    public ValidateCreateViewTask(String schemaName, String viewName, boolean isReplace) {
        super(schemaName);
        this.viewName = viewName;
        this.isReplace = isReplace;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        ViewManager viewManager = OptimizerContext.getContext(schemaName).getViewManager();

        if (viewManager.count(schemaName) > LogicalCreateViewHandler.MAX_VIEW_NUMBER) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "View number at most " + LogicalCreateViewHandler.MAX_VIEW_NUMBER);
        }
        // check view name
        TableMeta tableMeta;
        try {
            tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(viewName);
        } catch (Throwable throwable) {
            // pass
            tableMeta = null;
        }

        if (tableMeta != null) {
            if (isReplace) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "'" + viewName + "' is not VIEW ");
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "table '" + viewName + "' already exists ");
            }
        }
    }

    @Override
    protected String remark() {
        return "|schema: " + schemaName + " viewName: " + viewName;
    }
}
