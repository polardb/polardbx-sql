package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCreateViewHandler;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "CreateViewAddMetaTask")
public class CreateViewAddMetaTask extends BaseDdlTask {

    protected String viewName;
    protected Boolean isReplace;
    protected List<String> columnList;
    protected String viewDefinition;
    protected String planString;
    protected String planType;

    @JSONCreator
    public CreateViewAddMetaTask(String schemaName,
                                 String viewName,
                                 boolean isReplace,
                                 List<String> columnList,
                                 String viewDefinition,
                                 String planString,
                                 String planType) {
        super(schemaName);
        this.viewName = viewName;
        this.isReplace = isReplace;
        this.columnList = columnList;
        this.viewDefinition = viewDefinition;
        this.planString = planString;
        this.planType = planType;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ViewManager viewManager = OptimizerContext.getContext(schemaName).getViewManager();

        boolean success;
        if (isReplace) {
            success = viewManager
                .replace(viewName, columnList, viewDefinition, executionContext.getConnection().getUser(), planString,
                    planType);
            if (!success) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "can't replace view " + viewName);
            }
        } else {
            if (viewManager.select(viewName) != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "table '" + viewName + "' already exists ");
            }
            success = viewManager
                .insert(viewName, columnList, viewDefinition, executionContext.getConnection().getUser(), planString,
                    planType);
            if (!success) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "can't add view " + viewName);
            }
        }
    }

    @Override
    protected String remark() {
        return "|viewDefinition: " + viewDefinition;
    }

}
