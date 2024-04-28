package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.PolarDbXSystemTableView;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "DropViewRemoveMetaTask")
public class DropViewRemoveMetaTask extends BaseDdlTask {

    protected String viewName;

    @JSONCreator
    public DropViewRemoveMetaTask(String schemaName,
                                  String viewName) {
        super(schemaName);
        this.viewName = viewName;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        boolean success = OptimizerContext.getContext(schemaName).getViewManager().delete(viewName);

        if (!success) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "drop view " + viewName + " fail for " + PolarDbXSystemTableView.TABLE_NAME);
        }
    }
}
