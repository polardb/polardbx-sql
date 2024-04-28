package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-28 18:52
 **/
@TaskName(name = "CdcCreateViewMarkTask")
@Getter
@Setter
public class CdcCreateViewMarkTask extends BaseDdlTask {
    private final String viewName;
    private final Boolean isAlter;

    @JSONCreator
    public CdcCreateViewMarkTask(String schemaName, String viewName, Boolean isAlter) {
        super(schemaName);
        this.viewName = viewName;
        this.isAlter = isAlter;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        Map<String, Object> param = buildExtendParameter(executionContext);
        param.put(ICdcManager.CDC_DDL_SCOPE, DdlScope.Schema);

        if (isAlter) {
            CdcManagerHelper.getInstance()
                .notifyDdlNew(
                    schemaName,
                    viewName,
                    SqlKind.ALTER_VIEW.name(),
                    ddlContext.getDdlStmt(),
                    DdlType.ALTER_VIEW,
                    ddlContext.getJobId(),
                    getTaskId(),
                    CdcDdlMarkVisibility.Protected,
                    buildExtendParameter(executionContext));
        } else {
            CdcManagerHelper.getInstance()
                .notifyDdlNew(
                    schemaName,
                    viewName,
                    SqlKind.CREATE_VIEW.name(),
                    ddlContext.getDdlStmt(),
                    DdlType.CREATE_VIEW,
                    ddlContext.getJobId(),
                    getTaskId(),
                    CdcDdlMarkVisibility.Protected,
                    buildExtendParameter(executionContext));
        }
    }

    @Override
    protected String remark() {
        return "|SqlKind: " + (isAlter ? SqlKind.ALTER_VIEW.name() : SqlKind.CREATE_VIEW.name());
    }
}
