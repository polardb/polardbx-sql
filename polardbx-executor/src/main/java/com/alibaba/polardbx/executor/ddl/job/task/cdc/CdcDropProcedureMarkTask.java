package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.common.cdc.ICdcManager;
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
@TaskName(name = "CdcDropProcedureMarkTask")
@Getter
@Setter
public class CdcDropProcedureMarkTask extends BaseDdlTask {

    private final String procedureName;

    @JSONCreator
    public CdcDropProcedureMarkTask(String schemaName, String procedureName) {
        super(schemaName);
        this.procedureName = procedureName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        Map<String, Object> param = buildExtendParameter(executionContext);
        param.put(ICdcManager.CDC_DDL_SCOPE, DdlScope.Schema);

        CdcManagerHelper.getInstance()
            .notifyDdlNew(
                schemaName,
                procedureName,
                SqlKind.DROP_PROCEDURE.name(),
                ddlContext.getDdlStmt(),
                ddlContext.getDdlType(),
                ddlContext.getJobId(),
                getTaskId(),
                CdcDdlMarkVisibility.Protected,
                param);
    }
}
