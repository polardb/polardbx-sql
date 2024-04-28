package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@TaskName(name = "CdcLogicalSequenceMarkTask")
@Getter
@Setter
public class CdcLogicalSequenceMarkTask extends BaseDdlTask {
    private SqlKind sqlKind;
    private String sequenceName;
    private String ddlSql;

    @JSONCreator
    public CdcLogicalSequenceMarkTask(String schemaName, String sequenceName, String ddlSql, SqlKind sqlKind) {
        super(schemaName);
        this.sqlKind = sqlKind;
        this.sequenceName = sequenceName;
        this.ddlSql = ddlSql;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        mark4LogicalSequence(executionContext, sqlKind, schemaName, sequenceName, ddlSql);
    }

    private void mark4LogicalSequence(ExecutionContext executionContext, SqlKind sqlKind, String schemaName,
                                      String sequenceName, String ddlSql) {
        CdcManagerHelper.getInstance()
            .notifyDdlNew(
                schemaName,
                sequenceName,
                sqlKind.name(),
                ddlSql,
                DdlType.UNSUPPORTED,
                null,
                null,
                CdcDdlMarkVisibility.Protected,
                buildExtendParameter(executionContext));
    }

}
