package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

@Getter
@TaskName(name = "CdcDropColumnarIndexTask")
public class CdcDropColumnarIndexTask extends BaseDdlTask {
    private final String logicalTableName;
    private final String indexName;
    private final Long versionId;

    public CdcDropColumnarIndexTask(String schemaName, String logicalTableName, String indexName, Long versionId) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.versionId = versionId;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        SQLDropIndexStatement stmt = new SQLDropIndexStatement();
        stmt.setTableName(new SQLIdentifierExpr(logicalTableName));
        stmt.setIndexName(new SQLIdentifierExpr(indexName));

        String markSql = CdcMarkUtil.buildVersionIdHint(versionId) + stmt;
        CdcMarkUtil.useDdlVersionId(executionContext, versionId);
        CdcMarkUtil.useOriginalDDL(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, SqlKind.DROP_INDEX.name(),
                markSql, DdlType.DROP_INDEX, ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Protected, buildExtendParameter(executionContext));
    }
}
