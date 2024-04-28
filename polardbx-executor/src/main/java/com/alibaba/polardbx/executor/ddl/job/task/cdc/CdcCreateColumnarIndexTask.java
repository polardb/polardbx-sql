package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CciSchemaEvolutionTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

@Getter
@TaskName(name = "CdcCreateColumnarIndexTask")
public class CdcCreateColumnarIndexTask extends BaseDdlTask {
    private final String logicalTableName;
    private final String createIndexSql;
    private final String columnarIndexTableName;
    private final String originIndexName;
    private final Long versionId;
    private final CciSchemaEvolutionTask cciSchemaEvolutionTask;

    public CdcCreateColumnarIndexTask(String schemaName, String logicalTableName, String columnarIndexTableName,
                                      String originIndexName, Map<String, String> options,
                                      String createIndexSql, Long versionId) {
        this(schemaName,
            logicalTableName,
            columnarIndexTableName,
            originIndexName,
            createIndexSql,
            versionId,
            CciSchemaEvolutionTask.createCci(schemaName,
                logicalTableName,
                columnarIndexTableName,
                options,
                versionId));
    }

    @JSONCreator
    private CdcCreateColumnarIndexTask(String schemaName, String logicalTableName, String columnarIndexTableName,
                                       String originIndexName, String createIndexSql, Long versionId,
                                       CciSchemaEvolutionTask cciSchemaEvolutionTask) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.createIndexSql = createIndexSql;
        this.columnarIndexTableName = columnarIndexTableName;
        this.originIndexName = originIndexName;
        this.versionId = versionId;
        this.cciSchemaEvolutionTask = cciSchemaEvolutionTask;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        cciSchemaEvolutionTask.duringTransaction(jobId, metaDbConnection, executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        String markSql = CdcMarkUtil.buildVersionIdHint(versionId) + createIndexSql;
        CdcMarkUtil.useDdlVersionId(executionContext, versionId);
        CdcMarkUtil.useOriginalDDL(executionContext);
        final Map<String, Object> extParam = buildExtendParameter(executionContext, createIndexSql);
        // Set TASK_MARK_SEQ=0, so that CdcDdlMark for rollback task, with same jobId and taskId,
        // will not be ignored in com.alibaba.polardbx.cdc.CdcManager.recordDdl
        extParam.put(ICdcManager.TASK_MARK_SEQ, 1);

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, SqlKind.CREATE_INDEX.name(),
                markSql, DdlType.CREATE_INDEX, ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Protected, extParam);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        cciSchemaEvolutionTask.duringRollbackTransaction(jobId, metaDbConnection, executionContext);

        // DDL_ID for ext will be added in front of sql in CdcMarkUtil.buildExtendParameter()
        final String rollbackSql = "DROP INDEX `" + originIndexName + "` ON `" + logicalTableName + "`";
        final String markSql = CdcMarkUtil.buildVersionIdHint(versionId) + rollbackSql;
        CdcMarkUtil.useDdlVersionId(executionContext, versionId);
        CdcMarkUtil.useOriginalDDL(executionContext);
        final Map<String, Object> extParam = buildExtendParameter(executionContext, rollbackSql);
        // Set TASK_MARK_SEQ=1, so that CdcDdlMark for rollback task will not be ignored
        // in com.alibaba.polardbx.cdc.CdcManager.recordDdl
        extParam.put(ICdcManager.TASK_MARK_SEQ, 2);

        final DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, SqlKind.DROP_INDEX.name(),
                markSql, DdlType.DROP_INDEX, ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Protected, extParam);
    }

    @Override
    protected String remark() {
        return String.format("|ddlVersionId: %s |sql: %s", versionId, createIndexSql);
    }
}
