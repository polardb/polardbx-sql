package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CheckGsiTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlSelect;

import java.util.Map;

@TaskName(name = "CreateGsiCheckTask")
@Getter
public class CreateGsiCheckTask extends BaseBackfillTask {
    final private String logicalTableName;
    final private String indexTableName;
    public Map<String, String> virtualColumns;
    public Map<String, String> backfillColumnMap;

    @JSONCreator
    public CreateGsiCheckTask(String schemaName, String logicalTableName, String indexTableName,
                              Map<String, String> virtualColumns, Map<String, String> backfillColumnMap) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexTableName = indexTableName;
        this.virtualColumns = virtualColumns;
        this.backfillColumnMap = backfillColumnMap;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        final boolean skipCheck =
            executionContext.getParamManager().getBoolean(ConnectionParams.SKIP_CHANGE_SET_CHECKER);

        if (!skipCheck) {
            String lockMode = SqlSelect.LockMode.UNDEF.toString();
            GsiChecker.Params params = GsiChecker.Params.buildFromExecutionContext(executionContext);
            boolean isPrimaryBroadCast =
                OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(logicalTableName);
            boolean isGsiBroadCast =
                OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(indexTableName);
            CheckGsiTask checkTask =
                new CheckGsiTask(schemaName, logicalTableName, indexTableName, lockMode, lockMode, params, false, "",
                    isPrimaryBroadCast, isGsiBroadCast, virtualColumns, backfillColumnMap);
            checkTask.checkInBackfill(executionContext);
        }
    }
}