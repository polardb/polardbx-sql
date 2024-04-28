package com.alibaba.polardbx.executor.ddl.job.task.twophase;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@TaskName(name = "FinishTwoPhaseDdlTask")
@Getter
public class FinishTwoPhaseDdlTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long twoPhaseDdlId;

    final private String sqlTemplate;

    @JSONCreator
    public FinishTwoPhaseDdlTask(String schemaName, String logicalTableName,
                                 Map<String, Set<String>> sourcePhyTableNames,
                                 String sqlTemplate,
                                 ComplexTaskMetaManager.ComplexTaskType taskType,
                                 Long twoPhaseDdlId
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sqlTemplate = sqlTemplate;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.taskType = taskType;
        this.twoPhaseDdlId = twoPhaseDdlId;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            twoPhaseDdlManager =
                new TwoPhaseDdlManager(schemaName, logicalTableName, sqlTemplate, sourcePhyTableNames, twoPhaseDdlId);
            twoPhaseDdlManager.setJobId(jobId);
        }

        // should be idempotent
        try {
            twoPhaseDdlManager.twoPhaseDdlFinish(
                schemaName,
                logicalTableName,
                executionContext
            );
        } catch (RuntimeException | InterruptedException | ExecutionException exception) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_DDL_JOB_ERROR, exception.getMessage()
            );
            //TODO, process exception.
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            String.format("We don't support rollback in this phase, please continue"));
    }

    public static String getTaskName() {
        return "FinishTwoPhaseDdlTask";
    }

    @Override
    public String remark() {
        return "|finish TwoPhaseDdl, tableName: " + logicalTableName;
    }
}
