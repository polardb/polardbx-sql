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

@TaskName(name = "PrepareTwoPhaseDdlTask")
@Getter
public class PrepareTwoPhaseDdlTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long twoPhaseDdlId;
    final private int prepareDelay;

    @JSONCreator
    public PrepareTwoPhaseDdlTask(String schemaName, String logicalTableName,
                                  Map<String, Set<String>> sourcePhyTableNames,
                                  ComplexTaskMetaManager.ComplexTaskType taskType,
                                  Long twoPhaseDdlId,
                                  int prepareDelay
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.taskType = taskType;
        this.twoPhaseDdlId = twoPhaseDdlId;
        this.prepareDelay = prepareDelay;
        onExceptionTryRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "can't find twoPhaseDdlManager, CN HA may has happened! will rollback");
        }

        // should be idempotent
        try {
            // TODO(2pc-ddl) COMPUSORY.
            // before setting prepared, we should first check if physical ddl running.
            Thread.sleep(prepareDelay * 1000L);
            twoPhaseDdlManager.twoPhaseDdlPrepare(
                schemaName,
                logicalTableName,
                executionContext
            );
        } catch (RuntimeException | InterruptedException | ExecutionException exception) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_DDL_JOB_ERROR, exception.getMessage()
            );
            //TODO(2pc-ddl) IMPROVEMENT, process exception.
            // for example, if ExecuionException or Interrupted Exception, we should retry.
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
    }

    public static String getTaskName() {
        return "PrepareTwoPhaseDdlTask";
    }

    @Override
    public String remark() {
        return "|prepare TwoPhaseDdl, tableName: " + logicalTableName;
    }
}
