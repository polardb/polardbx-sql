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

@TaskName(name = "CommitTwoPhaseDdlTask")
@Getter
public class CommitTwoPhaseDdlTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long twoPhaseDdlId;
    final private String sqlTemplate;
    final private int commitDelay;

    @JSONCreator
    public CommitTwoPhaseDdlTask(String schemaName, String logicalTableName,
                                 Map<String, Set<String>> sourcePhyTableNames,
                                 ComplexTaskMetaManager.ComplexTaskType taskType,
                                 Long twoPhaseDdlId, String sqlTemplate,
                                 int commitDelay
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.taskType = taskType;
        this.twoPhaseDdlId = twoPhaseDdlId;
        this.sqlTemplate = sqlTemplate;
        this.commitDelay = commitDelay;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        //TODO(2pc-ddl)COMP, process exception.
        // even if CN HA, we should try our best to commit.
        // if partial failed, we would try to commit again.
        // if partial failed and commit again failed(DN restart/DN partition)
        // we would commit all that can be committed, and process inconsistency in further task.
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            twoPhaseDdlManager =
                new TwoPhaseDdlManager(schemaName, logicalTableName, sqlTemplate, sourcePhyTableNames, twoPhaseDdlId);
            twoPhaseDdlManager.setJobId(jobId);
//            Boolean allWaitCommitted = twoPhaseDdlManager.checkAllPhyDdlWaitCommitted();
//            if(allWaitCommitted) {
//                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
//                    "can't find twoPhaseDdlManager, HA may has happened! will rollback");
//            }
        }

        // should be idempotent
        try {
            updateSupportedCommands(true, false, null);
            Thread.sleep(commitDelay * 1000L);
            twoPhaseDdlManager.twoPhaseDdlCommit(
                schemaName,
                logicalTableName,
                executionContext
            );
        } catch (RuntimeException | InterruptedException | ExecutionException exception) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_DDL_JOB_ERROR, exception.getMessage()
            );
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
        return "CommitTwoPhaseDdlTask";
    }

    @Override
    public String remark() {
        return "|commit TwoPhaseDdl, tableName: " + logicalTableName;
    }
}
