package com.alibaba.polardbx.executor.ddl.job.task.twophase;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlManager;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.mysql.cj.x.protobuf.PolarxDatatypes;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import lombok.Getter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.REACHED_BARRIER;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.REACHED_BARRIER_RUNNING;
import static com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlData.RUNNING;

@TaskName(name = "WaitTwoPhaseDdlTask")
@Getter
public class WaitTwoPhaseDdlTask extends BasePhyDdlTask {
    final private String logicalTableName;
    final private Map<String, Set<String>> sourcePhyTableNames;
    final private ComplexTaskMetaManager.ComplexTaskType taskType;
    final private Long twoPhaseDdlId;

    final private String twoPhaseTaskName;
    final private int waitDelay;

    @JSONCreator
    public WaitTwoPhaseDdlTask(String schemaName, String logicalTableName,
                               Map<String, Set<String>> sourcePhyTableNames,
                               String twoPhaseTaskName,
                               ComplexTaskMetaManager.ComplexTaskType taskType,
                               Long twoPhaseDdlId,
                               int waitDelay
    ) {
        super(schemaName, null);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.twoPhaseTaskName = twoPhaseTaskName;
        this.taskType = taskType;
        this.twoPhaseDdlId = twoPhaseDdlId;
        this.waitDelay = waitDelay;
        onExceptionTryRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setSchemaName(schemaName);

        TwoPhaseDdlManager twoPhaseDdlManager = TwoPhaseDdlManager.globalTwoPhaseDdlManagerMap.get(twoPhaseDdlId);
        if (twoPhaseDdlManager == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "can't find twoPhaseDdlManager, HA may has happened! will rollback");
        }

        Set<String> expectedPhyDdlState =
            new HashSet<>(Arrays.asList(RUNNING, REACHED_BARRIER, REACHED_BARRIER_RUNNING));
        // should be idempotent

        int status;
        try {
            if (waitDelay > 0) {
                Thread.sleep(waitDelay * 1000L);
            }
            status = twoPhaseDdlManager.twoPhaseDdlWait(
                schemaName,
                logicalTableName,
                twoPhaseTaskName,
                expectedPhyDdlState,
                executionContext
            );
        } catch (RuntimeException | InterruptedException | ExecutionException exception) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_DDL_JOB_ERROR, exception.getMessage()
            );
            //TODO, process exception.
        }
        if (status == 0) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_DDL_JOB_ERROR,
                "there are some physical ddl failed! maybe self killing multiple-phase ddl or other unknown causes!"
            );
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
    }

    public static String getTaskName() {
        return "WaitTwoPhaseDdlTask";
    }

    @Override
    public String remark() {
        return "|wait TwoPhaseDdl, tableName: " + logicalTableName;
    }
}
