package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.DropPartsForTtlTblTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlAlterPartsTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.apachecommons.CommonsLog;

import java.sql.Connection;

/**
 * @author chenghui.lch
 */
@CommonsLog
@TaskName(name = "CheckAndPrepareDropPartsForTtlTblSqlTask")
@Getter
@Setter
public class CheckAndPrepareDropPartsForTtlTblSqlTask extends AbstractTtlJobTask {

    protected boolean archiveByPartitions = false;

    @JSONCreator
    public CheckAndPrepareDropPartsForTtlTblSqlTask(String schemaName, String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        executeInner(executionContext);
    }

    protected void executeInner(ExecutionContext executionContext) {

        TtlDefinitionInfo ttlDefinitionInfo = this.jobContext.getTtlInfo();
        PartKeyLevel tarPartLevel = PartKeyLevel.PARTITION_KEY;
        if (ttlDefinitionInfo.performArchiveBySubPartition()) {
            tarPartLevel = PartKeyLevel.SUBPARTITION_KEY;
        }

        String cleanUpUpperBoundStr = this.jobContext.getCleanUpUpperBound();
        Integer newAddedPartsCount = this.jobContext.getNewAddedPartsCount();
        TtlPartitionUtil.CleanupPostPartsCalcResult calcResult = null;
        try {
            TtlPartitionUtil.CleanupPostPartsCalcParams params = new TtlPartitionUtil.CleanupPostPartsCalcParams();

            params.setTtlInfo(ttlDefinitionInfo);
            params.setCleanupPostPastForCci(false);
            params.setTargetPartLevel(tarPartLevel);
            params.setCleanupUpperBoundDatetimeStr(cleanUpUpperBoundStr);
            params.setNewAddPartsCount(newAddedPartsCount);
            params.setEc(executionContext);
            calcResult = TtlPartitionUtil.calcCleanupPostParts(params);
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex);
        }

        if (calcResult != null && calcResult.needDropParts()) {
            /**
             * Build add part sql of ttl-tbl
             */
            String queryHintForAlterTableDropParts = TtlConfigUtil.getQueryHintForAutoDropParts();
            this.jobContext.setNeedDropPartsForTtlTbl(true);
            String dropPartsForTtlTbl = calcResult.generateDropPartsStmtSql(queryHintForAlterTableDropParts);
            this.jobContext.setTtlTblDropPartsSql(dropPartsForTtlTbl);
        } else {
            this.jobContext.setNeedAddPartsForTtlTbl(false);
        }
        DropPartsForTtlTblTaskLogInfo logInfo = new DropPartsForTtlTblTaskLogInfo();
        TtlAlterPartsTaskLogInfo.initAlterPartsTaskLogInfo(logInfo, this.jobContext, executionContext);
        dynamicNotifyDropPartsSubJobTaskToExecIfNeed(executionContext, logInfo);
        logTaskExecResult(this, this.jobContext, logInfo);
    }

    protected void dynamicNotifyDropPartsSubJobTaskToExecIfNeed(ExecutionContext executionContext,
                                                                DropPartsForTtlTblTaskLogInfo logInfo) {

        try (Connection metaDbConnection = MetaDbDataSource.getInstance().getConnection()) {
            boolean needDropPart = false;
            if (this.jobContext.getNeedDropPartsForTtlTbl()) {
                needDropPart = true;
            }
            dynamicNotifyDropPartsSubJobTaskToExecIfNeed(metaDbConnection, needDropPart, logInfo);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
            throw new TtlJobRuntimeException(ex);
        }
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        super.beforeRollbackTransaction(executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringRollbackTransaction(metaDbConnection, executionContext);
    }

    protected void fetchTtlJobContextFromPreviousTask() {
        Class previousTaskClass = PreparingFormattedCurrDatetimeTask.class;
        if (archiveByPartitions) {
            previousTaskClass = CheckAndPrepareAddPartsForTtlTblSqlTask.class;
        }
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(),
            previousTaskClass,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

    protected void dynamicNotifyDropPartsSubJobTaskToExecIfNeed(Connection metaDbConn,
                                                                boolean needDropParts,
                                                                DropPartsForTtlTblTaskLogInfo logInfo) {
        String oldDdlStmt = "";
        Long newSubJobId = 0L;
        String newDdlStmt = "";
        String newRollbackStmt = "";

        oldDdlStmt = TtlTaskSqlBuilder.buildSubJobTaskNameForDropPartsForTtlTblBySpecifySubJobStmt();
        if (needDropParts) {
            newDdlStmt = this.jobContext.getTtlTblDropPartsSql();
        } else {
            newSubJobId = DdlConstants.TRANSIENT_SUB_JOB_ID;
        }

        logInfo.needPerformSubJobTaskDdl = needDropParts;
        logInfo.subJobTaskDdlStmt = newDdlStmt;

        /**
         * Update jobid and ddl-stmt for the subjob
         */
        TtlJobUtil.updateSubJobTaskIdAndStmtByJobIdAndOldStmt(this.jobId, this.schemaName, this.logicalTableName,
            oldDdlStmt,
            newSubJobId, newDdlStmt,
            newRollbackStmt, metaDbConn);

    }

    @Override
    public TtlJobContext getJobContext() {
        return this.jobContext;
    }

    @Override
    public void setJobContext(TtlJobContext jobContext) {
        this.jobContext = jobContext;
    }

    protected void logTaskExecResult(DdlTask parentDdlTask,
                                     TtlJobContext jobContext,
                                     DropPartsForTtlTblTaskLogInfo logInfo) {
        logInfo.logTaskExecResult(parentDdlTask, jobContext);
    }

    public boolean isArchiveByPartitions() {
        return archiveByPartitions;
    }

    public void setArchiveByPartitions(boolean archiveByPartitions) {
        this.archiveByPartitions = archiveByPartitions;
    }
}
