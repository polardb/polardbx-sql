package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.AddPartsForTtlTblTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlAlterPartsTaskLogInfo;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.apachecommons.CommonsLog;

import java.sql.Connection;

/**
 * @author chenghui.lch
 */
@CommonsLog
@TaskName(name = "CheckAndPrepareAddPartsForTtlTblSqlTask")
@Getter
@Setter
public class CheckAndPrepareAddPartsForTtlTblSqlTask extends AbstractTtlJobTask {

    @JSONCreator
    public CheckAndPrepareAddPartsForTtlTblSqlTask(String schemaName, String logicalTableName) {
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
        int prePartCnt = ttlDefinitionInfo.getTtlInfoRecord().getArcPrePartCnt();
        TableMeta ttlTblMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
        Integer arcPartInterval = ttlDefinitionInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getArcPartUnit());
        TtlTimeUnit ttlTimeUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getTtlUnit());
        Integer ttlInterval = ttlDefinitionInfo.getTtlInfoRecord().getTtlInterval();
        TtlArchiveKind archiveKind = TtlArchiveKind.of(ttlDefinitionInfo.getTtlInfoRecord().getArcKind());
        PartKeyLevel tarPartLevel = PartKeyLevel.PARTITION_KEY;
        if (archiveKind == TtlArchiveKind.SUBPARTITION) {
            tarPartLevel = PartKeyLevel.SUBPARTITION_KEY;
        }
        String formatedCurrentDatetime = this.jobContext.getCurrentDateTimeFormatedByArcPartUnit();
        String targetPivotPointValStr = formatedCurrentDatetime;

        int finalPreBuildPartCnt =
            TtlJobUtil.decidePreBuiltPartCnt(prePartCnt, ttlInterval, ttlTimeUnit, arcPartInterval, arcPartUnit);
        boolean useExpireOver = ttlDefinitionInfo.useExpireOverPartitionsPolicy();
        if (useExpireOver) {
            targetPivotPointValStr =
                TtlPartitionUtil.findMaxPartBoundValStrFromNonMaxValParts(ttlTblMeta.getPartitionInfo(), tarPartLevel,
                    ttlDefinitionInfo);
            PartitionByDefinition tarPartBy = TtlPartitionUtil.getTargetPartBy(ttlTblMeta, tarPartLevel);
            int partCnt = tarPartBy.getPartitions().size();
            int expireOverCnt = prePartCnt;
            finalPreBuildPartCnt =
                TtlPartitionUtil.decideAddPartsCountForTtlTblWithExpiredOverPolicy(partCnt, expireOverCnt);
        }

        TtlPartitionUtil.BuildPrePartCalcResult calcResult = null;
        try {
            TtlPartitionUtil.BuildPrePartCalcParams params = new TtlPartitionUtil.BuildPrePartCalcParams();
            params.setTtlInfo(ttlDefinitionInfo);
            params.setBuildForCci(false);
            params.setTarTblMeta(ttlTblMeta);
            params.setTargetPartLevel(tarPartLevel);
            params.setPivotPointValStr(targetPivotPointValStr);
            params.setPreBuildNewPartCount(finalPreBuildPartCnt);
            params.setExpiredByOverPartCount(useExpireOver);
            params.setEc(executionContext);
            calcResult = TtlPartitionUtil.calcBuildPrePartSpecs(params);
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex);
        }

        if (calcResult != null && calcResult.needAddParts()) {
            /**
             * Build add part sql of ttl-tbl
             */
            String queryHintForAlterTableAddParts = TtlConfigUtil.getQueryHintForAutoAddParts();
            String addPartsSqlForTtlTbl = calcResult.generateAddPartsSql(queryHintForAlterTableAddParts);
            this.jobContext.setNeedAddPartsForTtlTbl(true);
            this.jobContext.setTtlTblNewMaxBoundValAfterAddingNewParts(
                calcResult.getNewMaxBoundValueAfterAddingParts());
            this.jobContext.setTtlTblAddPartsSql(addPartsSqlForTtlTbl);
            this.jobContext.setNewAddedPartsCount(calcResult.getNewAddPartSpecInfos().size());
        } else {
            this.jobContext.setNeedAddPartsForTtlTbl(false);
        }

        AddPartsForTtlTblTaskLogInfo logInfo = new AddPartsForTtlTblTaskLogInfo();
        TtlAlterPartsTaskLogInfo.initAlterPartsTaskLogInfo(logInfo, jobContext, executionContext);
        dynamicNotifyAddPartsSubJobTaskToExecIfNeed(executionContext, logInfo);
        logTaskExecResult(logInfo);
    }

    protected void dynamicNotifyAddPartsSubJobTaskToExecIfNeed(ExecutionContext executionContext,
                                                               AddPartsForTtlTblTaskLogInfo logInfo) {

        try (Connection metaDbConnection = MetaDbDataSource.getInstance().getConnection()) {
            boolean needAddPart = false;
            if (this.jobContext.getNeedAddPartsForTtlTbl()) {
                needAddPart = true;
            }
            dynamicNotifyAddPartsSubJobTaskToExecIfNeed(metaDbConnection, needAddPart, logInfo);
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
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(),
            PreparingFormattedCurrDatetimeTask.class,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

    protected void dynamicNotifyAddPartsSubJobTaskToExecIfNeed(Connection metaDbConn,
                                                               boolean needAddParts,
                                                               AddPartsForTtlTblTaskLogInfo logInfo) {
        String oldDdlStmt = "";
        Long newSubJobId = 0L;
        String newDdlStmt = "";
        String newRollbackStmt = "";

        oldDdlStmt = TtlTaskSqlBuilder.buildSubJobTaskNameForAddPartsFroTtlTblBySpecifySubJobStmt();
        if (needAddParts) {
            newDdlStmt = this.jobContext.getTtlTblAddPartsSql();
        } else {
            newSubJobId = DdlConstants.TRANSIENT_SUB_JOB_ID;
        }

        logInfo.needPerformSubJobTaskDdl = needAddParts;
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

    protected void logTaskExecResult(AddPartsForTtlTblTaskLogInfo logInfo) {
        TtlLoggerUtil.logTaskExecResult(this, this.jobContext, logInfo);
    }
}
