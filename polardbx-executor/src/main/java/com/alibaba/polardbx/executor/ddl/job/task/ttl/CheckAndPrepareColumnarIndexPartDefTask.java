package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
@TaskName(name = "CheckAndPrepareColumnarIndexPartDefTask")
@Getter
@Setter
public class CheckAndPrepareColumnarIndexPartDefTask extends AbstractTtlJobTask {

    protected String arcTblSchema;
    protected String arcTblName;

    @JSONCreator
    public CheckAndPrepareColumnarIndexPartDefTask(String schemaName, String logicalTableName,
                                                   String arcTblSchema, String arcTblName) {
        super(schemaName, logicalTableName);
        this.arcTblSchema = arcTblSchema;
        this.arcTblName = arcTblName;
        onExceptionTryRecoveryThenPause();

    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        executeInner(executionContext);
    }

    protected void executeInner(ExecutionContext executionContext) {

        String cleanUpLowerBound = this.jobContext.getCleanUpLowerBound();
        String cleanUpUpperBound = this.jobContext.getCleanUpUpperBound();
        String formatedCurrentTime = this.jobContext.getFormatedCurrentDateTime();

        TtlDefinitionInfo ttlDefinitionInfo = this.jobContext.getTtlInfo();
        String ttlTimeZone = ttlDefinitionInfo.getTtlInfoRecord().getTtlTimezone();

        int preBuiltPartCntForFuture = this.jobContext.getTtlInfo().getTtlInfoRecord().getArcPrePartCnt();
        int postBuiltPartCntForPast = this.jobContext.getTtlInfo().getTtlInfoRecord().getArcPostPartCnt();

        /**
         * Gen the range bound list by cleanUpLowerBound 、cleanUpUpperBound adn prePartCnt
         */
        List<String> outputPartBoundList = new ArrayList<>();
        List<String> outputPartPartList = new ArrayList<>();

        Integer ttlInterval = ttlDefinitionInfo.getTtlInfoRecord().getTtlInterval();
        TtlTimeUnit ttlUnitVal = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getTtlUnit());

        Integer arcPartMode = ttlDefinitionInfo.getTtlInfoRecord().getArcPartMode();
        Integer arcPartInterval = ttlDefinitionInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getArcPartUnit());

        int finalPreBuildPartCnt =
            TtlJobUtil.decidePreBuiltPartCnt(preBuiltPartCntForFuture, ttlInterval, ttlUnitVal, arcPartInterval,
                arcPartUnit);

        TtlJobUtil.genRangeBoundListByInterval(
            cleanUpLowerBound,
            formatedCurrentTime,
            finalPreBuildPartCnt,
            postBuiltPartCntForPast,
            ttlTimeZone,
            arcPartMode,
            arcPartInterval,
            arcPartUnit,
            outputPartBoundList,
            outputPartPartList);

        List<String> allPartBndValsToBeAdded = new ArrayList<>();
        List<String> allPartNamesToBeAdded = new ArrayList<>();

        /**
         * Put the last bound value into the list of allPartBndValsToBeAdded
         */
        allPartBndValsToBeAdded.addAll(outputPartBoundList);
        allPartNamesToBeAdded.addAll(outputPartPartList);

//        boolean needAutoAddMaxValPart = TtlConfigUtil.isAutoAddMaxValuePartForCci();
        Boolean needAutoAddMaxValPart = executionContext.getParamManager().getBoolean(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING);
        if (needAutoAddMaxValPart == null) {
            needAutoAddMaxValPart = TtlConfigUtil.isAutoAddMaxValuePartForCci();
        }
        if (needAutoAddMaxValPart) {
            allPartBndValsToBeAdded.add(TtlTaskSqlBuilder.ARC_TBL_MAXVALUE_BOUND);
            allPartNamesToBeAdded.add(TtlTaskSqlBuilder.ARC_TBL_MAXVAL_PART_NAME);
        }

        /**
         * Build completed partByDef of columnar ci
         */
        String createCiSqlForArcTbl =
            TtlTaskSqlBuilder.buildCreateColumnarIndexSqlForArcTbl(ttlDefinitionInfo, arcTblSchema, arcTblName,
                allPartBndValsToBeAdded,
                allPartNamesToBeAdded, executionContext);
        this.jobContext.setCreateColumnarIndexSqlForArcTbl(createCiSqlForArcTbl);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);
        dynamicModifyCreateColumnarIndexSubJobTaskStmt(metaDbConnection);
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
            PrepareCleanupIntervalTask.class,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

    protected void dynamicModifyCreateColumnarIndexSubJobTaskStmt(Connection metaDbConn) {
        String oldDdlStmt = "";
        Long newSubJobId = 0L;
        String newDdlStmt = "";
        String newRollbackStmt = "";

        oldDdlStmt = TtlTaskSqlBuilder.buildSubJobTaskNameForCreateColumnarIndexBySpecifySubjobStmt();
        newDdlStmt = this.jobContext.getCreateColumnarIndexSqlForArcTbl();

        /**
         * Update jobid and ddl-stmt for the subjob
         */
        TtlJobUtil.updateSubJobTaskIdAndStmtByJobIdAndOldStmt(this.jobId, this.schemaName, this.logicalTableName,
            oldDdlStmt,
            newSubJobId, newDdlStmt, newRollbackStmt, metaDbConn);
    }

    @Override
    public TtlJobContext getJobContext() {
        return this.jobContext;
    }

    @Override
    public void setJobContext(TtlJobContext jobContext) {
        this.jobContext = jobContext;
    }
}
