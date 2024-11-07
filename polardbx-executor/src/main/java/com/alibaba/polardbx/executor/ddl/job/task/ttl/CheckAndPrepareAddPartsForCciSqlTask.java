package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.apachecommons.CommonsLog;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
@CommonsLog
@TaskName(name = "CheckAndPrepareAddPartsForCciSqlTask")
@Getter
@Setter
public class CheckAndPrepareAddPartsForCciSqlTask extends AbstractTtlJobTask {

    @JSONCreator
    public CheckAndPrepareAddPartsForCciSqlTask(String schemaName, String logicalTableName) {
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

        boolean needPerformArchiving = this.jobContext.getTtlInfo().needPerformExpiredDataArchivingByCci();
        if (!needPerformArchiving) {
            return;
        }

        AddPartsForCciTaskLogInfo logInfo = new AddPartsForCciTaskLogInfo();
        logInfo.ttlTblSchema = schemaName;
        logInfo.ttlTblName = logicalTableName;
        logInfo.jobStatInfo = TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(schemaName, logicalTableName);
        logInfo.globalStatInfo = TtlScheduledJobStatManager.getInstance().getGlobalTtlJobStatInfo();

        String cleanUpLowerBound = this.jobContext.getCleanUpLowerBound();
        String cleanUpUpperBound = this.jobContext.getCleanUpUpperBound();

        TtlDefinitionInfo ttlDefinitionInfo = this.jobContext.getTtlInfo();
        String ttlTimeZone = ttlDefinitionInfo.getTtlInfoRecord().getTtlTimezone();

        int prePartCnt = ttlDefinitionInfo.getTtlInfoRecord().getArcPrePartCnt();
        int postPartCnt = ttlDefinitionInfo.getTtlInfoRecord().getArcPostPartCnt();

        TableMeta cciMeta = getArchiveCciTableMeta(this.schemaName, this.logicalTableName, executionContext);
        TableMeta arcCciTableMeta = cciMeta;

        Integer arcPartMode = ttlDefinitionInfo.getTtlInfoRecord().getArcPartMode();
        Integer arcPartInterval = ttlDefinitionInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartUnit = TtlTimeUnit.of(ttlDefinitionInfo.getTtlInfoRecord().getArcPartUnit());
        TtlTimeUnit ttlTimeUnit = TtlTimeUnit.of(this.jobContext.getTtlInfo().getTtlInfoRecord().getTtlUnit());
        Integer ttlInterval = this.jobContext.getTtlInfo().getTtlInfoRecord().getTtlInterval();

        logInfo.arcTmpTblSchema = this.jobContext.getTtlInfo().getTmpTableSchema();
        logInfo.arcTmpTblName = this.jobContext.getTtlInfo().getTmpTableName();
        logInfo.arcCciFullTblName = cciMeta.getTableName();

        logInfo.ttlTimeUnit = ttlTimeUnit.getTimeUnitName();
        logInfo.ttlInterval = Long.valueOf(ttlInterval);
        logInfo.arcPartTimeUnit = arcPartUnit.getTimeUnitName();
        logInfo.arcPartInterval = Long.valueOf(arcPartInterval);

        String currentDataTime = this.jobContext.getCurrentDateTime();
        logInfo.currentDateTime = currentDataTime;
        String formatedCurrentDatetime = this.jobContext.getFormatedCurrentDateTime();
        logInfo.formatedCurrentDateTime = formatedCurrentDatetime;

        /**
         * Gen the range bound list by cleanUpLowerBound 、cleanUpUpperBound adn prePartCnt
         */
        List<String> outputPartBoundList = new ArrayList<>();
        List<String> outputPartPartList = new ArrayList<>();

        int finalPreBuildPartCnt =
            TtlJobUtil.decidePreBuiltPartCnt(prePartCnt, ttlInterval, ttlTimeUnit, arcPartInterval, arcPartUnit);
        logInfo.preAllocateCount = Long.valueOf(finalPreBuildPartCnt);

        TtlJobUtil.genRangeBoundListByInterval(cleanUpLowerBound,
            formatedCurrentDatetime, finalPreBuildPartCnt, postPartCnt, ttlTimeZone, arcPartMode, arcPartInterval,
            arcPartUnit,
            outputPartBoundList,
            outputPartPartList);

        /**
         * Check and find the-missing-parts-on-ttl-tmp range bounds
         */
        List<String> boundListOfMissingPartOnTtlTmp = new ArrayList<>();
        List<String> partNameListOfMissingPartOnTtlTmp = new ArrayList<>();
        String[] maxValPartNameArr = new String[1];
        maxValPartNameArr[0] = null;
        findAndExtraMissingPartitionList(executionContext,
            outputPartBoundList,
            outputPartPartList,
            arcCciTableMeta,
            ttlTimeZone,
            boundListOfMissingPartOnTtlTmp/*Output*/,
            partNameListOfMissingPartOnTtlTmp/*Output*/,
            maxValPartNameArr/*Output*/);

        if (!boundListOfMissingPartOnTtlTmp.isEmpty()) {

            List<String> allPartBndValsToBeAdded = new ArrayList<>();
            List<String> allPartNamesToBeAdded = new ArrayList<>();

            /**
             * Put the last bound value into the list of allPartBndValsToBeAdded
             */
            allPartBndValsToBeAdded.addAll(boundListOfMissingPartOnTtlTmp);
            allPartNamesToBeAdded.addAll(partNameListOfMissingPartOnTtlTmp);

            /**
             * Build add part sql
             */
            String queryHintForAlterTableAddParts = TtlConfigUtil.getQueryHintForAutoAddParts();
            String addPartSqlTemp =
                TtlTaskSqlBuilder.buildAddPartitionsSqlTemplate(ttlDefinitionInfo, allPartBndValsToBeAdded,
                    allPartNamesToBeAdded, maxValPartNameArr[0], queryHintForAlterTableAddParts, executionContext);
            String ttlTblSchema = ttlDefinitionInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlDefinitionInfo.getTtlInfoRecord().getTableName();
            String ttlTmpTblSchema = ttlDefinitionInfo.getTmpTableSchema();
            String ttlTmpTblName = ttlDefinitionInfo.getTmpTableName();

            String addPartsSqlForTtlTmpTbl =
                TtlTaskSqlBuilder.buildAddPartitionsByTemplateAndTableName(ttlTblSchema, ttlTblName, ttlTmpTblName,
                    addPartSqlTemp);
            this.jobContext.setNeedAddPartsForArcTmpTbl(true);
            this.jobContext.setArcTmpTblAddPartsSql(addPartsSqlForTtlTmpTbl);
            this.jobContext.setArcTblAddPartsSql("");
            logInfo.addNewPartsSql = addPartsSqlForTtlTmpTbl;

        } else {
            this.jobContext.setNeedAddPartsForArcTmpTbl(false);
        }
        dynamicNotifyAddPartsSubJobTaskToExecIfNeed(executionContext, logInfo);
        logTaskExecResult(logInfo);
    }

    private void findAndExtraMissingPartitionList(
        ExecutionContext executionContext,
        List<String> outputPartBoundList,
        List<String> outputPartPartList,
        TableMeta arcCciTableMeta,
        String ttlTimeZone,
        List<String> boundListOfMissingPartOnTtlTmp,
        List<String> partNameListOfMissingPartOnTtlTmp,
        String[] maxValPartNameArr) {
        int outputPartCnt = outputPartBoundList.size();

        /**
         * before checking, should exclude the last bound from the list of outputPartBoundList,
         * because the last bound value is upper bound with less than
         */
        PartitionInfo arcTmpTblPartInfo = arcCciTableMeta.getPartitionInfo();
        PartitionByDefinition partByOfArcTmpTbl = arcTmpTblPartInfo.getPartitionBy();
        String maxValPartNameVal = null;
        for (int i = 0; i < partByOfArcTmpTbl.getPartitions().size(); i++) {
            PartitionSpec p = partByOfArcTmpTbl.getPartitions().get(i);
            if (p.getBoundSpec().containMaxValues()) {
                maxValPartNameVal = p.getName();
                break;
            }
        }
        maxValPartNameArr[0] = maxValPartNameVal;
        for (int i = 0; i < outputPartCnt; i++) {
            String boundStr = outputPartBoundList.get(i);
            String partNameStr = outputPartPartList.get(i);

            String boundStrToCheckIfPartExists = boundStr;
            if (i > 0) {
                boundStrToCheckIfPartExists = outputPartBoundList.get(i - 1);
            }

            /**
             * Find the target partName by routing the boundValStr
             */
            String targetPartName =
                routeFirstPartPartByTtlColValue(executionContext, arcCciTableMeta, ttlTimeZone,
                    boundStrToCheckIfPartExists);
            if (StringUtils.isEmpty(targetPartName)) {

                /**
                 * Maybe the missing-partition-routing boundStr is the bound value of the last partName which
                 * has existed in partInfo,
                 * so here need exclude the last partName
                 */
                boolean partNameExists = arcTmpTblPartInfo.getPartSpecSearcher().checkIfDuplicated(partNameStr);
                if (partNameExists) {
                    continue;
                }

                boundListOfMissingPartOnTtlTmp.add(boundStr);
                partNameListOfMissingPartOnTtlTmp.add(partNameStr);
            }
        }
    }

    protected void dynamicNotifyAddPartsSubJobTaskToExecIfNeed(ExecutionContext executionContext,
                                                               AddPartsForCciTaskLogInfo logInfo) {

        try (Connection metaDbConnection = MetaDbDataSource.getInstance().getConnection()) {
            boolean needAddPart = false;
            if (this.jobContext.getNeedAddPartsForArcTmpTbl()) {
                needAddPart = true;
            }
            dynamicNotifyAddPartsSubJobTaskToExecIfNeed(metaDbConnection, needAddPart, logInfo);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
            throw new TtlJobRuntimeException(ex);
        }
    }

    protected String routeFirstPartPartByTtlColValue(ExecutionContext ec,
                                                     TableMeta ttlTmpMeta,
                                                     String ttlTimeZone,
                                                     String ttlColVal) {
        List<PhysicalPartitionInfo> phyParts =
            TtlJobUtil.routeTargetPartsOfTtlTmpTblByTtlColVal(ec, ttlTmpMeta, ttlTimeZone, ttlColVal);
        if (phyParts.isEmpty()) {
            return null;
        }
        String partNameRs = phyParts.get(0).getPartName();
        PartitionInfo arcTmpTblPartInfo = ttlTmpMeta.getPartitionInfo();
        PartitionSpec partSpecRs = arcTmpTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(partNameRs);
        if (partSpecRs != null && partSpecRs.getBoundSpec().containMaxValues()) {
            return null;
        }
        return partNameRs;
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
            PrepareCleanupIntervalTask.class,
            getSchemaName(),
            this.logicalTableName
        );
        this.jobContext = jobContext;
    }

    protected void dynamicNotifyAddPartsSubJobTaskToExecIfNeed(Connection metaDbConn,
                                                               boolean needAddParts,
                                                               AddPartsForCciTaskLogInfo logInfo) {
        String oldDdlStmt = "";
        Long newSubJobId = 0L;
        String newDdlStmt = "";
        String newRollbackStmt = "";

        oldDdlStmt = TtlTaskSqlBuilder.buildSubJobTaskNameForAddPartsFroActTmpCciBySpecifySubjobStmt();
        if (needAddParts) {
            newDdlStmt = this.jobContext.getArcTmpTblAddPartsSql();
        } else {
            newSubJobId = DdlConstants.TRANSIENT_SUB_JOB_ID;
        }

        boolean findOptiTblDdlRunning = true;
        if (needAddParts) {
            /**
             * Check if ttl-table running optimize table
             */
            String ttlTblSchema = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
            String ttlTblName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
            Long[] ddlStmtJobIdOutput = new Long[1];
            String[] ddlStmtOutput = new String[1];
            Boolean[] ddlStmtFromTtlJob = new Boolean[1];
            try {
                findOptiTblDdlRunning =
                    CheckAndPerformingOptiTtlTableTask.checkIfOptiTblDdlExecuting(metaDbConn, ddlStmtJobIdOutput,
                        ddlStmtOutput, ddlStmtFromTtlJob, ttlTblSchema, ttlTblName);
            } catch (Throwable ex) {
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
            }
            if (findOptiTblDdlRunning) {
                /**
                 * If find the ttl-table is running optimize-table job,
                 * then ignore the add parts for ttl-table
                 *
                 */
                newDdlStmt = "";
                newSubJobId = DdlConstants.TRANSIENT_SUB_JOB_ID;
                TtlLoggerUtil.TTL_TASK_LOGGER.warn(String.format(
                    "found optimize-table job running on ttlTable[%s.%s], so currJob [%s] ignore adding parts for cci",
                    schemaName, ttlTblSchema, ttlTblName, jobId));
            }
        }

        logInfo.needAddNewParts = needAddParts;
        logInfo.foundOptiTblDdlRunning = findOptiTblDdlRunning;
        logInfo.addNewPartsSql = newDdlStmt;

        /**
         * Update jobid and ddl-stmt for the subjob
         */
        TtlJobUtil.updateSubJobTaskIdAndStmtByJobIdAndOldStmt(this.jobId, this.schemaName, this.logicalTableName,
            oldDdlStmt,
            newSubJobId, newDdlStmt,
            newRollbackStmt, metaDbConn);

    }

    protected TableMeta getArchiveCciTableMeta(String ttlTblSchema, String ttlTblName,
                                               ExecutionContext executionContext) {
        TableMeta ttlTblMeta = executionContext.getSchemaManager(ttlTblSchema).getTable(ttlTblName);
        String tblNameOfArcCci = TtlJobUtil.getActualTableNameForArcCci(ttlTblMeta, executionContext);
        String arcTblSchema = ttlTblMeta.getTtlDefinitionInfo().getArchiveTableSchema();
        TableMeta cciTblMeta = executionContext.getSchemaManager(arcTblSchema).getTable(tblNameOfArcCci);
        return cciTblMeta;
    }

    @Override
    public TtlJobContext getJobContext() {
        return this.jobContext;
    }

    @Override
    public void setJobContext(TtlJobContext jobContext) {
        this.jobContext = jobContext;
    }

    protected static class AddPartsForCciTaskLogInfo {

        public AddPartsForCciTaskLogInfo() {
        }

        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo;
        TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo;

        protected Long jobId = 0L;
        protected Long taskId = 0L;
        protected String ttlTblSchema = "";
        protected String ttlTblName = "";
        protected String arcTmpTblSchema = "";
        protected String arcTmpTblName = "";
        protected String arcCciFullTblName = "";

        protected String ttlTimeUnit = "";
        protected Long ttlInterval = 0L;
        protected String arcPartTimeUnit = "";
        protected Long arcPartInterval = 0L;
        protected Long preAllocateCount = 0L;

        protected String currentDateTime = "";
        protected String formatedCurrentDateTime = "";
        protected String currMaxBoundVal = "";
        protected String currMaxBoundPartName = "";
        protected String addNewPartsSql = "";

        protected boolean needAddNewParts = false;
        protected boolean foundOptiTblDdlRunning = false;
        protected boolean ignoreAddPartsCurrRound = false;

    }

    protected void logTaskExecResult(CheckAndPrepareAddPartsForCciSqlTask.AddPartsForCciTaskLogInfo logInfo) {

        try {

            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = logInfo.jobStatInfo;
            TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo = logInfo.globalStatInfo;

            String logMsg = "";
            String msgPrefix = TtlLoggerUtil.buildTaskLogRowMsgPrefix(jobId, taskId, "AddNewPartsForArcTable");
            logMsg += String.format("ttlTblSchema: %s, ttlTblName: %s\n", logInfo.ttlTblSchema, logInfo.ttlTblName);
            logMsg += String.format("arcTmpTblSchema: %s, arcTmpTblName: %s\n", logInfo.arcTmpTblSchema,
                logInfo.arcTmpTblName);
            logMsg += String.format("cciFullTable: %s\n", logInfo.arcCciFullTblName);

            logMsg += msgPrefix;
            logMsg += String.format("ttlInterval: %s, ttlTimeUnit: %s\n", logInfo.ttlInterval, logInfo.ttlTimeUnit);

            logMsg += msgPrefix;
            logMsg += String.format("arcPartInterval: %s, arcPartTimeUnit: %s\n", logInfo.arcPartInterval,
                logInfo.arcPartTimeUnit);

            logMsg += msgPrefix;
            logMsg += String.format("preAllocateCount: %s\n", logInfo.preAllocateCount);

            logMsg += msgPrefix;
            logMsg += String.format("currentDateTime: %s\n", logInfo.currentDateTime);
            logMsg += msgPrefix;
            logMsg += String.format("formatedCurrentDateTime: %s\n", logInfo.formatedCurrentDateTime);
            logMsg += msgPrefix;
            logMsg += String.format("currMaxBoundVal: %s\n", logInfo.currMaxBoundVal);
            logMsg += msgPrefix;
            logMsg += String.format("addNewPartsSql: %s\n", logInfo.addNewPartsSql);

            logMsg += msgPrefix;
            logMsg += String.format("needAddParts: %s\n", logInfo.needAddNewParts);
            logMsg += msgPrefix;
            logMsg += String.format("foundOptiTblDdlRunning: %s\n", logInfo.foundOptiTblDdlRunning);
            logMsg += msgPrefix;
            logMsg += String.format("ignoreAddPartsCurrRound: %s\n", logInfo.ignoreAddPartsCurrRound);

            if (logInfo.needAddNewParts && !logInfo.ignoreAddPartsCurrRound) {
                jobStatInfo.getAddPartSqlCount().addAndGet(1);
                globalStatInfo.getTotalAddPartSqlCount().addAndGet(1);
            }

            TtlLoggerUtil.logTaskMsg(this, this.jobContext, logMsg);
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
        }
    }
}
