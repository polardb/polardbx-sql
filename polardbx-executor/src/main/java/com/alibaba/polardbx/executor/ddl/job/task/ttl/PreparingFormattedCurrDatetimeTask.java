package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
@Getter
@TaskName(name = "PrepareFormattedCurrDatetimeTask")
public class PreparingFormattedCurrDatetimeTask extends AbstractTtlJobTask {

    protected String currentDatetime = null;
    protected String formattedCurrentDatetime = null;
    protected String expiredUpperBound = null;
    protected String currentDatetimeFormatedByTtlExprUnit = null;
    protected String currentDatetimeFormatedByArcPartUnit = null;

    @JSONCreator
    public PreparingFormattedCurrDatetimeTask(String schemaName,
                                              String logicalTableName) {
        super(schemaName, logicalTableName);
        onExceptionTryRecoveryThenPause();
    }

    public void executeImpl(ExecutionContext executionContext) {
        TtlJobUtil.updateJobStage(this.jobContext, "PrepareFormattedCurrDatetimeTask");
        initCurrDatetimeAndExpiredUpperBound(executionContext);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        executeImpl(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);
    }

    protected void initCurrDatetimeAndExpiredUpperBound(ExecutionContext ec) {
        final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
        final String ttlTblSchemaName = this.jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();

        TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
        boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();
        if (useExpireOver) {
            return;
        }
        String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
        String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
        String groupParallelismForConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDqlConn());
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        sessionVariables.put("time_zone", ttlTimezoneStr);
        sessionVariables.put("names", charsetEncoding);
        sessionVariables.put("sql_mode", sqlModeSetting);
        sessionVariables.put("group_parallelism", groupParallelismForConnStr);

        /**
         * query the round-downed upper bound of expired value of ttl_col
         * <pre>
         * set TIME_ZONE='xxx';
         * SELECT DATE_FORMAT( DATE_SUB(NOW(), INTERVAL %s %s), formatter ) expired_upper_bound;
         * formatter is like %Y-%m-%d 00:00:00.000000
         * </pre>
         *
         */
        fetchCurrDatetimeAndExpiredUpperBound(serverConfigManager, ttlTblSchemaName, ec, sessionVariables);

        this.jobContext.setCurrentDateTime(this.currentDatetime);
        this.jobContext.setCurrentDateTimeFormatedByArcPartUnit(this.currentDatetimeFormatedByArcPartUnit);
        this.jobContext.setCurrentDateTimeFormatedByTtlExprUnit(this.currentDatetimeFormatedByTtlExprUnit);
        this.jobContext.setCleanUpUpperBound(this.expiredUpperBound);
    }

    private void fetchCurrDatetimeAndExpiredUpperBound(IServerConfigManager serverConfigManager,
                                                       String ttlTblSchemaName,
                                                       ExecutionContext ec,
                                                       Map<String, Object> sessionVariables) {
        TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            ttlTblSchemaName,
            sessionVariables,
            (transConn) -> {

                /**
                 * Select the current_datetime value
                 * <pre>
                 * set TIME_ZONE='xxx';
                 * SELECT now() as current_datetime;
                 * </pre>
                 *
                 */
                final String selectCurrentDtStringSql = TtlTaskSqlBuilder.buildSelectCurrentDatetimeValueStringSql(ec);
                List<Map<String, Object>> currentDtResult =
                    TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                        ttlTblSchemaName,
                        transConn,
                        ec,
                        selectCurrentDtStringSql);
                final String currentDatetimeValStr = TtlJobUtil.fetchStringFromQueryValue(currentDtResult,
                    TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_CURRENT_DATETIME);
                this.currentDatetime = currentDatetimeValStr;

                /**
                 * Select the current_datetime value formated by arc_part timeunit
                 * <pre>
                 * set TIME_ZONE='xxx';
                 * SELECT DATE_FORMAT( current_datetime, %s ) as formated_current_datetime;
                 * </pre>
                 *
                 */
                final String selectCurrentDtFormatedByArcPartUnitSql =
                    buildSelectFormatedDatetimeStringSql(ec, currentDatetimeValStr, true);
                List<Map<String, Object>> currentDtFormatedByArcPartUnitResult =
                    TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                        ttlTblSchemaName,
                        transConn,
                        ec,
                        selectCurrentDtFormatedByArcPartUnitSql);
                String currentDtFormatedByArcPartUnit =
                    TtlJobUtil.fetchStringFromQueryValue(currentDtFormatedByArcPartUnitResult,
                        TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_FORMATED_CURRENT_DATETIME);
                this.currentDatetimeFormatedByArcPartUnit = currentDtFormatedByArcPartUnit;

                boolean specifyTtlExprUnit = this.jobContext.getTtlInfo().isExpireIntervalSpecified();
                if (specifyTtlExprUnit) {
                    /**
                     * Select
                     *  the current_datetime value formated by ttl_expr timeunit
                     *  and
                     *  the round-downed upper bound of expired value of ttl_col by current_datetime
                     *
                     * <pre>
                     * set TIME_ZONE='xxx';
                     * SELECT
                     *  DATE_FORMAT( current_datetime, %s ) as formated_current_datetime
                     *  ,
                     *  DATE_FORMAT( DATE_SUB(NOW(), INTERVAL %s %s), formatter ) expired_upper_bound;
                     * formatter is like %Y-%m-%d 00:00:00.000000
                     * </pre>
                     *
                     */
                    final String selectExpiredUpperBoundSql =
                        buildSelectExpiredUpperBoundSql(ec, currentDatetimeValStr);
                    List<Map<String, Object>> upperBoundResult =
                        TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                            ttlTblSchemaName,
                            transConn,
                            ec,
                            selectExpiredUpperBoundSql);
                    String currentDtFormatedByTtlExprUnit = TtlJobUtil.fetchStringFromQueryValue(upperBoundResult,
                        TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_FORMATED_CURRENT_DATETIME);
                    this.currentDatetimeFormatedByTtlExprUnit = currentDtFormatedByTtlExprUnit;
                    String expiredUpperBoundStr =
                        TtlJobUtil.fetchStringFromQueryValue(upperBoundResult,
                            TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_UPPER_BOUND);
                    this.expiredUpperBound = expiredUpperBoundStr;
                }
                return 0;
            }
        );
    }

    protected String buildSelectExpiredUpperBoundSql(ExecutionContext ec, String nowFuncExpr) {
        TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
        String selectSql = TtlTaskSqlBuilder.buildCleanupUpperBoundValueSqlTemplate(ttlInfo, ec, nowFuncExpr);
        return selectSql;
    }

    protected String buildSelectFormatedDatetimeStringSql(ExecutionContext ec,
                                                          String currTimeValStr,
                                                          boolean isForArcPart) {
        TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
        TtlTimeUnit targetTimeUnit = null;
        if (isForArcPart) {
            targetTimeUnit = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getArcPartUnit());
        } else {
            targetTimeUnit = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getTtlUnit());
        }

        String tarTimeUnitName = targetTimeUnit.getUnitName();
        String selectSql =
            TtlTaskSqlBuilder.buildSelectFormatedDatetimeValueSql(ttlInfo, ec, currTimeValStr, tarTimeUnitName);
        return selectSql;
    }

}