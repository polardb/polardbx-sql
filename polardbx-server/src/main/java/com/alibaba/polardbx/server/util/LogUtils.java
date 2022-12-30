/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.server.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.gms.privilege.audit.AuditPrivilege;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclMetric;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetric;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.alibaba.polardbx.statistics.RuntimeStatistics.Metrics;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.plan.RelOptCost;
import org.apache.commons.lang.StringUtils;

import java.util.List;

import static com.alibaba.polardbx.common.utils.logger.support.LogFormat.formatLog;

/**
 * @author lingce.ldm 2018-07-03 17:11
 */
public class LogUtils {

    private static final Logger logger = LoggerFactory.getLogger(LogUtils.class);
    private static final Logger recordSql = SQLRecorderLogger.sqlLogger;
    private static final Logger recordDdl = SQLRecorderLogger.ddlLogger;
    private static int MAX_SQL_LENGTH = 4096;
    private static boolean enableSqlProfileLog = true;

    public static void recordSql(ServerConnection c, ByteString sql, Throwable ex) {
        long endTimeNano = System.nanoTime();
        recordSql(c, "", sql, endTimeNano, ex != null ? -1 : 0);
    }

    public static void recordSql(ServerConnection c, ByteString sql, boolean success) {
        long endTimeNano = System.nanoTime();
        recordSql(c, "", sql, endTimeNano, success ? 0 : -1);
    }

    public static void recordSql(ServerConnection c, String tag, ByteString sql, long endTimeNano, long affectedRows) {
        recordSql(c, tag, sql, null, null, affectedRows, endTimeNano, null, null, null, WorkloadType.TP, null, null,
            false);
    }

    public static void recordPreparedSql(ServerConnection c, String stmtId,
                                         ByteString sql, long endTimeNano, long affectedRows) {
        if (!recordSql.isInfoEnabled()) {
            return;
        }
        StringBuilder tagInfo = new StringBuilder();
        tagInfo.append("[prepare] ");
        tagInfo.append("[stmt:").append(stmtId).append("]");
        recordSql(c, tagInfo.toString(), sql, null, null, affectedRows, endTimeNano, null, null, null, WorkloadType.TP,
            null, null, true);
    }

    public static void recordSql(ServerConnection c, String tag, ByteString sqlBytes,
                                 List<Pair<Integer, ParameterContext>> params, String transactionPolicy,
                                 long affectRow, long endTimeNano, QueryMetrics metrics, Integer baselineInfoId,
                                 Integer planInfoId, WorkloadType workloadType, RelOptCost cost, ExecutorMode mode,
                                 boolean recordSlowDetail) {

        try {
            if (!recordSql.isInfoEnabled()) {
                return;
            }

            if (c.getSchema() == null) {
                return;
            }

            SchemaConfig schema = c.getSchemaConfig();
            if (schema == null) {
                return;
            }

            if (schema.getDataSource() == null) {
                return;
            }

            if (schema.getDataSource().getConnectionProperties() == null) {
                return;
            }

            if (!GeneralUtil.getPropertyBoolean(schema.getDataSource().getConnectionProperties(),
                ConnectionProperties.RECORD_SQL,
                true)) {
                return;
            }

            /**
             * For dal/dcl stmt, its profile is disable, so the val of metrics
             * will be null.
             */
            Metrics statMetrics = null;
            if (metrics != null) {
                RuntimeStatistics runTimeStat = metrics.runTimeStat;
                if (runTimeStat != null && runTimeStat.isRunningWithCpuProfile()) {
                    statMetrics = runTimeStat.toMetrics();
                    runTimeStat.setStoredMetrics(statMetrics);
                }
            }

            long sqlBeginTs = c.getSqlBeginTimestamp();
            long startTime = c.getLastActiveTime();
            // microseconds
            long duration = (endTimeNano - startTime) / 1000;

            StringBuilder sqlInfo = new StringBuilder();
            if (tag != null && !tag.isEmpty()) {
                sqlInfo.append(tag).append(" ");
            }

            if (!c.isAutocommit()) {
                sqlInfo.append("[autocommit=0,");
                sqlInfo.append(transactionPolicy);
                sqlInfo.append("] ");
            }

            String sql;
            if (sqlBytes.length() > MAX_SQL_LENGTH) {
                String suffix = String.format("... +%d more", sqlBytes.length() - MAX_SQL_LENGTH);
                sql = sqlBytes.substring(0, MAX_SQL_LENGTH) + suffix;
                sqlInfo.append("[TOO LONG] ");
            } else {
                sql = sqlBytes.toString();
            }

            Token token = getToken(sqlBytes);
            if (isDDL(token)) {
                recordDdl.info(
                    SQLRecorderLogger.ddlLogFormat.format(new Object[] {sql, duration, affectRow, c.getTraceId()}));
                polarAuditDb(c, sql, token);
            }

            sqlInfo.append("[V3] ");

            String formatSql = formatLog(sql);
            sqlInfo.append("[len=").append(formatSql.length()).append("] ");
            sqlInfo.append(formatSql);

            JSONArray jsonArray = new JSONArray();
            if (params != null && params.size() > 0) {
                for (Pair<Integer, ParameterContext> pair : params) {
                    jsonArray.add(pair.getValue().getValue());
                }
            }

            String jsonArrayString = jsonArray.toJSONString();
            sqlInfo.append(" [len=").append(jsonArrayString.length()).append("] ");
            sqlInfo.append(jsonArrayString);

            /*
             * Records the metrics information. e.g. ... #
             * [rt=5,affected_rows=10] # d718b4458400000-3 ... #
             * [rt=5,type=111,mem=123,cpu=123,network=123,processed_rows= ...
             */
            sqlInfo.append(" # [");
            sqlInfo.append(QueryMetricsAttribute.SQL_RT).append(duration);
            sqlInfo.append(QueryMetricsAttribute.AFFECT_ROWS).append(affectRow);
            boolean cclTrigger = affectRow != -1 && (metrics == null || metrics.cclMetric == null)
                && CclManager
                .getTriggerService().isWorking();

            CclSqlMetric cclSqlMetric = null;
            if (cclTrigger) {
                cclSqlMetric = new CclSqlMetric();
                cclSqlMetric.setSchemaName(schema.getName());
                cclSqlMetric.setOriginalSql(sqlBytes.toString());
                cclSqlMetric.setResponseTime(duration /1000);
                cclSqlMetric.setAffectedRows(affectRow);
                if (metrics != null && metrics.runTimeStat != null && metrics.runTimeStat.getSqlType() != null) {
                    cclSqlMetric.setSqlType(metrics.runTimeStat.getSqlType().getI());
                }
            }

            if (metrics != null) {
                String type = encodeType(metrics.hasTempTable, metrics.hasUnpushedJoin, metrics.hasMultiShards);
                sqlInfo.append(QueryMetricsAttribute.STAT_TYPE).append(type);

                if (statMetrics != null) {

                    if (enableSqlProfileLog) {
                        printMetric(sqlInfo, QueryMetricsAttribute.FETCHED_ROWS, statMetrics.fetchedRows);
                        printMetric(sqlInfo, QueryMetricsAttribute.AFFECTED_PHY_ROWS, statMetrics.affectedPhyRows);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_COUNT, statMetrics.phySqlCount);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_MEMORY, statMetrics.queryMem);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_MEMORY_PCT, statMetrics.queryMemPct);
                        printMetric(sqlInfo, QueryMetricsAttribute.SHARD_PLAN_MEMORY, statMetrics.planShardMem);
                        printMetric(sqlInfo, QueryMetricsAttribute.TMP_TABLE_MEMORY, statMetrics.planTmpTbMem);
                        printMetric(sqlInfo, QueryMetricsAttribute.LOGICAL_TIMECOST, statMetrics.logCpuTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.SQL_TO_PALN_TIMECOST, statMetrics.sqlToPlanTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.EXEC_PLAN_TIMECOST, statMetrics.execPlanTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.PHYSICAL_TIMECOST, statMetrics.phyCpuTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.EXEC_SQL_TIMECOST, statMetrics.execSqlTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.FETCH_RS_TIMECOST, statMetrics.fetchRsTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.PHYSICAL_CONN_TIMECOST, statMetrics.phyConnTc);
                        printMetric(sqlInfo, QueryMetricsAttribute.SPILL_COUNT, statMetrics.spillCnt);
                        printMetric(sqlInfo, QueryMetricsAttribute.MEM_BLOCKED, statMetrics.memBlockedFlag);

                        if (cclTrigger) {
                            cclSqlMetric.setFetchRows(statMetrics.fetchedRows);
                            cclSqlMetric.setAffectedPhyRows(statMetrics.affectedPhyRows);
                            cclSqlMetric.setPhySqlCount(statMetrics.phySqlCount);
                        }

                    }
                }
                if (metrics.sqlTemplateId != null) {
                    sqlInfo.append(QueryMetricsAttribute.SQL_TEMPLATE_ID).append(metrics.sqlTemplateId);
                    if (cclTrigger) {
                        cclSqlMetric.setTemplateId(metrics.sqlTemplateId);
                    }
                }
                sqlInfo.append(QueryMetricsAttribute.SQL_TIMESTAMP).append(sqlBeginTs);
                sqlInfo.append(QueryMetricsAttribute.MEMORY_REJECT).append(metrics.rejectByMemoryLimit ? "1" : "0");
            }

            if (cclTrigger && StringUtils.isNotEmpty(cclSqlMetric.getTemplateId())) {
                cclSqlMetric.setActiveSession(ServerThreadPool.AppStats.nodeTaskCount.get());
                cclSqlMetric.setParams(params);
                CclManager.getTriggerService().offerSample(cclSqlMetric, true);
            }

            if (baselineInfoId == null) {
                baselineInfoId = -1;
            }
            sqlInfo.append(QueryMetricsAttribute.BASELINE_ID).append(baselineInfoId);

            if (planInfoId == null) {
                planInfoId = -1;
            }
            sqlInfo.append(QueryMetricsAttribute.PLAN_ID).append(planInfoId);

            if (workloadType == null) {
                workloadType = WorkloadType.TP;
            }

            sqlInfo.append(QueryMetricsAttribute.WORKLOAD_TYPE).append(workloadType.name());

            if (mode != null) {
                sqlInfo.append(QueryMetricsAttribute.EXECUTOR_MODE).append(mode.name());
            } else {
                sqlInfo.append(QueryMetricsAttribute.EXECUTOR_MODE).append(ExecutorMode.NONE.name());
            }

            if (cost == null) {
                sqlInfo.append(QueryMetricsAttribute.CPU_COST).append(0);
                sqlInfo.append(QueryMetricsAttribute.MEMORY_COST).append(0);
                sqlInfo.append(QueryMetricsAttribute.IO_COST).append(0);
                sqlInfo.append(QueryMetricsAttribute.NET_COST).append(0);
            } else {
                sqlInfo.append(QueryMetricsAttribute.CPU_COST).append((long) cost.getCpu());
                sqlInfo.append(QueryMetricsAttribute.MEMORY_COST).append((long) cost.getMemory());
                sqlInfo.append(QueryMetricsAttribute.IO_COST).append((long) cost.getIo());
                sqlInfo.append(QueryMetricsAttribute.NET_COST).append(cost.getNet());
            }

            if (metrics != null) {
                CclMetric cclMetric = metrics.cclMetric;
                if (cclMetric != null) {
                    sqlInfo.append(QueryMetricsAttribute.CCL_RULE_NAME).append(cclMetric.getRuleName());
                    sqlInfo.append(QueryMetricsAttribute.CCL_STATUS).append(cclMetric.getType());
                    sqlInfo.append(QueryMetricsAttribute.CCL_WAIT_TIME).append(cclMetric.getValue());
                    sqlInfo.append(QueryMetricsAttribute.CCL_HC).append(cclMetric.isHitCache() ? 1 : 0);
                }
            }

            if (metrics != null) {
                sqlInfo.append(QueryMetricsAttribute.USING_RETURNING).append(metrics.optimizedWithReturning ? 1 : 0);
            }

            sqlInfo.append("] # ").append(c.getTraceId());
            String sqlLogContent = sqlInfo.toString();
            SQLRecorderLogger.sqlLogger.info(sqlLogContent);
            //log slow detail
            if (recordSlowDetail) {
                SQLRecorderLogger.slowDetailLogger.info(sqlLogContent);
            }
        } catch (Throwable ex) {
            logger.info("record sql failed", ex);
        }

    }

    protected static void printMetric(StringBuilder sqlInfo, String metricKey, long metricVal) {
        sqlInfo.append(metricKey).append(metricVal);
    }

    protected static void printMetric(StringBuilder sqlInfo, String metricKey, double metricVal) {
        sqlInfo.append(metricKey).append(metricVal);
    }

    /**
     * SqlParserUtils.getSQLType doesn't include DROP SqlTypeParser.typeOf can't
     * parse an incomplete statement. SqlTypeParser.typeOf parses all the
     * statement, which is unnecessary.
     */
    private static Token getToken(ByteString sql) {
        try {
            Lexer lexer = new Lexer(sql);

            do {
                // skip whitespaces
                lexer.nextToken();

                if (lexer.token() == Token.COMMENT || lexer.token() == Token.LINE_COMMENT
                    || lexer.token() == Token.MULTI_LINE_COMMENT) {

                } else if (lexer.token() == Token.SLASH) {

                    int i = lexer.pos();
                    if (lexer.charAt(i) == '!') {
                        for (i++; ; i++) {
                            char ch = lexer.charAt(i);
                            if (ch == 26) {
                                return null; // error
                            } else if (ch == '*' && lexer.charAt(i + 1) == '/') {
                                lexer.reset(i + 2);
                                break;
                            }
                        }
                    } else {
                        return null; // error
                    }
                } else {
                    break;
                }

            } while (true);

            switch (lexer.token()) {
            case CREATE:
            case DROP:
            case TRUNCATE:
            case PURGE:
            case ALTER:
                return lexer.token();
            }
        } catch (Throwable e) {

        }

        return null;
    }

    public static boolean isDDL(Token token) {
        if (token == null) {
            return false;
        }
        switch (token) {
        case CREATE:
        case DROP:
        case TRUNCATE:
        case PURGE:
        case ALTER:
            return true;
        }
        return false;
    }

    private static String encodeType(boolean hasTempTable, boolean hasUnpushedJoin, boolean hasScanWholeTable) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(hasTempTable ? "1" : "0");
        stringBuilder.append(hasUnpushedJoin ? "1" : "0");
        stringBuilder.append(hasScanWholeTable ? "1" : "0");
        return stringBuilder.toString();
    }

    /**
     * Metrics Information for logging
     */
    public static class QueryMetrics {

        // label if the query is reject by memory pool limit
        public boolean rejectByMemoryLimit;

        // This query used some operator with buffered data (e.g. HashJoin)
        public boolean hasTempTable;

        // This query contains Join operator in logical plan
        public boolean hasUnpushedJoin;

        // This query scans more than one shards
        public boolean hasMultiShards;

        public RuntimeStatistics runTimeStat;

        // The sql template Id
        public String sqlTemplateId;

        //metric for ccl
        public CclMetric cclMetric;

        // Whether dml optimized with returning
        public boolean optimizedWithReturning = false;
    }

    public class QueryMetricsAttribute {

        public static final String SQL_RT = "rt=";
        public static final String AFFECT_ROWS = ",rows=";
        public static final String STAT_TYPE = ",type=";
        public static final String SQL_TIMESTAMP = ",ts=";
        public static final String BASELINE_ID = ",bid=";
        public static final String PLAN_ID = ",pid=";
        public static final String FETCHED_ROWS = ",frows=";
        public static final String AFFECTED_PHY_ROWS = ",arows=";
        public static final String SQL_COUNT = ",scnt=";
        public static final String SQL_MEMORY = ",mem=";
        public static final String SQL_MEMORY_PCT = ",mpct=";
        public static final String SHARD_PLAN_MEMORY = ",smem=";
        public static final String TMP_TABLE_MEMORY = ",tmem=";
        public static final String LOGICAL_TIMECOST = ",ltc=";
        public static final String SQL_TO_PALN_TIMECOST = ",lotc=";
        public static final String EXEC_PLAN_TIMECOST = ",letc=";
        public static final String PHYSICAL_TIMECOST = ",ptc=";
        public static final String EXEC_SQL_TIMECOST = ",pstc=";
        public static final String FETCH_RS_TIMECOST = ",prstc=";
        public static final String PHYSICAL_CONN_TIMECOST = ",pctc=";
        public static final String SQL_TEMPLATE_ID = ",tid=";
        public static final String MEMORY_REJECT = ",mr=";
        public static final String WORKLOAD_TYPE = ",wt=";
        public static final String EXECUTOR_MODE = ",em=";
        public static final String SPILL_COUNT = ",sct=";
        public static final String MEM_BLOCKED = ",mbt=";
        public static final String CPU_COST = ",lcpu=";
        public static final String MEMORY_COST = ",lmem=";
        public static final String IO_COST = ",lio=";
        public static final String NET_COST = ",lnet=";
        public static final String CCL_RULE_NAME = ",ccl=";
        public static final String CCL_WAIT_TIME = ",cclwt=";
        public static final String CCL_STATUS = ",cclst=";
        public static final String CCL_HC = ",cclhc=";
        public static final String USING_RETURNING = ",ur=";
    }

    public static void resetMaxSqlLen(int newLen) {
        LogUtils.MAX_SQL_LENGTH = newLen;
    }

    public static void resetEnableSqlProfileLog(boolean enableSqlProfileLog) {
        LogUtils.enableSqlProfileLog = enableSqlProfileLog;
    }

    public static void polarAuditDb(ServerConnection c, String auditInfo, Token token) {
        if (token == null) {
            return;
        }
        AuditAction auditAction = AuditAction.value(token.name);
        if (auditAction == null) {
            return;
        }
        AuditPrivilege.polarAuditDb(c.getConnectionInfo(), auditInfo, auditAction);
    }
}
