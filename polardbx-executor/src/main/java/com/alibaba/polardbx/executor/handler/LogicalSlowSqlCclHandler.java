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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.FetchPlanCacheSyncAction;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.ccl.CclManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclCondition;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetric;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCclRule;
import org.apache.calcite.sql.SqlSlowSqlCcl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2021/5/23 8:56 下午
 */
public class LogicalSlowSqlCclHandler extends HandlerCommon {

    private static final String SLOW_SQL_CCL_TRIGGER_NAME_FORMAT = "_SYSTEM_SLOW_SQL_CCL_TRIGGER_%s_";

    private static final String SLOW_SQL_CCL_OPERATION_GO = "GO";

    private static final String SLOW_SQL_CCL_OPERATION_ON = "ON";

    private static final String SLOW_SQL_CCL_OPERATION_BACK = "BACK";

    private static final String SLOW_SQL_CCL_OPERATION_OFF = "OFF";

    private static final String SLOW_SQL_CCL_OPERATION_SHOW = "SHOW";

    private static final int DEFAULT_MAX_CCL_RULE = LogicalCreateCclTriggerHandler.MAX_CCL_RULE;

    private static final int DEFAULT_MAX_SQL_SIZE = LogicalCreateCclTriggerHandler.MAX_SQL_SIZE;

    private static final Class SHOW_PROCESSLIST_SYNC_ACTION_CLASS;

    private static final Class KILL_SYNC_ACTION_CLASS;

    private static final Set<String> SUPPORTED_SQL_TYPES =
        Sets.newHashSet("SELECT", "UPDATE", "DELETE", "INSERT", "SELECT_FOR_UPDATE", "REPLACE", "INSERT_INTO_SELECT",
            "REPLACE_INTO_SELECT", "SELECT_UNION", "SELECT_WITHOUT_TABLE", "ALL");

    private List<String> cclTriggerIds =
        SUPPORTED_SQL_TYPES.stream().map((e) -> String.format(SLOW_SQL_CCL_TRIGGER_NAME_FORMAT, e))
            .collect(Collectors.toList());

    static {
        try {
            SHOW_PROCESSLIST_SYNC_ACTION_CLASS =
                Class.forName("com.alibaba.polardbx.server.response.ShowProcesslistSyncAction");
            KILL_SYNC_ACTION_CLASS = Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    private static final String SCHEMA_TEMPLATE_FORMAT = "%s-%s";

    public LogicalSlowSqlCclHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        LogicalCcl plan = (LogicalCcl) logicalPlan;
        SqlSlowSqlCcl slowSqlCcl = (SqlSlowSqlCcl) plan.getSqlDal();
        //检查触发器是否存在，不存在则创建
        String operation = slowSqlCcl.getOperation().getSimple();
        operation = StringUtils.upperCase(operation);
        Cursor cursor = null;
        switch (operation) {
        case SLOW_SQL_CCL_OPERATION_GO:
        case SLOW_SQL_CCL_OPERATION_ON:
            cursor = handleGo(slowSqlCcl, executionContext);
            break;
        case SLOW_SQL_CCL_OPERATION_BACK:
        case SLOW_SQL_CCL_OPERATION_OFF:
            cursor = handleBack(slowSqlCcl, executionContext);
            break;
        case SLOW_SQL_CCL_OPERATION_SHOW:
            cursor = handleShow(slowSqlCcl, executionContext);
            break;
        default:
            throw new TddlNestableRuntimeException(String.format("The operation of %s does not exist", operation));
        }

        return cursor;

    }

    public Cursor handleGo(SqlSlowSqlCcl easyCcl, ExecutionContext executionContext) {
        int maxConcurrency = ThreadCpuStatUtil.NUM_CORES / 2;
        int maxCclRule = DEFAULT_MAX_CCL_RULE;
        long slowSqlTime = executionContext.getParamManager().getLong(ConnectionParams.SLOW_SQL_TIME);
        slowSqlTime = (Long) executionContext.getUserDefVariables().getOrDefault("SLOW_SQL_TIME", slowSqlTime);
        slowSqlTime = (Long) executionContext.getUserDefVariables().getOrDefault("slow_sql_time", slowSqlTime);
        if (slowSqlTime > (long) Integer.MAX_VALUE) {
            slowSqlTime = Integer.MAX_VALUE;
        }
        String sqlType = "SELECT";
        List<SqlNode> sqlNodes = easyCcl.getParams();
        if (CollectionUtils.isNotEmpty(sqlNodes)) {
            int value = -1;
            for (int i = 0; i < sqlNodes.size(); ++i) {
                SqlNode sqlNode = sqlNodes.get(i);
                if (i == 1) {
                    value = Integer.parseInt(sqlNode.toString());
                    if (value >= 0) {
                        maxConcurrency = value;
                    }
                } else if (i == 2) {
                    value = Integer.parseInt(sqlNode.toString());
                    if (value >= 0) {
                        slowSqlTime = value;
                    }
                } else if (i == 0) {
                    sqlType = sqlNode.toString();
                    sqlType = StringUtils.upperCase(sqlType.replace("'", ""));
                    if (!SUPPORTED_SQL_TYPES.contains(sqlType)) {
                        throw new TddlNestableRuntimeException("Not Supported Sql Type of " + sqlType);
                    }
                } else if (i == 3) {
                    value = Integer.parseInt(sqlNode.toString());
                    if (value >= 0) {
                        maxCclRule = value;
                    }
                }
            }
        }

        boolean tryCreateResult = tryCreateCclTrigger(maxConcurrency, maxCclRule, (int) slowSqlTime, sqlType);
        if (!tryCreateResult) {
            if (!executionContext.getExtraDatas().containsKey(ExecutionContext.FAILED_MESSAGE)) {
                executionContext.getExtraDatas()
                    .put(ExecutionContext.FAILED_MESSAGE, Lists.newArrayListWithCapacity(1));
            }
            List<ExecutionContext.ErrorMessage> errorMessages =
                (List<ExecutionContext.ErrorMessage>) executionContext.getExtraDatas()
                    .get(ExecutionContext.FAILED_MESSAGE);
            errorMessages.add(new ExecutionContext.ErrorMessage(ErrorCode.ERR_CCL.getCode(), null,
                "The ccl trigger has been existing"));
        }
        //step 1 get processlist;
        List<Long> killIds = Lists.newArrayList();
        List<String> killSchemas = Lists.newArrayList();
        Set<String> templateSet = Sets.newHashSet();

        ISyncAction showProcesslistSyncAction;
        try {
            showProcesslistSyncAction = (ISyncAction) SHOW_PROCESSLIST_SYNC_ACTION_CLASS
                .getConstructor(boolean.class)
                .newInstance(true);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> processListResults = SyncManagerHelper.sync(showProcesslistSyncAction);
        for (List<Map<String, Object>> nodeRows : processListResults) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                String command = String.valueOf(row.get("COMMAND"));
                if (!StringUtils.equalsIgnoreCase(command, "QUERY")) {
                    continue;
                }
                int time = Integer.parseInt(String.valueOf(row.get("TIME"))) * 1000;
                if (time < slowSqlTime) {
                    continue;
                }
                String querySqlType = String.valueOf(row.get("SQL_TYPE"));
                if (!StringUtils.equalsIgnoreCase("ALL", sqlType) && !StringUtils
                    .equalsIgnoreCase(querySqlType, sqlType)) {
                    continue;
                }

                String sql = String.valueOf(row.get("INFO"));
                String templateId = String.valueOf(row.get("SQL_TEMPLATE_ID"));
                String schemaName = String.valueOf(row.get("DB"));
                if (!StringUtils.equalsIgnoreCase(templateId, "NULL")) {
                    if (templateSet.add(String.format(SCHEMA_TEMPLATE_FORMAT, schemaName, templateId))) {
                        //添加成功放入 把样本推给限流触发器
                        CclSqlMetric cclSqlMetric = new CclSqlMetric();
                        cclSqlMetric.setOriginalSql(sql);
                        cclSqlMetric.setSchemaName(schemaName);
                        cclSqlMetric.setParams(Lists.newArrayList());
                        cclSqlMetric.setTemplateId(templateId);
                        cclSqlMetric.setResponseTime(time);
                        CclManager.getTriggerService().offerSample(cclSqlMetric, false);
                    }
                }
            }

            for (Map<String, Object> row : nodeRows) {
                String command = String.valueOf(row.get("COMMAND"));
                if (!StringUtils.equalsIgnoreCase(command, "QUERY")) {
                    continue;
                }
                String schemaName = String.valueOf(row.get("DB"));
                long id = Long.parseLong(String.valueOf(row.get("ID")));
                String templateId = String.valueOf(row.get("SQL_TEMPLATE_ID"));
                if (templateSet.contains(String.format(SCHEMA_TEMPLATE_FORMAT, schemaName, templateId))) {
                    killIds.add(id);
                    killSchemas.add(schemaName);
                }
            }
        }
        CclManager.getTriggerService().processSamples();
        CclManager.getCclConfigService().reloadConfig();

        String user = executionContext.getPrivilegeContext().getUser();
        int count = 0;
        for (int i = 0; i < killIds.size(); ++i) {
            long id = killIds.get(i);
            String schema = killSchemas.get(i);
            ISyncAction killSyncAction;
            try {
                killSyncAction = (ISyncAction) KILL_SYNC_ACTION_CLASS
                    .getConstructor(String.class, long.class, boolean.class)
                    .newInstance(user, id, true);
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
            }
            List<List<Map<String, Object>>> results = SyncManagerHelper.sync(killSyncAction, schema);
            for (List<Map<String, Object>> result : results) {
                count += (Integer) result.iterator().next().get(ResultCursor.AFFECT_ROW);
            }
        }
        return new AffectRowCursor(new int[] {count});
    }

    private boolean tryCreateCclTrigger(int maxConcurrency, int maxCclRule, int slowSqlTime, String sqlType) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(false);
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            cclTriggerAccessor.setConnection(metaDbConn);
            CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
            cclRuleAccessor.setConnection(metaDbConn);
            String slowSqlCclTriggerName = String.format(SLOW_SQL_CCL_TRIGGER_NAME_FORMAT, sqlType);
            List<String> toDeleteCclTriggers = Lists.newArrayList(slowSqlCclTriggerName);
            List<CclTriggerRecord> cclTriggerRecords =
                cclTriggerAccessor.queryByIds(toDeleteCclTriggers);
            List<String> schemas = Lists.newArrayList();
            List<String> templates = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(cclTriggerRecords)) {
                List<Integer> cclTriggerPriorities =
                    cclTriggerRecords.stream().map(e -> e.priority).collect(Collectors.toList());
                List<CclRuleRecord> triggerCclRuleRecords = cclRuleAccessor.queryByPriorities(cclTriggerPriorities);
                System.out.println(triggerCclRuleRecords);
                for (CclRuleRecord cclRuleRecord : triggerCclRuleRecords) {
                    String ruleTemplates = cclRuleRecord.templateId;
                    if (StringUtils.isNotEmpty(ruleTemplates)) {
                        String[] splitTemplates = StringUtils.split(ruleTemplates, ",");
                        for (String splitTemplate : splitTemplates) {
                            schemas.add(cclRuleRecord.dbName);
                            templates.add(splitTemplate);
                        }
                    }
                }
                cclRuleAccessor.deleteByTriggers(cclTriggerPriorities);
                cclTriggerAccessor.deleteByIds(toDeleteCclTriggers);
            }

            CclTriggerRecord cclTriggerRecord = new CclTriggerRecord();
            cclTriggerRecord.id = slowSqlCclTriggerName;
            cclTriggerRecord.schema = "*";
            List<CclCondition> cclConditions = Lists.newArrayList();
            cclConditions.add(
                CclCondition.builder().sqlMetricName(CclSqlMetric.METRIC_NAME_RESPONSE_TIME).comparison(">=")
                    .value((long) slowSqlTime)
                    .build());
            if (!StringUtils.equalsIgnoreCase(sqlType, "ALL")) {
                cclConditions
                    .add(CclCondition.builder().sqlMetricName(CclSqlMetric.METRIC_SQL_TYPE).comparison("=").value(
                        SqlType.valueOf(sqlType).getI()).build());
            }
            cclTriggerRecord.conditions = JSON.toJSONString(cclConditions);
            cclTriggerRecord.ruleConfig = String
                .format("[{\"comparison\":\"=\",\"sqlMetricName\":\"MAX_CONCURRENCY\",\"value\":%d}]",
                    maxConcurrency);
            cclTriggerRecord.maxCclRule = maxCclRule;
            cclTriggerRecord.ruleUpgrade = 1;
            cclTriggerRecord.cclRuleCount = 0;
            cclTriggerRecord.maxSQLSize = DEFAULT_MAX_SQL_SIZE;
            cclTriggerAccessor.insert(cclTriggerRecord);
            String dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
            MetaDbConfigManager metaDbConfigManager = MetaDbConfigManager.getInstance();
            metaDbConfigManager.notify(dataId, metaDbConn);
            MetaDbUtil.commit(metaDbConn);
            metaDbConfigManager.sync(dataId);
            CclManager.getCclConfigService().reloadConfig();
            for (int i = 0; i < schemas.size(); ++i) {
                String schema = schemas.get(i);
                String template = templates.get(i);
                CclSqlMetric cclSqlMetric = new CclSqlMetric();
                cclSqlMetric.setSchemaName(schema);
                cclSqlMetric.setParams(Lists.newArrayList());
                cclSqlMetric.setTemplateId(template);
                cclSqlMetric.setResponseTime(Integer.MAX_VALUE);
                CclManager.getTriggerService().offerSample(cclSqlMetric, false);
            }
            return true;
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    private Cursor handleBack(SqlSlowSqlCcl slowSqlCcl, ExecutionContext executionContext) {
        int affectedRows = 0;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(false);
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            cclTriggerAccessor.setConnection(metaDbConn);
            CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
            cclRuleAccessor.setConnection(metaDbConn);

            List<CclTriggerRecord> cclTriggerRecords =
                cclTriggerAccessor.queryByIds(cclTriggerIds);
            if (CollectionUtils.isNotEmpty(cclTriggerRecords)) {
                List<Integer> cclTriggerPriorities =
                    cclTriggerRecords.stream().map(e -> e.priority).collect(Collectors.toList());
                affectedRows = cclRuleAccessor.deleteByTriggers(cclTriggerPriorities);
            }
            cclTriggerAccessor.deleteByIds(cclTriggerIds);
            String dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
            MetaDbConfigManager metaDbConfigManager = MetaDbConfigManager.getInstance();
            metaDbConfigManager.notify(dataId, metaDbConn);
            MetaDbUtil.commit(metaDbConn);
            metaDbConfigManager.sync(dataId);
        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
        return new AffectRowCursor(new int[] {affectedRows});
    }

    private Cursor handleShow(SqlSlowSqlCcl slowSqlCcl, ExecutionContext executionContext) {

        ArrayResultCursor result = new ArrayResultCursor("Slow_Sql_Ccl");
        result.addColumn("NO.", DataTypes.IntegerType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("TEMPLATE_ID", DataTypes.StringType);
        result.addColumn("SQL", DataTypes.StringType);
        result.addColumn("RULE_NAME", DataTypes.StringType);
        result.addColumn("RUNNING", DataTypes.IntegerType);
        result.addColumn("WAITING", DataTypes.IntegerType);
        result.addColumn("KILLED", DataTypes.LongType);
        result.addColumn("TOTAL_MATCH", DataTypes.LongType);
        result.addColumn("ACTIVE_NODE_COUNT", DataTypes.IntegerType);
        result.addColumn("MAX_CONCURRENCY_PER_NODE", DataTypes.IntegerType);
        result.addColumn("CREATED_TIME", DataTypes.DatetimeType);
        result.initMeta();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(false);
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            cclTriggerAccessor.setConnection(metaDbConn);
            List<Integer> cclTriggerPriorities =
                cclTriggerAccessor.queryByIds(cclTriggerIds).stream().map((e) -> e.priority)
                    .collect(Collectors.toList());
            CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
            cclRuleAccessor.setConnection(metaDbConn);

            if (CollectionUtils.isNotEmpty(cclTriggerPriorities)) {
                Set<String> cclRuleRecords =
                    cclRuleAccessor.queryByPriorities(cclTriggerPriorities).stream().map((e) -> e.id).collect(
                        Collectors.toSet());
                if (!cclRuleRecords.isEmpty()) {
                    LogicalShowCclRuleHandler logicalShowCclRuleHandler = new LogicalShowCclRuleHandler(null);
                    SqlShowCclRule sqlShowCclRule =
                        new SqlShowCclRule(SqlParserPos.ZERO, Lists.newArrayList(SqlSpecialIdentifier.CCL_RULE), true,
                            Lists.newArrayListWithCapacity(0));
                    Cursor planCacheCursor = getPlanCacheCursor(executionContext);
                    Map<String, String> buildMap = Maps.newHashMap();
                    Map<String, Integer> planCacheMetaPosMap = getPosMap(planCacheCursor.getReturnColumns());
                    Integer schemaNamePos = planCacheMetaPosMap.get("SCHEMA_NAME");
                    Integer idPos = planCacheMetaPosMap.get("ID");
                    Integer sqlPos = planCacheMetaPosMap.get("SQL");
                    Row row = null;
                    while (!planCacheCursor.isFinished() && (row = planCacheCursor.next()) != null) {
                        buildMap.put(row.getString(schemaNamePos) + "-" + row.getString(idPos), row.getString(sqlPos));
                    }
                    planCacheCursor.close(Lists.newArrayList());
                    Cursor cclRuleCursor = logicalShowCclRuleHandler.handle(sqlShowCclRule, executionContext);
                    Map<String, Integer> cclRuleColumnMetaPosMap = getPosMap(cclRuleCursor.getReturnColumns());
                    Integer tablePos = cclRuleColumnMetaPosMap.get("TABLE");
                    Integer ruleNamePos = cclRuleColumnMetaPosMap.get("RULE_NAME");
                    Integer runningPos = cclRuleColumnMetaPosMap.get("RUNNING");
                    Integer waitingPos = cclRuleColumnMetaPosMap.get("WAITING");
                    Integer killedPos = cclRuleColumnMetaPosMap.get("KILLED");
                    Integer totalMatchPos = cclRuleColumnMetaPosMap.get("TOTAL_MATCH");
                    Integer activeNodeCountPos = cclRuleColumnMetaPosMap.get("ACTIVE_NODE_COUNT");
                    Integer maxConcurrencyPerNodePos = cclRuleColumnMetaPosMap.get("MAX_CONCURRENCY_PER_NODE");
                    Integer templateIdPos = cclRuleColumnMetaPosMap.get("TEMPLATE_ID");
                    Integer createdTimePos = cclRuleColumnMetaPosMap.get("CREATED_TIME");
                    int number = 1;
                    while (!cclRuleCursor.isFinished() && (row = cclRuleCursor.next()) != null) {
                        String table = row.getString(tablePos);
                        String ruleName = row.getString(ruleNamePos);
                        int running = row.getInteger(runningPos);
                        int waiting = row.getInteger(waitingPos);
                        int killed = row.getInteger(killedPos);
                        int totalMatch = row.getInteger(totalMatchPos);
                        int activeNodeCount = row.getInteger(activeNodeCountPos);
                        int maxConcurrencyPerNode = row.getInteger(maxConcurrencyPerNodePos);
                        Date createdTime = row.getDate(createdTimePos);
                        String schema = "";
                        if (StringUtils.isNotEmpty(table)) {
                            int splitChPos = table.indexOf(".");
                            if (splitChPos > 0) {
                                schema = table.substring(0, splitChPos);
                            }
                        }
                        String templateIdsStr = row.getString(templateIdPos);
                        if (StringUtils.isNotEmpty(templateIdsStr)) {
                            String[] templateIds = StringUtils.split(templateIdsStr, ",");
                            for (String templateId : templateIds) {
                                String key = schema + "-" + templateId;
                                String sql = buildMap.get(key);
                                if (sql == null) {
                                    sql = "";
                                }
                                result.addRow(new Object[] {
                                    number++, schema, templateId, sql, ruleName, running, waiting, killed, totalMatch,
                                    activeNodeCount, maxConcurrencyPerNode, createdTime});
                            }
                        }
                    }
                    cclRuleCursor.close(Lists.newArrayList());
                }
            }

        } catch (Throwable e) {
            throw new TddlNestableRuntimeException(e);
        }
        return result;
    }

    private Cursor getPlanCacheCursor(ExecutionContext executionContext) {
        ArrayResultCursor cursor = new ArrayResultCursor("PLAN_CACHE");
        cursor.addColumn("COMPUTE_NODE", DataTypes.StringType);
        cursor.addColumn("SCHEMA_NAME", DataTypes.StringType);
        cursor.addColumn("TABLE_NAMES", DataTypes.StringType);
        cursor.addColumn("ID", DataTypes.StringType);
        cursor.addColumn("HIT_COUNT", DataTypes.LongType);
        cursor.addColumn("SQL", DataTypes.StringType);

        cursor.initMeta();

        Set<String> schemaNames = OptimizerContext.getActiveSchemaNames();
        for (String schemaName : schemaNames) {

            if (SystemDbHelper.CDC_DB_NAME.equalsIgnoreCase(schemaName)) {
                continue;
            }

            List<List<Map<String, Object>>> results = SyncManagerHelper.sync(new FetchPlanCacheSyncAction(schemaName,
                    false),
                schemaName);

            for (List<Map<String, Object>> nodeRows : results) {
                if (nodeRows == null) {
                    continue;
                }

                for (Map<String, Object> row : nodeRows) {

                    final String host = DataTypes.StringType.convertFrom(row.get("COMPUTE_NODE"));
                    final String tableNames = DataTypes.StringType.convertFrom(row.get("TABLE_NAMES"));
                    final String id = DataTypes.StringType.convertFrom(row.get("ID"));
                    final Long hitCount = DataTypes.LongType.convertFrom(row.get("HIT_COUNT"));
                    final String sql = DataTypes.StringType.convertFrom(row.get("SQL"));

                    cursor.addRow(new Object[] {
                        host,
                        schemaName,
                        tableNames,
                        id,
                        hitCount,
                        sql
                    });
                }
            }
        }
        return cursor;

    }

    private Map<String, Integer> getPosMap(List<ColumnMeta> columnMetas) {
        Map<String, Integer> posMap = Maps.newHashMap();
        for (int i = 0; i < columnMetas.size(); ++i) {
            ColumnMeta columnMeta = columnMetas.get(i);
            String columnName = StringUtils.upperCase(columnMeta.getName());
            posMap.put(columnName, i);
        }
        return posMap;
    }

}
