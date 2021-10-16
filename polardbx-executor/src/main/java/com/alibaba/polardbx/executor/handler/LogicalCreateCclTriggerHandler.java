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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.ccl.common.CclCondition;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetric;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateCclTrigger;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author busu
 * date: 2021/4/18 4:48 下午
 */
public class LogicalCreateCclTriggerHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateCclTriggerHandler.class);

    private static final Set<String> WITH_KEYS = Sets.newHashSet();

    static {
        WITH_KEYS.add("MAX_CONCURRENCY");
        WITH_KEYS.add("WAIT_QUEUE_SIZE");
        WITH_KEYS.add("WAIT_TIMEOUT");
        WITH_KEYS.add("FAST_MATCH");
        WITH_KEYS.add("LIGHT_WAIT");
    }

    private static final Set<String> LIMIT_KEYS = Sets.newHashSet();

    static {
        LIMIT_KEYS.add("QUERY_RULE_UPGRADE");
        LIMIT_KEYS.add("MAX_CCL_RULE");
        LIMIT_KEYS.add("MAX_SQL_SIZE");
    }

    private static final Set<String> METRIC_KEYS = Sets.newHashSet();

    static {
        METRIC_KEYS.add("RESPONSE_TIME");
        METRIC_KEYS.add("AFFECTED_ROWS");
        METRIC_KEYS.add("FETCH_ROWS");
        METRIC_KEYS.add("PHY_AFFECTED_ROWS");
        METRIC_KEYS.add("PHYSICAL_SQL_COUNT");
        METRIC_KEYS.add("DN_REQUEST_COUNT");
        METRIC_KEYS.add("ACTIVE_SESSION");
        METRIC_KEYS.add("SQL_TYPE");
    }

    private static final Set<String> SQL_TYPE_OPERATORS = Sets.newHashSet();

    static {
        SQL_TYPE_OPERATORS.add("=");
    }

    private static final Set<String> SQL_TYPE_VALUES = Sets.newHashSet();

    static {
        SQL_TYPE_VALUES.add("SELECT");
        SQL_TYPE_VALUES.add("INSERT");
        SQL_TYPE_VALUES.add("UPDATE");
        SQL_TYPE_VALUES.add("DELETE");
        SQL_TYPE_VALUES.add("SELECT_FOR_UPDATE");
        SQL_TYPE_VALUES.add("REPLACE");
        SQL_TYPE_VALUES.add("INSERT_INTO_SELECT");
        SQL_TYPE_VALUES.add("REPLACE_INTO_SELECT");
        SQL_TYPE_VALUES.add("SELECT_UNION");
        SQL_TYPE_VALUES.add("SELECT_WITHOUT_TABLE");
        SQL_TYPE_VALUES.add("ALL");
    }

    public static final int MAX_CCL_RULE = 1000;

    public static final int DEFAULT_RULE_UPGRADE = 1;

    public static final int MAX_SQL_SIZE = 4096;

    public LogicalCreateCclTriggerHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalCcl plan = (LogicalCcl) logicalPlan;
        SqlCreateCclTrigger createCclTrigger = (SqlCreateCclTrigger) plan.getSqlDal();
        String triggerName = createCclTrigger.getTriggerName().getSimple();
        String schemaName = createCclTrigger.getSchemaName().getSimple();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(false);
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            cclTriggerAccessor.setConnection(metaDbConn);

            //check if the trigger name exists in the metd db
            List<CclTriggerRecord> existTriggers = cclTriggerAccessor.queryByIds(Lists.newArrayList(triggerName));
            if (CollectionUtils.isNotEmpty(existTriggers)) {
                if (createCclTrigger.isIfNotExits()) {
                    if (!executionContext.getExtraDatas().containsKey(ExecutionContext.FailedMessage)) {
                        executionContext.getExtraDatas()
                            .put(ExecutionContext.FailedMessage, Lists.newArrayListWithCapacity(1));
                    }
                    List<ExecutionContext.ErrorMessage> errorMessages =
                        (List<ExecutionContext.ErrorMessage>) executionContext.getExtraDatas()
                            .get(ExecutionContext.FailedMessage);
                    errorMessages.add(new ExecutionContext.ErrorMessage(ErrorCode.ERR_CCL.getCode(), null,
                        "The ccl trigger has been existing"));
                    return new AffectRowCursor(new int[] {0});
                } else {
                    logger.error("The trigger name has been existing.");
                    throw new TddlNestableRuntimeException("Failed. The trigger name has been existing.");
                }
            }

            CclTriggerRecord cclTriggerRecord = new CclTriggerRecord();
            cclTriggerRecord.id = triggerName;
            cclTriggerRecord.schema = schemaName;

            List<Pair<SqlNode, SqlNode>> ruleWiths = createCclTrigger.getRuleWiths();
            //不为空
            if (CollectionUtils.isEmpty(ruleWiths)) {
                logger.error("The with item list must not be empty");
                throw new TddlNestableRuntimeException("Failed. The with items list must not be empty.");
            }
            Map<String, Integer> withMap = Maps.newHashMap();
            for (Pair<SqlNode, SqlNode> sqlNodePair : ruleWiths) {
                String withKey = ((SqlIdentifier) sqlNodePair.left).getSimple();
                withKey = StringUtils.upperCase(withKey);
                //check the validation of the with key.
                if (!WITH_KEYS.contains(withKey)) {
                    logger.error("Invalid with key " + withKey);
                    throw new TddlNestableRuntimeException("Failed. Invalid with key " + withKey);
                }
                int value = ((SqlNumericLiteral) sqlNodePair.right).intValue(true);
                if (value < 0) {
                    throw new TddlNestableRuntimeException(
                        "Ccl rule with option " + withKey + " must not be less than 0.");
                }
                withMap.put(withKey, value);
            }
            if (!withMap.containsKey("MAX_CONCURRENCY")) {
                throw new TddlNestableRuntimeException("Ccl rule with option MAX_CONCURRENCY must be provided.");
            }

            List<CclCondition> cclRuleConditions = Lists.newArrayList();
            for (Map.Entry<String, Integer> entry : withMap.entrySet()) {
                CclCondition cclCondition = new CclCondition();
                cclCondition.setComparison("=");
                cclCondition.setSqlMetricName(entry.getKey());
                cclCondition.setValue(Long.valueOf(entry.getValue()));
                cclRuleConditions.add(cclCondition);
            }
            String withJson = JSON.toJSONString(cclRuleConditions);
            cclTriggerRecord.ruleConfig = withJson;

            //check the
            List<Pair<SqlNode, SqlNode>> limits = createCclTrigger.getLimits();
            if (CollectionUtils.isEmpty(limits)) {
                logger.error("The limit item list must not be empty");
                throw new TddlNestableRuntimeException("Failed. The limit item list must not be empty.");
            }
            Map<String, Integer> limitMap = Maps.newHashMap();
            for (Pair<SqlNode, SqlNode> sqlNodePair : limits) {
                String limitKey = ((SqlIdentifier) sqlNodePair.left).getSimple();
                limitKey = StringUtils.upperCase(limitKey);
                if (!LIMIT_KEYS.contains(limitKey)) {
                    logger.error("Invalid limit key " + limitKey);
                    throw new TddlNestableRuntimeException("Failed. Invalid limit key " + limitKey);
                }
                int value = ((SqlNumericLiteral) sqlNodePair.right).intValue(true);
                if (value < 0) {
                    throw new TddlNestableRuntimeException("Limit option " + limitKey + " must not be less than 0.");
                }
                limitMap.put(limitKey, value);
            }
            if (limitMap.isEmpty()) {
                throw new TddlNestableRuntimeException("Limit options must not be empty.");
            }
            cclTriggerRecord.maxSQLSize = MAX_SQL_SIZE;
            if (!limitMap.containsKey("MAX_CCL_RULE")) {
                throw new TddlNestableRuntimeException("The limit option MAX_CCL_RULE must be provided.");
            }
            cclTriggerRecord.maxCclRule = limitMap.get("MAX_CCL_RULE");
            if (cclTriggerRecord.maxCclRule > MAX_CCL_RULE) {
                throw new TddlNestableRuntimeException(
                    "The limit option MAX_CCL_RULE must not be larger than " + MAX_CCL_RULE);
            }
            if (limitMap.containsKey("MAX_SQL_SIZE")) {
                cclTriggerRecord.maxSQLSize = limitMap.get("MAX_SQL_SIZE");
            }
            if (limitMap.containsKey("QUERY_RULE_UPGRADE")) {
                cclTriggerRecord.ruleUpgrade = limitMap.get("QUERY_RULE_UPGRADE");
            } else {
                cclTriggerRecord.ruleUpgrade = DEFAULT_RULE_UPGRADE;
            }

            //deal with whens
            List<SqlNode> leftOperands = createCclTrigger.getLeftOperands();
            List<SqlNode> operators = createCclTrigger.getOperators();
            List<SqlNode> rightOperands = createCclTrigger.getRightOperands();
            if (CollectionUtils.isEmpty(leftOperands) || CollectionUtils.isEmpty(operators) || CollectionUtils
                .isEmpty(rightOperands)) {
                throw new TddlNestableRuntimeException("Trigger Conditions must not be empty.");
            }
            List<CclCondition> cclConditions = Lists.newArrayList();
            for (int i = 0; i < leftOperands.size(); ++i) {
                String metricKey = ((SqlIdentifier) leftOperands.get(i)).getSimple();
                metricKey = StringUtils.upperCase(metricKey);
                long value = CclSqlMetric.DEFAULT_VALUE;
                SqlNode rightOperand = rightOperands.get(i);
                if (rightOperand instanceof SqlCharStringLiteral && StringUtils
                    .equals("SQL_TYPE", StringUtils.upperCase(metricKey))) {
                    String sqlTypeValue = ((SqlCharStringLiteral) rightOperand).toValue();
                    sqlTypeValue = StringUtils.upperCase(sqlTypeValue);
                    if (SQL_TYPE_VALUES.contains(sqlTypeValue)) {
                        if (StringUtils.equals("ALL", sqlTypeValue)) {
                            continue;
                        }
                        value = SqlType.valueOf(sqlTypeValue).getI();
                    } else {
                        throw new TddlNestableRuntimeException(
                            "Invalid metric key: " + metricKey + " invalid value " + sqlTypeValue);
                    }
                }
                if (value == CclSqlMetric.DEFAULT_VALUE) {
                    value = ((SqlNumericLiteral) rightOperands.get(i)).longValue(true);
                }
                CclCondition cclCondition =
                    CclCondition.builder().sqlMetricName(metricKey)
                        .comparison(((SqlIdentifier) operators.get(i)).getSimple())
                        .value(value).build();
                cclConditions.add(cclCondition);
            }

            //check the invalidation of the ccl conditions
            for (CclCondition cclCondition : cclConditions) {
                String metricKey = cclCondition.getSqlMetricName();
                metricKey = StringUtils.upperCase(metricKey);
                if (!METRIC_KEYS.contains(StringUtils.upperCase(metricKey))) {
                    throw new TddlNestableRuntimeException("Invalid metric key: " + metricKey);
                }
                if (StringUtils.equals("SQL_TYPE", metricKey)) {
                    if (cclCondition.getValue() < 0) {
                        throw new TddlNestableRuntimeException(
                            "Invalid metric key: " + metricKey + " invalid value");
                    }
                    if (!SQL_TYPE_OPERATORS.contains(cclCondition.getComparison())) {
                        throw new TddlNestableRuntimeException(
                            "Invalid metric key: " + metricKey + " require that the operator should != or =");
                    }
                }

                //replace the name of the metric key
                if (StringUtils.equalsIgnoreCase(metricKey, "DN_REQUEST_COUNT")) {
                    cclCondition.setSqlMetricName("PHYSICAL_SQL_COUNT");
                }

            }

            cclTriggerRecord.conditions = JSON.toJSONString(cclConditions);
            cclTriggerRecord.cclRuleCount = 0;
            cclTriggerAccessor.insert(cclTriggerRecord);
            String dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
            MetaDbConfigManager metaDbConfigManager = MetaDbConfigManager.getInstance();
            metaDbConfigManager.notify(dataId, metaDbConn);
            MetaDbUtil.commit(metaDbConn);
            metaDbConfigManager.sync(dataId);
            return new AffectRowCursor(new int[] {1});
        } catch (Throwable throwable) {
            throw new TddlRuntimeException(ErrorCode.ERR_CCL, throwable.getMessage(), throwable);
        }
    }

}
