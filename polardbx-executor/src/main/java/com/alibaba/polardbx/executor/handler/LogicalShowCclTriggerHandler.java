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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.ccl.common.CclCondition;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlShowCclTrigger;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2021/4/18 4:48 下午
 */
public class LogicalShowCclTriggerHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowCclTriggerHandler.class);

    public LogicalShowCclTriggerHandler(IRepository repo) {
        super(repo);
    }

    public Cursor handle(SqlDal sqlDal, ExecutionContext executionContext) {
        SqlShowCclTrigger sqlShowCclTrigger = (SqlShowCclTrigger) sqlDal;
        List<CclTriggerRecord> allRecords = Lists.newArrayList();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            metaDbConn.setAutoCommit(true);
            cclTriggerAccessor.setConnection(metaDbConn);
            if (sqlShowCclTrigger.isShowAll()) {
                List<CclTriggerRecord> queriedCclTriggers = cclTriggerAccessor.query();
                allRecords.addAll(queriedCclTriggers);
            } else {
                if (CollectionUtils.isNotEmpty(sqlShowCclTrigger.getNames())) {
                    List<String> triggerNames = sqlShowCclTrigger.getNames().stream().map((e) -> e.getSimple()).collect(
                        Collectors.toList());
                    List<CclTriggerRecord> queriedCclTriggers = cclTriggerAccessor.queryByIds(triggerNames);
                    allRecords.addAll(queriedCclTriggers);
                }
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CCL, " Failed to SHOW CCL_TRIGGER", e);
        }
        allRecords.sort(Comparator.comparing((e) -> -(e.priority)));
        ArrayResultCursor arrayResultCursor = buildResult(allRecords);
        return arrayResultCursor;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalCcl plan = (LogicalCcl) logicalPlan;
        SqlShowCclTrigger sqlShowCclTrigger = (SqlShowCclTrigger) plan.getSqlDal();
        return handle(sqlShowCclTrigger, executionContext);
    }

    private ArrayResultCursor buildResult(List<CclTriggerRecord> cclTriggerRecords) {
        ArrayResultCursor result = new ArrayResultCursor("Ccl_Trigger");
        result.addColumn("NO.", DataTypes.IntegerType);
        result.addColumn("TRIGGER_NAME", DataTypes.StringType);
        result.addColumn("CCL_RULE_COUNT", DataTypes.IntegerType);
        result.addColumn("DATABASE", DataTypes.StringType);
        result.addColumn("CONDITIONS", DataTypes.StringType);
        result.addColumn("RULE_CONFIG", DataTypes.StringType);
        result.addColumn("QUERY_RULE_UPGRADE", DataTypes.LongType);
        result.addColumn("MAX_CCL_RULE", DataTypes.LongType);
        result.addColumn("MAX_SQL_SIZE", DataTypes.IntegerType);
        result.addColumn("CREATED_TIME", DataTypes.DatetimeType);
        result.initMeta();

        int no = 1;
        for (CclTriggerRecord cclTriggerRecord : cclTriggerRecords) {
            String triggerName = cclTriggerRecord.id;
            Integer cclRuleCount = cclTriggerRecord.cclRuleCount;
            String database = cclTriggerRecord.schema;
            String formattedRuleConditions = formatCclCondition(cclTriggerRecord.conditions);
            String formattedRuleConfig = formatCclCondition(cclTriggerRecord.ruleConfig);
            Integer ruleUpgrade = cclTriggerRecord.ruleUpgrade;
            Integer maxCclRule = cclTriggerRecord.maxCclRule;
            Integer maxSqlSize = cclTriggerRecord.maxSQLSize;
            Date createdDate = cclTriggerRecord.gmtCreated;
            result.addRow(new Object[] {
                no++, triggerName, cclRuleCount, database, formattedRuleConditions, formattedRuleConfig, ruleUpgrade,
                maxCclRule,
                maxSqlSize, createdDate});
        }

        return result;
    }

    private String formatCclCondition(String conditions) {
        if (StringUtils.isEmpty(conditions)) {
            return StringUtils.EMPTY;
        }
        List<CclCondition> cclConditions = JSON.parseArray(conditions, CclCondition.class);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cclConditions.size(); ++i) {
            CclCondition cclCondition = cclConditions.get(i);
            if (StringUtils.equalsIgnoreCase(cclCondition.getSqlMetricName(), "PHYSICAL_SQL_COUNT")) {
                cclCondition.setSqlMetricName("DN_REQUEST_COUNT");
            }
            sb.append(cclCondition.getSqlMetricName());
            sb.append(" ");
            sb.append(cclCondition.getComparison());
            sb.append(" ");
            if (StringUtils.equalsIgnoreCase(cclCondition.getSqlMetricName(), "SQL_TYPE")) {
                sb.append("'");
                sb.append(SqlType.getValueFromI((int) cclCondition.getValue()).toString());
                sb.append("'");
            } else {
                sb.append(cclCondition.getValue());
            }
            if (i != cclConditions.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

}
