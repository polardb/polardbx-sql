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

import com.alibaba.polardbx.gms.sync.SyncScope;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ShowCclStatsSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlShowCclRule;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2020/10/27 1:27 下午
 */
public class LogicalShowCclRuleHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowCclRuleHandler.class);

    public LogicalShowCclRuleHandler(IRepository repo) {
        super(repo);
    }

    public Cursor handle(SqlDal sqlDal, ExecutionContext executionContext) {
        SqlShowCclRule sqlNode = (SqlShowCclRule) sqlDal;
        List<CclRuleRecord> records = Lists.newArrayList();
        try (Connection metaDbConn =  MetaDbUtil.getConnection() ) {
            metaDbConn.setAutoCommit(true);
            CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
            cclRuleAccessor.setConnection(metaDbConn);
            if (sqlNode.isShowAll()) {
                List<CclRuleRecord> allRecords = cclRuleAccessor.query();
                records.addAll(allRecords);
            } else {
                List<String> ruleNames =
                    sqlNode.getRuleNames().stream().map((e) -> e.getSimple()).collect(Collectors.toList());
                List<CclRuleRecord> queryRecords = cclRuleAccessor.queryByIds(ruleNames);
                records.addAll(queryRecords);
            }

        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CCL, " Failed to SHOW CCL_RULE", e);
        }
        ArrayResultCursor arrayResultCursor = buildResult(records, executionContext);
        return arrayResultCursor;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalCcl plan = (LogicalCcl) logicalPlan;
        SqlShowCclRule sqlNode = (SqlShowCclRule) plan.getSqlDal();
        return handle(sqlNode, executionContext);
    }

    private ArrayResultCursor buildResult(List<CclRuleRecord> records, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("Ccl_Rule");
        result.addColumn("NO.", DataTypes.IntegerType);
        result.addColumn("RULE_NAME", DataTypes.StringType);
        result.addColumn("RUNNING", DataTypes.IntegerType);
        result.addColumn("WAITING", DataTypes.IntegerType);
        result.addColumn("KILLED", DataTypes.LongType);
        result.addColumn("MATCH_HIT_CACHE", DataTypes.LongType);
        result.addColumn("TOTAL_MATCH", DataTypes.LongType);
        result.addColumn("ACTIVE_NODE_COUNT", DataTypes.IntegerType);
        result.addColumn("MAX_CONCURRENCY_PER_NODE", DataTypes.IntegerType);
        result.addColumn("WAIT_QUEUE_SIZE_PER_NODE", DataTypes.IntegerType);
        result.addColumn("WAIT_TIMEOUT", DataTypes.IntegerType);
        result.addColumn("FAST_MATCH", DataTypes.IntegerType);
        result.addColumn("LIGHT_WAIT", DataTypes.IntegerType);
        result.addColumn("SQL_TYPE", DataTypes.StringType);
        result.addColumn("USER", DataTypes.StringType);
        result.addColumn("TABLE", DataTypes.StringType);
        result.addColumn("KEYWORDS", DataTypes.StringType);
        result.addColumn("TEMPLATE_ID", DataTypes.StringType);
        result.addColumn("QUERY", DataTypes.StringType);
        result.addColumn("CREATED_TIME", DataTypes.DatetimeType);
        result.initMeta();

        List<List<Map<String, Object>>> syncResult = SyncManagerHelper.sync(new ShowCclStatsSyncAction(),
            SystemDbHelper.DEFAULT_DB_NAME,
            SyncScope.CURRENT_ONLY);
        Map<String, Map<String, Long>> aggSynResult = Maps.newHashMapWithExpectedSize(syncResult.size());
        syncResult.forEach((e) -> {
            if (e == null) {
                return;
            }
            e.forEach(
                (innere) -> {
                    String ruleName = (String) innere.get(ShowCclStatsSyncAction.ID);
                    Integer running = (Integer) innere.get(ShowCclStatsSyncAction.RUNNING);
                    Integer waiting = (Integer) innere.get(ShowCclStatsSyncAction.WAITING);
                    Long killed = (Long) innere.get(ShowCclStatsSyncAction.KILLED);
                    Long matchHitCache = (Long) innere.get(ShowCclStatsSyncAction.MATCH_HIT_CACHE);
                    Long totalMatchCount = (Long) innere.get(ShowCclStatsSyncAction.TOTAL_MATCH);
                    Map<String, Long> ruleRecord = aggSynResult.get(ruleName);
                    if (ruleRecord == null) {
                        ruleRecord = Maps.newHashMapWithExpectedSize(3);
                        aggSynResult.put(ruleName, ruleRecord);
                        ruleRecord.put(ShowCclStatsSyncAction.RUNNING, 0L);
                        ruleRecord.put(ShowCclStatsSyncAction.WAITING, 0L);
                        ruleRecord.put(ShowCclStatsSyncAction.KILLED, 0L);
                        ruleRecord.put(ShowCclStatsSyncAction.MATCH_HIT_CACHE, 0L);
                        ruleRecord.put(ShowCclStatsSyncAction.TOTAL_MATCH, 0L);
                    }
                    ruleRecord.put(ShowCclStatsSyncAction.RUNNING,
                        ruleRecord.get(ShowCclStatsSyncAction.RUNNING) + Long.valueOf(running));
                    ruleRecord.put(ShowCclStatsSyncAction.WAITING,
                        ruleRecord.get(ShowCclStatsSyncAction.WAITING) + Long.valueOf(waiting));
                    ruleRecord.put(ShowCclStatsSyncAction.KILLED,
                        ruleRecord.get(ShowCclStatsSyncAction.KILLED) + Long.valueOf(killed));
                    ruleRecord.put(ShowCclStatsSyncAction.MATCH_HIT_CACHE,
                        ruleRecord.get(ShowCclStatsSyncAction.MATCH_HIT_CACHE) + Long.valueOf(matchHitCache));
                    ruleRecord.put(ShowCclStatsSyncAction.TOTAL_MATCH,
                        ruleRecord.get(ShowCclStatsSyncAction.TOTAL_MATCH) + Long.valueOf(totalMatchCount));
                }
            );
        });

        int num = 1;
        for (CclRuleRecord record : records) {
            Map<String, Long> syncRecord = aggSynResult.get(record.id);
            long running = 0;
            long waiting = 0;
            long killed = 0;
            long matchHitCache = 0;
            long totalMatchCount = 0;
            if (syncRecord != null) {
                running = syncRecord.get(ShowCclStatsSyncAction.RUNNING);
                waiting = syncRecord.get(ShowCclStatsSyncAction.WAITING);
                killed = syncRecord.get(ShowCclStatsSyncAction.KILLED);
                matchHitCache = syncRecord.get(ShowCclStatsSyncAction.MATCH_HIT_CACHE);
                totalMatchCount = syncRecord.get(ShowCclStatsSyncAction.TOTAL_MATCH);
            }
            result.addRow(new Object[] {
                num++,
                record.id,
                running,
                waiting,
                killed,
                matchHitCache,
                totalMatchCount,
                syncResult.size(),
                record.parallelism,
                record.queueSize,
                record.waitTimeout,
                record.fastMatch,
                record.lightWait,
                record.sqlType,
                record.userName == null ? "ALL" : record.userName + "@" + record.clientIp,
                record.dbName + "." + record.tableName,
                record.keywords,
                record.templateId,
                record.query,
                record.gmtCreated
            });
        }
        return result;
    }

}
