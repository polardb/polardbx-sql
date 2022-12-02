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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryByDigest;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryByDigestEntry;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryElementByDigest;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.StatementSummarySyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStatementSummary;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author busu
 * date: 2021/11/15 8:37 下午
 */
public class InformationSchemaStatementSummaryHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaStatementSummaryHandler(
        VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        long timestamp = System.currentTimeMillis();
        StatementSummarySyncAction statementSummarySyncAction = new StatementSummarySyncAction();
        statementSummarySyncAction.setHistory(false);
        statementSummarySyncAction.setTimestamp(timestamp);
        String schema = SystemDbHelper.DEFAULT_DB_NAME;
        boolean local = !executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_REMOTE_SYNC_ACTION);
        buildFinalResultFromSync(statementSummarySyncAction, local, schema, cursor);
        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaStatementSummary;
    }

    public static StatementSummaryByDigestEntry createByMap(Map<String, Object> map) {
        StatementSummaryElementByDigest statementSummaryElementByDigest = new StatementSummaryElementByDigest();
        statementSummaryElementByDigest
            .setBeginTime(DataTypes.DatetimeType.convertFrom(map.get("BEGIN_TIME")).getTime());
        statementSummaryElementByDigest.setCount(DataTypes.LongType.convertFrom(map.get("COUNT")));
        statementSummaryElementByDigest.setErrorCount(DataTypes.LongType.convertFrom(map.get("ERROR_COUNT")));
        statementSummaryElementByDigest
            .setMaxAffectedRows(DataTypes.LongType.convertFrom(map.get("MAX_AFFECTED_ROWS")));
        statementSummaryElementByDigest
            .setSumAffectedRows(DataTypes.LongType.convertFrom(map.get("SUM_AFFECTED_ROWS")));
        statementSummaryElementByDigest
            .setMaxTransTime(DataTypes.LongType.convertFrom(map.get("MAX_TRANSACTION_TIME")));
        statementSummaryElementByDigest
            .setSumTransTime(DataTypes.LongType.convertFrom(map.get("SUM_TRANSACTION_TIME")));
        statementSummaryElementByDigest
            .setMaxResponseTime(DataTypes.LongType.convertFrom(map.get("MAX_RESPONSE_TIME")));
        statementSummaryElementByDigest
            .setSumResponseTime(DataTypes.LongType.convertFrom(map.get("SUM_RESPONSE_TIME")));
        statementSummaryElementByDigest
            .setSumPhysicalTime(DataTypes.LongType.convertFrom(map.get("SUM_PHYSICAL_TIME")));
        statementSummaryElementByDigest
            .setMaxPhysicalTime(DataTypes.LongType.convertFrom(map.get("MAX_PHYSICAL__TIME")));
        statementSummaryElementByDigest
            .setSumPhysicalExecCount(DataTypes.LongType.convertFrom(map.get("SUM_PHYSICAL_EXEC_COUNT")));
        statementSummaryElementByDigest
            .setMaxPhysicalExecCount(DataTypes.LongType.convertFrom(map.get("MAX_PHYSICAL_EXEC_COUNT")));
        statementSummaryElementByDigest
            .setMaxParseCpuTime(DataTypes.LongType.convertFrom(map.get("MAX_PARSE_CPU_TIME")));
        statementSummaryElementByDigest
            .setSumParseCpuTime(DataTypes.LongType.convertFrom(map.get("SUM_PARSE_CPU_TIME")));
        statementSummaryElementByDigest
            .setMaxExecPlanCpuTime(DataTypes.LongType.convertFrom(map.get("MAX_EXEC_PLAN_CPU_TIME")));
        statementSummaryElementByDigest
            .setSumExecPlanCpuTime(DataTypes.LongType.convertFrom(map.get("SUM_EXEC_PLAN_CPU_TIME")));
        statementSummaryElementByDigest
            .setMaxPhyFetchRows(DataTypes.LongType.convertFrom(map.get("MAX_PHYSICAL_FETCH_ROWS")));
        statementSummaryElementByDigest
            .setSumPhyFetchRows(DataTypes.LongType.convertFrom(map.get("SUM_PHYSICAL_FETCH_ROWS")));
        statementSummaryElementByDigest
            .setMaxPhysicalTime(DataTypes.LongType.convertFrom(map.get("MAX_PHYSICAL__TIME")));
        statementSummaryElementByDigest
            .setSumPhysicalTime(DataTypes.LongType.convertFrom(map.get("SUM_PHYSICAL_TIME")));
        statementSummaryElementByDigest
            .setFirstSeen(DataTypes.DatetimeType.convertFrom(map.get("FIRST_SEEN")).getTime());
        statementSummaryElementByDigest.setLastSeen(DataTypes.DatetimeType.convertFrom(map.get("LAST_SEEN")).getTime());

        String schema = DataTypes.StringType.convertFrom(map.get("SCHEMA"));
        int templateHash = DataTypes.IntegerType.convertFrom(map.get("TEMPLATE_ID"));
        int planHash = DataTypes.IntegerType.convertFrom(map.get("PLAN_HASH"));
        String sqlSample = DataTypes.StringType.convertFrom(map.get("SQL_SAMPLE"));
        String sqlTemplateText = DataTypes.StringType.convertFrom(map.get("SQL_TEMPLATE"));
        String sqlType = DataTypes.StringType.convertFrom(map.get("SQL_TYPE"));
        int prevTemplateHash = DataTypes.IntegerType.convertFrom(map.get("PREV_TEMPLATE_ID"));
        String prevSqlTemplate = DataTypes.StringType.convertFrom(map.get("PREV_SQL_TEMPLATE"));
        String sampleTraceId = DataTypes.StringType.convertFrom(map.get("SAMPLE_TRACE_ID"));
        String workloadType = DataTypes.StringType.convertFrom(map.get("WORKLOAD_TYPE"));
        String executeMode = DataTypes.StringType.convertFrom(map.get("EXECUTE_MODE"));
        StatementSummaryByDigest statementSummaryByDigest =
            new StatementSummaryByDigest(schema, templateHash, planHash, sqlSample, sqlTemplateText, sqlType,
                prevTemplateHash, prevSqlTemplate, sampleTraceId, workloadType, executeMode);
        StatementSummaryByDigestEntry statementSummaryByDigestEntry = new StatementSummaryByDigestEntry();
        statementSummaryByDigestEntry.setKey(statementSummaryByDigest);
        statementSummaryByDigestEntry.setValue(statementSummaryElementByDigest);
        return statementSummaryByDigestEntry;
    }

    public static void buildFinalResultFromSync(StatementSummarySyncAction statementSummarySyncAction, boolean local,
                                                String schema,
                                                ArrayResultCursor cursor) {

        List<List<Map<String, Object>>> syncResult = null;
        if (local) {
            String localServerKey = TddlNode.getServerKeyByNodeId(TddlNode.getNodeId());
            List<Map<String, Object>> result =
                SyncManagerHelper.sync(statementSummarySyncAction, schema, localServerKey);
            syncResult = Lists.newArrayListWithCapacity(1);
            syncResult.add(result);
        } else {
            syncResult = SyncManagerHelper.sync(statementSummarySyncAction, schema, SyncScope.CURRENT_ONLY);
        }

        Map<String, StatementSummaryByDigestEntry> aggResult = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(syncResult)) {
            for (int i = 0; i < syncResult.size(); ++i) {
                List<Map<String, Object>> oneSyncResult = syncResult.get(i);
                if (CollectionUtils.isNotEmpty(oneSyncResult)) {
                    for (int j = 0; j < oneSyncResult.size(); ++j) {
                        Map<String, Object> oneLine = oneSyncResult.get(j);
                        StatementSummaryByDigestEntry statementSummaryByDigestEntry = createByMap(oneLine);

                        String key = String.format("%s:%d:%d:%d:%d", statementSummaryByDigestEntry.getKey().getSchema(),
                            statementSummaryByDigestEntry.getKey().getTemplateHash(),
                            statementSummaryByDigestEntry.getKey().getPrevTemplateHash(),
                            statementSummaryByDigestEntry.getKey().getPlanHash(),
                            statementSummaryByDigestEntry.getValue().getBeginTime());

                        StatementSummaryByDigestEntry valueInMap = aggResult.get(key);
                        if (valueInMap == null) {
                            aggResult.put(key, statementSummaryByDigestEntry);
                        } else {
                            valueInMap.getValue().merge(statementSummaryByDigestEntry.getValue());
                        }
                    }
                }
            }
        }

        List<StatementSummaryByDigestEntry> result = Lists.newArrayList(aggResult.values());
        result.sort((e1, e2) -> {
            if (e1.getValue().getSumResponseTime() > e2.getValue().getSumResponseTime()) {
                return -1;
            } else if (e1.getValue().getSumResponseTime() < e2.getValue().getSumResponseTime()) {
                return 1;
            }
            if (e1.getValue().getCount() > e2.getValue().getCount()) {
                return 1;
            } else if (e1.getValue().getCount() < e2.getValue().getCount()) {
                return -1;
            }
            if (e1.getValue().getSumPhysicalExecCount() > e2.getValue().getSumPhysicalExecCount()) {
                return -1;
            } else if (e1.getValue().getSumPhysicalExecCount() < e2.getValue().getSumPhysicalExecCount()) {
                return 1;
            }
            return e1.getKey().getTemplateHash() - e2.getKey().getTemplateHash();
        });

        result.forEach((entry) -> {
            Date beginTime = new Date(entry.getValue().getBeginTime());
            String sqlType = entry.getKey().getSqlType();
            String templateId = TStringUtil.int2FixedLenHexStr(entry.getKey().getTemplateHash());
            String planHash = TStringUtil.int2FixedLenHexStr(entry.getKey().getPlanHash());
            String sqlTemplate = StringUtils.replace(entry.getKey().getSqlTemplateText(), "\n", " ");
            Long count = entry.getValue().getCount();
            Long errorCount = entry.getValue().getErrorCount();
            Long sumResponseTime = entry.getValue().getSumResponseTime();
            Long avgResponseTime = count == 0 ? 0 : sumResponseTime / count;
            Long maxResponseTime = entry.getValue().getMaxResponseTime();
            Long sumAffectedRows = entry.getValue().getSumAffectedRows();
            Long avgAffectedRows = count == 0 ? 0 : sumAffectedRows / count;
            Long maxAffectedRows = entry.getValue().getMaxAffectedRows();
            Long sumTransactionTime = entry.getValue().getSumTransTime();
            Long avgTransactionTime = count == 0 ? 0 : sumTransactionTime / count;
            Long maxTransactionTime = entry.getValue().getMaxTransTime();
            Long sumParseCpuTime = entry.getValue().getSumParseCpuTime();
            Long avgParseCpuTime = count == 0 ? 0 : sumParseCpuTime / count;
            Long maxParseCpuTime = entry.getValue().getMaxParseCpuTime();
            Long sumExecPlanCpuTime = entry.getValue().getSumExecPlanCpuTime();
            Long avgExecPlanCpuTime = count == 0 ? 0 : sumExecPlanCpuTime / count;
            Long maxExecPlanCpuTime = entry.getValue().getMaxExecPlanCpuTime();
            Long sumPhysicalExecCount = entry.getValue().getSumPhysicalExecCount();
            Long avgPhysicalExecCount = count == 0 ? 0 : sumPhysicalExecCount / count;
            Long maxPhysicalExecCount = entry.getValue().getMaxPhysicalExecCount();
            Long sumPhysicalFetchRows = entry.getValue().getSumPhyFetchRows();
            Long avgPhysicalFetchRows = count == 0 ? 0 : sumPhysicalFetchRows / count;
            Long maxPhysicalFetchRows = entry.getValue().getMaxPhyFetchRows();
            Long sumPhysicalTime = entry.getValue().getSumPhysicalTime();
            Long avgPhysicalTime = sumPhysicalExecCount == 0 ? 0 : sumPhysicalTime / sumPhysicalExecCount;
            Long maxPhysicalTime = entry.getValue().getMaxPhysicalTime();
            Date firstSeen = new Date(entry.getValue().getFirstSeen());
            Date lastSeen = new Date(entry.getValue().getLastSeen());
            String sqlSample = entry.getKey().getSqlSample();
            String prevTemplateId = TStringUtil.int2FixedLenHexStr(entry.getKey().getPrevTemplateHash());
            String prevSqlTemplate = entry.getKey().getPrevSqlTemplate();
            String sampleTraceId = entry.getKey().getSampleTraceId();
            String workloadType = entry.getKey().getWorkloadType();
            String executeMode = entry.getKey().getExecuteMode();
            cursor.addRow(new Object[] {
                beginTime,
                entry.getKey().getSchema(),
                sqlType,
                templateId,
                planHash,
                sqlTemplate,
                count,
                errorCount,
                sumResponseTime / 1000.0,
                avgResponseTime / 1000.0,
                maxResponseTime / 1000.0,
                sumAffectedRows,
                avgAffectedRows,
                maxAffectedRows,
                sumTransactionTime,
                avgTransactionTime,
                maxTransactionTime,
                sumParseCpuTime / 1000.0,
                avgParseCpuTime / 1000.0,
                maxParseCpuTime / 1000.0,
                sumExecPlanCpuTime / 1000.0,
                avgExecPlanCpuTime / 1000.0,
                maxExecPlanCpuTime / 1000.0,
                sumPhysicalTime / 1000.0,
                avgPhysicalTime / 1000.0,
                maxPhysicalTime / 1000.0,
                sumPhysicalExecCount,
                avgPhysicalExecCount,
                maxPhysicalExecCount,
                sumPhysicalFetchRows,
                avgPhysicalFetchRows,
                maxPhysicalFetchRows,
                firstSeen,
                lastSeen,
                sqlSample,
                prevTemplateId,
                prevSqlTemplate,
                sampleTraceId,
                workloadType,
                executeMode
            });
        });

    }

}
