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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.statementsummary.StatementSummaryManager;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryByDigestEntry;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.Iterator;

/**
 * @author busu
 * date: 2021/11/17 1:09 下午
 */
@Data
public class StatementSummarySyncAction implements ISyncAction {
    private boolean history;
    private long timestamp;

    @Override
    public ResultCursor sync() {
        Iterator<StatementSummaryByDigestEntry> statementSummaryByDigestEntries;
        if (history) {
            statementSummaryByDigestEntries = StatementSummaryManager.getInstance().getStmtHistorySummaries(timestamp);
        } else {
            statementSummaryByDigestEntries = StatementSummaryManager.getInstance().getCurrentStmtSummaries(timestamp);
        }
        ArrayResultCursor resultCursor = buildResultCursor();

        if (statementSummaryByDigestEntries != null) {
            while (statementSummaryByDigestEntries.hasNext()) {
                StatementSummaryByDigestEntry entry = statementSummaryByDigestEntries.next();
                Date beginTime = new Date(entry.getValue().getBeginTime());
                String schema = entry.getKey().getSchema();
                String sqlType = entry.getKey().getSqlType();
                int templateId = entry.getKey().getTemplateHash();
                int planHash = entry.getKey().getPlanHash();
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
                Long maxPhysicalTime = entry.getValue().getMaxPhysicalTime();
                Long avgPhysicalTime = count == 0 ? 0 : sumPhysicalTime / count;
                Date firstSeen = new Date(entry.getValue().getFirstSeen());
                Date lastSeen = new Date(entry.getValue().getLastSeen());
                String sqlSample = entry.getKey().getSqlSample();
                int prevTemplateId = entry.getKey().getPrevTemplateHash();
                String prevSqlTemplate = StringUtils.replace(entry.getKey().getPrevSqlTemplate(), "\n", " ");
                String sampleTraceId = entry.getKey().getSampleTraceId();
                String workloadType = entry.getKey().getWorkloadType();
                String executeMode = entry.getKey().getExecuteMode();
                resultCursor.addRow(new Object[] {
                    beginTime,
                    schema,
                    sqlType,
                    templateId,
                    planHash,
                    sqlTemplate,
                    count,
                    errorCount,
                    sumResponseTime,
                    avgResponseTime,
                    maxResponseTime,
                    sumAffectedRows,
                    avgAffectedRows,
                    maxAffectedRows,
                    sumTransactionTime,
                    avgTransactionTime,
                    maxTransactionTime,
                    sumParseCpuTime,
                    avgParseCpuTime,
                    maxParseCpuTime,
                    sumExecPlanCpuTime,
                    avgExecPlanCpuTime,
                    maxExecPlanCpuTime,
                    sumPhysicalExecCount,
                    avgPhysicalExecCount,
                    maxPhysicalExecCount,
                    sumPhysicalFetchRows,
                    avgPhysicalFetchRows,
                    maxPhysicalFetchRows,
                    sumPhysicalTime,
                    avgPhysicalTime,
                    maxPhysicalTime,
                    firstSeen,
                    lastSeen,
                    sqlSample,
                    prevTemplateId,
                    prevSqlTemplate,
                    sampleTraceId,
                    workloadType,
                    executeMode
                });
            }
        }
        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("STATEMENT_SUMMARY");
        result.addColumn("BEGIN_TIME", DataTypes.DatetimeType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("SQL_TYPE", DataTypes.StringType);
        result.addColumn("TEMPLATE_ID", DataTypes.IntegerType);
        result.addColumn("PLAN_HASH", DataTypes.IntegerType);
        result.addColumn("SQL_TEMPLATE", DataTypes.StringType);
        result.addColumn("COUNT", DataTypes.LongType);
        result.addColumn("ERROR_COUNT", DataTypes.LongType);

        result.addColumn("SUM_RESPONSE_TIME", DataTypes.LongType);
        result.addColumn("AVG_RESPONSE_TIME", DataTypes.LongType);
        result.addColumn("MAX_RESPONSE_TIME", DataTypes.LongType);

        result.addColumn("SUM_AFFECTED_ROWS", DataTypes.LongType);
        result.addColumn("AVG_AFFECTED_ROWS", DataTypes.LongType);
        result.addColumn("MAX_AFFECTED_ROWS", DataTypes.LongType);

        result.addColumn("SUM_TRANSACTION_TIME", DataTypes.LongType);
        result.addColumn("AVG_TRANSACTION_TIME", DataTypes.LongType);
        result.addColumn("MAX_TRANSACTION_TIME", DataTypes.LongType);

        result.addColumn("SUM_PARSE_CPU_TIME", DataTypes.LongType);
        result.addColumn("AVG_PARSE_CPU_TIME", DataTypes.LongType);
        result.addColumn("MAX_PARSE_CPU_TIME", DataTypes.LongType);

        result.addColumn("SUM_EXEC_PLAN_CPU_TIME", DataTypes.LongType);
        result.addColumn("AVG_EXEC_PLAN_CPU_TIME", DataTypes.LongType);
        result.addColumn("MAX_EXEC_PLAN_CPU_TIME", DataTypes.LongType);

        result.addColumn("SUM_PHYSICAL_EXEC_COUNT", DataTypes.LongType);
        result.addColumn("AVG_PHYSICAL_EXEC_COUNT", DataTypes.LongType);
        result.addColumn("MAX_PHYSICAL_EXEC_COUNT", DataTypes.LongType);

        result.addColumn("SUM_PHYSICAL_FETCH_ROWS", DataTypes.LongType);
        result.addColumn("AVG_PHYSICAL_FETCH_ROWS", DataTypes.LongType);
        result.addColumn("MAX_PHYSICAL_FETCH_ROWS", DataTypes.LongType);

        result.addColumn("SUM_PHYSICAL_TIME", DataTypes.LongType);
        result.addColumn("AVG_PHYSICAL_TIME", DataTypes.LongType);
        result.addColumn("MAX_PHYSICAL__TIME", DataTypes.LongType);

        result.addColumn("FIRST_SEEN", DataTypes.DatetimeType);
        result.addColumn("LAST_SEEN", DataTypes.DatetimeType);

        result.addColumn("SQL_SAMPLE", DataTypes.StringType);
        result.addColumn("PREV_TEMPLATE_ID", DataTypes.IntegerType);
        result.addColumn("PREV_SQL_TEMPLATE", DataTypes.StringType);
        result.addColumn("SAMPLE_TRACE_ID", DataTypes.StringType);
        result.addColumn("WORKLOAD_TYPE", DataTypes.StringType);
        result.addColumn("EXECUTE_MODE", DataTypes.StringType);

        return result;
    }

}

