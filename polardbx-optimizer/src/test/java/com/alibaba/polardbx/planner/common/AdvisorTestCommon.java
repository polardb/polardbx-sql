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

package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.index.AdviceResult;
import com.alibaba.polardbx.optimizer.index.IndexAdvisor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.Map;
import java.util.Set;

/**
 * @author dylan
 */
public abstract class AdvisorTestCommon extends PlanTestCommon {

    public IndexAdvisor.AdviseType adviseType = IndexAdvisor.AdviseType.LOCAL_INDEX;

    public AdvisorTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        String planStr;
        ExecutionContext executionContext = new ExecutionContext();

        SqlNodeList astList = new FastsqlParser().parse(testSql, executionContext);

        SqlNode ast = astList.get(0);

        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setInternalSystemSql(false);
        executionContext.setUsingPhySqlCache(true);

        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName, executionContext.getExtraCmds(),
            executionContext.getGroupHint());
        executionContext.setRandomPhyTableEnabled(false);

        executionContext.setParams(new Parameters());
        executionContext.getExtraCmds().put(ConnectionProperties.PARALLELISM, enableParallelQuery ? -1 : 0);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_JOIN_CLUSTERING, enableJoinClustering);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, enableMpp);
        executionContext.getExtraCmds().put(ConnectionProperties.STORAGE_SUPPORTS_BLOOM_FILTER,
            storageSupportsBloomFilter);
        if (forceWorkloadTypeAP) {
            executionContext.getExtraCmds().put(ConnectionProperties.WORKLOAD_TYPE, "AP");
        }
        if (inValuesThread > 1) {
            executionContext.getExtraCmds().put(ConnectionProperties.IN_SUB_QUERY_THRESHOLD, inValuesThread);
            executionContext.setSqlType(sqlType);
        }

        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);

        if (partialAggBucketThreshold != -1) {
            executionContext.getExtraCmds().put(ConnectionProperties.PARTIAL_AGG_BUCKET_THRESHOLD,
                partialAggBucketThreshold);
        }
        ExplainResult explainResult = new ExplainResult();
        explainResult.explainMode = ExplainResult.ExplainMode.ADVISOR;
        executionContext.setExplain(explainResult);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        executionPlan = PostPlanner.getInstance().optimize(executionPlan, executionContext);

        IndexAdvisor indexAdvisor = new IndexAdvisor(executionPlan, executionContext);
        AdviceResult adviceResult = indexAdvisor.advise(adviseType);

        RelNode advisedPlan = adviceResult.getAfterPlan();

        if (advisedPlan == null) {
            throw new AssertionError("advisor test can not advise an index");
        }

        // deal with advising broadcast table
        if (adviseType == IndexAdvisor.AdviseType.BROADCAST) {
            Map<String, Set<String>> broadcasts = adviceResult.getConfiguration().getBroadcastTable();
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Set<String>> entry : broadcasts.entrySet()) {
                if (appName.equals(entry.getKey())) {
                    for (String table : entry.getValue()) {
                        sb.append(' ').append(table);
                    }
                } else {
                    for (String table : entry.getValue()) {
                        sb.append(' ').append(entry.getKey()).append('.').append(table);
                    }
                }
            }
            return sb.toString().trim();
        }
        assertPlanProperty(advisedPlan);
        planStr = adviceResult.getAfterPlanForDisplay();
        return removeSubqueryHashCode(planStr, advisedPlan, null);
    }
}
