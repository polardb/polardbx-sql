package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.druid.util.StringUtils;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.Map;
import java.util.Set;

public abstract class AdvisorUserCaseTestCommon extends PlanTestCommon {
    public AdvisorUserCaseTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
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

        executionContext.setParams(new Parameters());
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_AUTO_FORCE_INDEX, enableAutoForceIndex);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_RANDOM_PHY_TABLE_NAME, false);
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

        IndexAdvisor indexAdvisor = new IndexAdvisor(executionPlan, plannerContext.getExecutionContext());

        AdviceResult bestResult = null;
        IndexAdvisor.AdviseType[] types = {
            IndexAdvisor.AdviseType.LOCAL_INDEX,
            IndexAdvisor.AdviseType.GLOBAL_INDEX,
            IndexAdvisor.AdviseType.BROADCAST};
        for (IndexAdvisor.AdviseType type : types) {
            AdviceResult adviceResult = indexAdvisor.advise(type);
            if (adviceResult.getAfterPlan() != null) {
                if (bestResult == null || adviceResult.getConfiguration().getAfterCost()
                    .isLt(bestResult.getConfiguration().getAfterCost())) {
                    bestResult = adviceResult;
                }
            }
        }
        if (bestResult == null) {
            bestResult =
                indexAdvisor.advise(IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX);
        }

        if (bestResult == null) {
            throw new AssertionError("advisor test can not advise an index");
        }

        // deal with advising broadcast table
        if (!StringUtils.isEmpty(bestResult.getConfiguration().broadcastSql())) {
            Map<String, Set<String>> broadcasts = bestResult.getConfiguration().getBroadcastTable();
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
        assertPlanProperty(bestResult.getAfterPlan());
        planStr = bestResult.getAfterPlanForDisplay();
        return removeSubqueryHashCode(planStr, bestResult.getAfterPlan(), null);
    }
}
