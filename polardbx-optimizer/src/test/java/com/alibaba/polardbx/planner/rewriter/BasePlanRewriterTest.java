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

package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public abstract class BasePlanRewriterTest extends BasePlannerTest {

    public BasePlanRewriterTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        Planner planner = new RewriterPlanner();
        FastsqlParser fastSqlParser = new FastsqlParser();
        SqlNodeList astList = fastSqlParser.parse(testSql);

        SqlNode ast = astList.get(0);

        ExecutionContext executionContext = new ExecutionContext();
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName, executionContext.getExtraCmds(),
            executionContext.getGroupHint());

        executionContext.setParams(new Parameters());
        executionContext.getExtraCmds().put(ConnectionProperties.PARALLELISM, enableParallelQuery ? -1 : 0);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_JOIN_CLUSTERING, enableJoinClustering);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, enableMpp);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_LV_SUBQUERY_UNWRAP, false);
        if (forceWorkloadTypeAP) {
            executionContext.getExtraCmds().put(ConnectionProperties.WORKLOAD_TYPE, "AP");
        }

        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);

        if (partialAggBucketThreshold != -1) {
            executionContext.getExtraCmds().put(ConnectionProperties.PARTIAL_AGG_BUCKET_THRESHOLD,
                partialAggBucketThreshold);
        }

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        ExecutionPlan executionPlan = planner.getPlan(ast, plannerContext);
        RelDrdsWriter relWriter = new RelDrdsWriter();
        executionPlan.getPlan().explainForDisplay(relWriter);

        String planStr = RelUtils.toString(executionPlan.getPlan(), null, null, executionContext);

        return removeSubqueryHashCode(planStr, executionPlan.getPlan(), null);
    }

    protected static class RewriterPlanner extends Planner {
        @Override
        public RelNode optimize(RelNode input, PlannerContext plannerContext) {
            return optimizeBySqlWriter(input, plannerContext);
        }
    }
}
