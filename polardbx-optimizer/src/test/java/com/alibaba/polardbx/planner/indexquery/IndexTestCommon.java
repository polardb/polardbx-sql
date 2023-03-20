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

package com.alibaba.polardbx.planner.indexquery;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.runner.RunWith;

@RunWith(EclipseParameterized.class)
public abstract class IndexTestCommon extends PlanTestCommon {

    public IndexTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        final SqlNodeList astList = new FastsqlParser().parse(testSql);

        SqlNode ast = astList.get(0);
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE,
            false);
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setParams(new Parameters());
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName, executionContext.getExtraCmds(),
            executionContext.getGroupHint());
        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        final RelNode relNode = executionPlan.getPlan();

        RelDrdsWriter relWriter = new RelDrdsWriter();
        relWriter.setExecutionContext(executionContext);
        relNode.explainForDisplay(relWriter);
        String plan = relWriter.asString();
        // System.out.println(plan);
        plan = removeSubqueryHashCode(plan, relNode, null);
        return plan;
    }
}
