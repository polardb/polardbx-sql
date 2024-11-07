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

package com.alibaba.polardbx.planner.hintplan;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner4Test;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator.CmdBean;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter.HintCollection;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.HashMap;

/**
 * @author chenmo.cm
 */
public abstract class HintPlanTestCommon extends PlanTestCommon {

    public HintPlanTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE,
            false);
        final SqlNodeList astList = new FastsqlParser().parse(testSql, executionContext);
        final SqlNode ast = astList.get(0);

        final HintPlanner4Test hintPlanner = HintPlanner4Test.getInstance(appName);

        final CmdBean cmdBean = new CmdBean(appName, new HashMap<>(), "");

        HintCollection hintCollection = hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
        executionContext.setOriginSqlPushdownOrRoute(hintCollection.pushdownSqlOrRoute());
        executionContext.setServerVariables(new HashMap<>());

        ExecutionPlan executionPlan;

        PlannerContext plannerContext = new PlannerContext(executionContext);
        plannerContext.setExtraCmds(executionContext.getExtraCmds());
        plannerContext.getExtraCmds().put(ConnectionProperties.ENABLE_AUTO_FORCE_INDEX, false);
        plannerContext.setExecutionContext(executionContext);
        plannerContext.setParams(new Parameters());
        if (hintCollection.cmdOnly()) {
            executionPlan = hintPlanner.pushdown(Planner.getInstance().getPlan(ast, plannerContext),
                ast,
                cmdBean,
                hintCollection,
                new HashMap<>(),
                executionContext.getExtraCmds(), executionContext);
        } else {
            executionPlan = hintPlanner.getPlan(ast, plannerContext, executionContext);
        }

        return RelUtils
            .toString(executionPlan.getPlan(), null, RexUtils.getEvalFunc(executionContext), executionContext);
    }
}
