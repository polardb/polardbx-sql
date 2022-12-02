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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsParameterizeSqlVisitor;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.commons.lang.StringUtils;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
@RunWith(EclipseParameterized.class)
public abstract class ParameterizedTestCommon extends PlanTestCommon {

    public ParameterizedTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    public void setExplainCost(boolean explainCost) {
        this.explainCost = explainCost;
    }

    /**
     * show the cost of plan
     */
    boolean explainCost = false;

    @Override
    protected String getPlan(String testSql) {
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setAppName(appName);
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(testSql), currentParameter, executionContext, false);
        Map<Integer, ParameterContext> param = OptimizerUtils.buildParam(sqlParameterized.getParameters());
        SqlNodeList astList = new FastsqlParser().parse(
            sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        SqlNode ast = astList.get(0);
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setParams(new Parameters(param, false));
        executionContext.getExtraCmds().putAll(configMaps);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName,
            executionContext.getExtraCmds(),
            executionContext.getGroupHint());
        if (explainCost) {
            executionContext.setCalcitePlanOptimizerTrace(new CalcitePlanOptimizerTrace());
            executionContext.getCalcitePlanOptimizerTrace()
                .ifPresent(x -> x.setSqlExplainLevel(SqlExplainLevel.ALL_ATTRIBUTES));
        }

        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
        processParameter(sqlParameterized, executionContext);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        executionPlan = PostPlanner.getInstance().optimize(executionPlan, executionContext);
        String planStr = RelUtils
            .toString(executionPlan.getPlan(), param, RexUtils.getEvalFunc(executionContext), executionContext);

        String code = removeSubqueryHashCode(planStr, executionPlan.getPlan(),
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter(),
            executionContext.getSqlExplainLevel());
        return code;
    }

    private void processParameter(SqlParameterized sqlParameterized, ExecutionContext executionContext) {
        if (sqlParameterized != null) {
            List<Object> p = sqlParameterized.getParameters();
            for (int i = 0; i < p.size(); i++) {
                if (p.get(i) instanceof DrdsParameterizeSqlVisitor.UserDefVariable) {
                    DrdsParameterizeSqlVisitor.UserDefVariable userDefVariable =
                        (DrdsParameterizeSqlVisitor.UserDefVariable) p.get(i);
                    Map<String, Object> userDefVariables = executionContext.getUserDefVariables();
                    p.set(i, userDefVariables.get(userDefVariable.getName().toLowerCase()));
                } else if (p.get(i) instanceof DrdsParameterizeSqlVisitor.SysDefVariable) {
                    DrdsParameterizeSqlVisitor.SysDefVariable sysDefVariable =
                        (DrdsParameterizeSqlVisitor.SysDefVariable) p.get(i);
                    String name = StringUtils.strip(sysDefVariable.getName().toLowerCase(), "`");
                    if ("last_insert_id".equals(name)) {
                        p.set(i, 10001L);
                    } else {
                        throw new RuntimeException("add more sys variables mock");
                    }
                }
            }
            Parameters parameters = executionContext.getParams();
            parameters.setParams(OptimizerUtils.buildParam(sqlParameterized.getParameters(), executionContext));
        }
    }
}
