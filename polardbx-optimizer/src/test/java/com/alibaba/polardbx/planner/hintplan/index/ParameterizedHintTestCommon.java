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

package com.alibaba.polardbx.planner.hintplan.index;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator.CmdBean;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter.HintCollection;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenmo.cm
 */
@RunWith(EclipseParameterized.class)
public abstract class ParameterizedHintTestCommon extends PlanTestCommon {

    public ParameterizedHintTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(testSql, currentParameter, false);

        final Map<Integer, ParameterContext> param = new HashMap<>();

        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE,
            false);
        final SqlNodeList astList = parse(testSql, sqlParameterized, param, executionContext);

        final ExecutionPlan executionPlan = optimize(param, astList, executionContext);

        executionContext.setParams(new Parameters(param));
        String planStr = RelUtils.toString(executionPlan.getPlan(), param, null, executionContext);
        planStr = removeSubqueryHashCode(planStr, executionPlan.getPlan(), param);
        return planStr;
    }

    private ExecutionPlan optimize(Map<Integer, ParameterContext> param, SqlNodeList astList, ExecutionContext ec) {
        final SqlNode ast = astList.get(0);
        ExecutionPlan executionPlan;
        ec.setParams(new Parameters(param, false));

        ec.getExtraCmds().put(ConnectionProperties.PARALLELISM, enableParallelQuery ? -1 : 0);
        ec.getExtraCmds().put(ConnectionProperties.ENABLE_JOIN_CLUSTERING, enableJoinClustering);
        ec.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, enableMpp);
        ec.getExtraCmds().put(ConnectionProperties.ENABLE_AUTO_FORCE_INDEX, false);
        if (forceWorkloadTypeAP) {
            ec.getExtraCmds().put(ConnectionProperties.WORKLOAD_TYPE, "AP");
        }

        HintCollection hintCollection = null;
        if (ec.isUseHint()) {
            // init HINT
            final HintPlanner hintPlanner = HintPlanner.getInstance(appName, ec);
            final CmdBean cmdBean = new CmdBean(appName, ec.getExtraCmds(), ec.getGroupHint());
            hintCollection = hintPlanner.collectAndPreExecute(ast, cmdBean, false, ec);

            ec.setGroupHint(cmdBean.getGroupHint().toString());
            // rebuild plannerContext since executionContext has changed
            if (hintCollection.pushdownOriginSql()) {
                executionPlan = hintPlanner.direct(ast, cmdBean, hintCollection, param, ec.getSchemaName(), ec);
            } else if (hintCollection.cmdOnly()) {
                PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);

                if (ast instanceof SqlExplain) {
                    executionPlan = hintPlanner.pushdown(Planner.getInstance()
                            .getPlan(((SqlExplain) ast).getExplicandum(), plannerContext),
                        ast,
                        cmdBean,
                        hintCollection,
                        param,
                        ec.getExtraCmds(), ec);
                } else {
                    executionPlan = hintPlanner.pushdown(Planner.getInstance().getPlan(ast, plannerContext),
                        ast,
                        cmdBean,
                        hintCollection,
                        param,
                        ec.getExtraCmds(), ec);
                }
            } else {
                PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
                executionPlan = hintPlanner.getPlan(ast, plannerContext, ec);
            }
        } else {
            PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
            executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        }

        executionPlan.setExplain(false);
        if (null == hintCollection || hintCollection.usePostPlanner()) {
            /**
             * PostPlanner
             */
            executionPlan = PostPlanner.getInstance().optimize(executionPlan, ec);
        }
        return executionPlan;
    }

    private SqlNodeList parse(String testSql, SqlParameterized sqlParameterized, Map<Integer, ParameterContext> param,
                              ExecutionContext executionContext) {
        SqlNodeList astList;
        if (null != sqlParameterized) {
            param.putAll(OptimizerUtils.buildParam(sqlParameterized.getParameters(), executionContext));
            astList = new FastsqlParser()
                .parse(sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        } else {
            astList = new FastsqlParser().parse(testSql, executionContext);
        }
        return astList;
    }
}
