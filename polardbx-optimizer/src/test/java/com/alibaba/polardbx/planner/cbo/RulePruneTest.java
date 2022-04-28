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

package com.alibaba.polardbx.planner.cbo;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * check the usage of cbo push join limit
 */
@RunWith(EclipseParameterized.class)
public class RulePruneTest extends BasePlannerTest {

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(RulePruneTest.class);
    }

    public RulePruneTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                         String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected void execSqlAndVerifyPlan(String testMethodName, Integer sqlIdx, String targetSql, String targetPlan,
                                        String expect, String nodetree) {

        System.out.println("Running test " + testMethodName + " - " + sqlIndex);
        System.out.println("link: xx.xx(" + testMethodName + ":" + lineNum + ")");
        getPlan(targetSql);
    }

    protected ExecutionContext getExecutionContext(SqlNode ast) {
        ExecutionContext executionContext = new ExecutionContext();
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setInternalSystemSql(false);
        executionContext.setUsingPhySqlCache(true);

        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName, executionContext.getExtraCmds(),
            executionContext.getGroupHint());

        executionContext.setParams(new Parameters());
        executionContext.getExtraCmds().put(ConnectionProperties.PARALLELISM, enableParallelQuery ? -1 : 0);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_JOIN_CLUSTERING, enableJoinClustering);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, enableMpp);
        executionContext.getExtraCmds().put(ConnectionProperties.STORAGE_SUPPORTS_BLOOM_FILTER,
            storageSupportsBloomFilter);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_PUSH_JOIN, false);

        if (forceWorkloadTypeAP) {
            executionContext.getExtraCmds().put(ConnectionProperties.WORKLOAD_TYPE, "AP");
        }
        if (inValuesThread > 1) {
            executionContext.getExtraCmds().put(ConnectionProperties.IN_SUB_QUERY_THRESHOLD, inValuesThread);
            executionContext.setSqlType(sqlType);
        }
        executionContext.setServerVariables(new HashMap<>());
        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);

        if (partialAggBucketThreshold != -1) {
            executionContext.getExtraCmds().put(ConnectionProperties.PARTIAL_AGG_BUCKET_THRESHOLD,
                partialAggBucketThreshold);
        }

        // enable cbo rule counter
        executionContext.setEnableRuleCounter(true);
        // disable push join limit
        executionContext.getExtraCmds().put(ConnectionProperties.CBO_RESTRICT_PUSH_JOIN_LIMIT, -1);
        return executionContext;
    }

    @Override
    protected String getPlan(String testSql) {
        this.cluster =
            SqlConverter.getInstance(new ExecutionContext()).createRelOptCluster(PlannerContext.EMPTY_CONTEXT);

        SqlNodeList astList = new FastsqlParser().parse(testSql);
        SqlNode ast = astList.get(0);

        ExecutionContext executionContext = getExecutionContext(ast);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        // reset cluster for every statement
        Planner.getInstance().getPlan(ast, plannerContext);
        long prevCount = executionContext.getRuleCount();

        ast = new FastsqlParser().parse(testSql).get(0);
        executionContext = getExecutionContext(ast);
        plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        // enable push join limit
        executionContext.getExtraCmds().put(ConnectionProperties.CBO_RESTRICT_PUSH_JOIN_LIMIT, 0);
        executionContext.getExtraCmds().put(ConnectionProperties.CBO_RESTRICT_PUSH_JOIN_COUNT, 0);
        Planner.getInstance().getPlan(ast, plannerContext);
        long curCount = executionContext.getRuleCount();
        assertTrue("previous is " + prevCount + " now is " + curCount, prevCount <= 0 || curCount < prevCount);

        return null;
    }
}
