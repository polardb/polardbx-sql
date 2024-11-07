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
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.planmanager.DRDSRelJsonReader;
import com.alibaba.polardbx.optimizer.planmanager.DRDSRelJsonWriter;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author chenghui.lch 2018年1月3日 下午5:01:30
 * @since 5.0.0
 */
@RunWith(EclipseParameterized.class)
public abstract class PlanTestCommon extends BasePlannerTest {

    public PlanTestCommon(String caseName, String targetEnvFile) {
        super(caseName, targetEnvFile);
    }

    public PlanTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    public PlanTestCommon(String caseName, String targetEnvFile, int sqlIndex, String sql, String expectedPlan,
                          String lineNum) {
        super(caseName, targetEnvFile, sqlIndex, sql, expectedPlan, lineNum);
    }

    public PlanTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum,
                          String expect, String nodetree, String struct) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum, expect, nodetree, struct);
    }

    protected void assertPlanProperty(RelNode plan) {

    }

    protected SqlNode parserSqlNode(ExecutionContext context, String testSql) {
        SqlNodeList astList = new FastsqlParser().parse(testSql, context);
        SqlNode ast = astList.get(0);
        return ast;
    }

    @Override
    protected String getPlan(String testSql) {
        final String[] planStr = new String[1];

        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan executionPlan = getExecutionPlan(testSql, executionContext);
        assertPlanProperty(executionPlan.getPlan());

        planStr[0] = returnPlanStr(executionContext, executionPlan.getPlan());

        return removeSubqueryHashCode(planStr[0], executionPlan.getPlan(), null);
    }

    protected ExecutionPlan getExecutionPlan(String testSql, ExecutionContext executionContext) {
        executionContext.setParams(new Parameters());
        SqlNode ast = parserSqlNode(executionContext, testSql);

        this.cluster =
            SqlConverter.getInstance(executionContext).createRelOptCluster(PlannerContext.EMPTY_CONTEXT);

        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setInternalSystemSql(false);
        executionContext.setUsingPhySqlCache(true);

        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName, executionContext.getExtraCmds(),
            executionContext.getGroupHint());

        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_AUTO_FORCE_INDEX, enableAutoForceIndex);
        executionContext.getExtraCmds().putAll(configMaps);
        executionContext.getExtraCmds().put(ConnectionProperties.PARALLELISM, enableParallelQuery ? -1 : 0);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_JOIN_CLUSTERING, enableJoinClustering);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, enableMpp);
        executionContext.getExtraCmds().put(ConnectionProperties.STORAGE_SUPPORTS_BLOOM_FILTER,
            storageSupportsBloomFilter);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_RUNTIME_FILTER_XXHASH,
            storageUsingXxHashInBloomFilter);
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

        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_SCALE_OUT_FEATURE,
            false);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);
        // reset cluster for every statement
        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        executionPlan = PostPlanner.getInstance().optimize(executionPlan, executionContext);

        if (enablePlanManagementTest) {
            /** test plan externalization */
            final DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
            executionPlan.getPlan().explain(writer);
            String jsonString = writer.asString();
            final DRDSRelJsonReader reader = new DRDSRelJsonReader(executionPlan.getPlan().getCluster(),
                SqlConverter.getInstance(appName, executionContext).getCatalog(),
                null,
                false);
            RelNode node;
            try {
                node = reader.read(jsonString);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            executionPlan.setPlan(node);
        }
        return executionPlan;
    }

    protected String returnPlanStr(ExecutionContext executionContext, RelNode plan) {
        return RelUtils.toString(plan, executionContext.getParams().getCurrentParameter(),
            RexUtils.getEvalFunc(executionContext), executionContext);
    }
}
