package com.alibaba.polardbx.optimizer.hotgsi;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.GsiEvolutionInfo;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.HotGsiCapture;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.HotGsiEvolution;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.planner.common.HotGsiTestCommon;
import com.clearspring.analytics.util.Lists;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * test entry point of HotGsiEvolution
 */
public class HotGsiEvolutionTest extends HotGsiTestCommon {
    public HotGsiEvolutionTest(String caseName, int sqlIndex, String sql,
                               String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HotGsiEvolutionTest.class);
    }

    void invokeEvolve(SqlParameterized sqlParameterized, ExecutionPlan executionPlan,
                      ExecutionContext executionContext) {
        ExecutorService producer = null;
        try {
            producer = Executors.newFixedThreadPool(5);
            int threadSize = 5;

            // add task parallel
            List<Future<Boolean>> futures = Lists.newArrayList();
            for (int i = 0; i < threadSize; i++) {
                ExecutionPlan finalExecutionPlan = executionPlan;
                futures.add(
                    producer.submit(() -> {
                        GsiEvolutionInfo info = new GsiEvolutionInfo(sqlParameterized, executionContext,
                            finalExecutionPlan,
                            HotGsiCapture.candidateHotGsi(executionContext, executionPlan.getPlan()),
                            TStringUtil.int2FixedLenHexStr(sqlParameterized.getSql().hashCode()));
                        HotGsiEvolution.evolution(info);
                        return true;
                    }));
            }
            for (Future<Boolean> future : futures) {
                // all threads should return true
                Assert.assertTrue(future.get(5L, TimeUnit.SECONDS));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (producer != null) {
                producer.shutdownNow();
            }
        }
    }

    protected String validator(SqlParameterized sqlParameterized, ExecutionPlan executionPlan,
                               ExecutionContext executionContext) {
        PlanManager.getInstance().getBaselineMap().clear();

        invokeEvolve(sqlParameterized, executionPlan, executionContext);
        Assert.assertTrue(PlanManager.getInstance().getBaselineMap(appName).size() == 1);
        BaselineInfo baselineInfo = PlanManager.getInstance().getBaselineMap(appName).get(sqlParameterized.getSql());
        Assert.assertTrue(baselineInfo != null);
        Assert.assertTrue(baselineInfo.isHotEvolution());
        Assert.assertTrue(baselineInfo.getAcceptedPlans().size() == baselineInfo.getPlans().size());

        String planStr = RelUtils
            .toString(executionPlan.getPlan(), OptimizerUtils.buildParam(sqlParameterized.getParameters()),
                RexUtils.getEvalFunc(executionContext), executionContext);

        String code = removeSubqueryHashCode(planStr, executionPlan.getPlan(),
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter(),
            executionContext.getSqlExplainLevel());

        return baselineInfo.getAcceptedPlans().size() + "\n" + code;
    }
}
