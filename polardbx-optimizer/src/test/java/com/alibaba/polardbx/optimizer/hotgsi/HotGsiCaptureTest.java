package com.alibaba.polardbx.optimizer.hotgsi;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.HotGsiCapture;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.HotGsiEvolution;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.planner.common.HotGsiTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * test HotGsiCapture
 */
public class HotGsiCaptureTest extends HotGsiTestCommon {
    public HotGsiCaptureTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HotGsiCaptureTest.class);
    }

    @Override
    protected String validator(SqlParameterized sqlParameterized, ExecutionPlan executionPlan,
                               ExecutionContext executionContext) {
        PlanCache.CacheKey cacheKey =
            PlanCache.getCacheKey(executionContext.getSchemaName(), sqlParameterized, executionContext, false);
        boolean evolute = HotGsiCapture.capture(
            sqlParameterized,
            executionContext,
            executionPlan,
            HotGsiEvolution.getInstance(),
            cacheKey.getTemplateId());

        String planStr = RelUtils
            .toString(executionPlan.getPlan(), OptimizerUtils.buildParam(sqlParameterized.getParameters()),
                RexUtils.getEvalFunc(executionContext), executionContext);

        String code = removeSubqueryHashCode(planStr, executionPlan.getPlan(),
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter(),
            executionContext.getSqlExplainLevel());
        return evolute + "\n" + code;
    }
}
