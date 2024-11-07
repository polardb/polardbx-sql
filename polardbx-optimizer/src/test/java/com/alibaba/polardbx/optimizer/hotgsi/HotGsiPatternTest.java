package com.alibaba.polardbx.optimizer.hotgsi;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.hotevolution.HotGsiPattern;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.planner.common.HotGsiTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * check whether the plan fits HotGsiPattern
 */
public class HotGsiPatternTest extends HotGsiTestCommon {
    public HotGsiPatternTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HotGsiPatternTest.class);
    }

    protected String validator(SqlParameterized sqlParameterized, ExecutionPlan executionPlan,
                               ExecutionContext executionContext) {

        boolean evolute = HotGsiPattern.findPattern(executionPlan.getPlan()).getKey();

        if (PlannerContext.getPlannerContext(executionPlan.getPlan()).getJoinCount() > 0) {
            evolute = false;
        }
        String planStr = RelUtils
            .toString(executionPlan.getPlan(), OptimizerUtils.buildParam(sqlParameterized.getParameters()),
                RexUtils.getEvalFunc(executionContext), executionContext);

        String code = removeSubqueryHashCode(planStr, executionPlan.getPlan(),
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter(),
            executionContext.getSqlExplainLevel());

        return evolute + "\n" + code;
    }
}
