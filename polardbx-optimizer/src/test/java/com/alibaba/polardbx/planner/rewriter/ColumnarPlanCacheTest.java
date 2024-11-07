package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(EclipseParameterized.class)
public class ColumnarPlanCacheTest extends ConstantFoldRuleTest {

    public ColumnarPlanCacheTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ColumnarPlanCacheTest.class);
    }

    protected void enableExplainCost(ExecutionContext executionContext) {
        executionContext.setColumnarPlanCache(true);
    }
}
