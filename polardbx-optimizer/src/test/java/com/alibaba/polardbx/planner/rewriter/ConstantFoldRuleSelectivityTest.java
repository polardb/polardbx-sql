package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(EclipseParameterized.class)
public class ConstantFoldRuleSelectivityTest extends ConstantFoldRuleTest {

    public ConstantFoldRuleSelectivityTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                           String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ConstantFoldRuleSelectivityTest.class);
    }

    protected void enableExplainCost(ExecutionContext executionContext) {
        executionContext.setCalcitePlanOptimizerTrace(new CalcitePlanOptimizerTrace());
        executionContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.setSqlExplainLevel(SqlExplainLevel.ALL_ATTRIBUTES));

    }
}
