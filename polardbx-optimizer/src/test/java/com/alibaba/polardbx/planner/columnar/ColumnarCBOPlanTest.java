package com.alibaba.polardbx.planner.columnar;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class ColumnarCBOPlanTest extends ParameterizedTestCommon {
    public ColumnarCBOPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ColumnarCBOPlanTest.class);
    }
}
