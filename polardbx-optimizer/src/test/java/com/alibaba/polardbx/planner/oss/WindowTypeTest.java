package com.alibaba.polardbx.planner.oss;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class WindowTypeTest extends PlanTestCommon {

    public WindowTypeTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        enableMpp = true;
        forceWorkloadTypeAP = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(WindowTypeTest.class);
    }
}
