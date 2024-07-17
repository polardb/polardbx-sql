package com.alibaba.polardbx.executor.mpp.operator;

import org.junit.runners.Parameterized;

import java.util.List;

public class FragmentRFScheduleTest extends PlanFragmentTestBase {
    public FragmentRFScheduleTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, FragmentRFScheduleTest.class.getSimpleName(), sqlIndex, sql, expectedPlan, lineNum,
            new PlanFragmentRFTester());
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(FragmentRFScheduleTest.class);
    }
}
