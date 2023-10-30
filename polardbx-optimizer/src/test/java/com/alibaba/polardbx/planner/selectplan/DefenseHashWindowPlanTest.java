package com.alibaba.polardbx.planner.selectplan;

import com.alibaba.polardbx.planner.common.DefensePlannerTest;
import org.junit.runners.Parameterized;

import java.util.List;

public class DefenseHashWindowPlanTest extends DefensePlannerTest {
    public DefenseHashWindowPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        // TODO Auto-generated constructor stub
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(DefenseHashWindowPlanTest.class);
    }
}