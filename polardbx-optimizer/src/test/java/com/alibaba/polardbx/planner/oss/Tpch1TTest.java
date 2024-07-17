package com.alibaba.polardbx.planner.oss;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class Tpch1TTest extends ParameterizedTestCommon {
    public Tpch1TTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        //setExplainCost(true);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(Tpch1TTest.class);
    }
}
