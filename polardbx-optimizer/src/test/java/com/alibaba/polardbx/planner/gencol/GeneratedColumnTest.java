package com.alibaba.polardbx.planner.gencol;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

public class GeneratedColumnTest extends ParameterizedTestCommon {
    public GeneratedColumnTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(GeneratedColumnTest.class);
    }
}
