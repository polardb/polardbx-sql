package com.alibaba.polardbx.planner.forceindex;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class PaginationIgnoreTest extends ParameterizedTestCommon {
    public PaginationIgnoreTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PaginationIgnoreTest.class);
    }

}
