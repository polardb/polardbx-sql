package com.alibaba.polardbx.planner.joinplan;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * can't be opensource
 */
public class BKAJoinSelectTest extends ParameterizedTestCommon {
    public BKAJoinSelectTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(BKAJoinSelectTest.class);
    }
}
