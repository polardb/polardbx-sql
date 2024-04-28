package com.alibaba.polardbx.planner.cbo;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class CBOIndexSelectionPruneTest extends PlanTestCommon {

    public CBOIndexSelectionPruneTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(CBOIndexSelectionPruneTest.class);
    }

}