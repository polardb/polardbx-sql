package com.alibaba.polardbx.planner.values;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.alibaba.polardbx.planner.tpch.MppTpchPlan100gPartitionTest;
import org.junit.runners.Parameterized;

import java.util.List;

public class ValuesPlanTest extends PlanTestCommon {

    public ValuesPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                          String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        enableMpp = true;
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ValuesPlanTest.class);
    }

}

