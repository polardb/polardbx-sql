package com.alibaba.polardbx.planner.partition;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author dylan
 */
public class PartitionDistinctAggTest extends PlanTestCommon {

    public PartitionDistinctAggTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PartitionDistinctAggTest.class);
    }

}
