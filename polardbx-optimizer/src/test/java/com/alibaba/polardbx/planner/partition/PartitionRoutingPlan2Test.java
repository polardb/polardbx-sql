package com.alibaba.polardbx.planner.partition;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionRoutingPlan2Test extends PlanTestCommon {

    public PartitionRoutingPlan2Test(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);

    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PartitionRoutingPlan2Test.class);
    }

    @Test
    public void testPartitionRouting() {

        System.out.print("toBeAdd test case for ");
    }

}

