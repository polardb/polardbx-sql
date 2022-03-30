package com.alibaba.polardbx.planner.advisor;

import com.alibaba.polardbx.optimizer.index.IndexAdvisor;
import com.alibaba.polardbx.planner.common.AdvisorTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author shengyu
 */
public class BroadcastIndexAdvisorPlanTest extends AdvisorTestCommon {

    public BroadcastIndexAdvisorPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                         String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        adviseType = IndexAdvisor.AdviseType.BROADCAST;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(BroadcastIndexAdvisorPlanTest.class);
    }

}
