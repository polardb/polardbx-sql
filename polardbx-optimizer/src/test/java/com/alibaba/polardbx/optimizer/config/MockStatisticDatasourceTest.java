package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * The unit test aims at checking that
 * the result cost of mock is exactly what the optimizer get in production environment.
 * It is an example to use 'explain statistics'
 *
 * @author Shi Yuxuan
 */
public class MockStatisticDatasourceTest extends ParameterizedTestCommon {
    public MockStatisticDatasourceTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        setExplainCost(true);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(MockStatisticDatasourceTest.class);
    }
}
