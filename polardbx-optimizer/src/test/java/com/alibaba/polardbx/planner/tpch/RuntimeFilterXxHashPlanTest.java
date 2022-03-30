package com.alibaba.polardbx.planner.tpch;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class RuntimeFilterXxHashPlanTest extends PlanTestCommon {
    public RuntimeFilterXxHashPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        enableMpp = true;
        storageSupportsBloomFilter = true;
        storageUsingXxHashInBloomFilter = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(RuntimeFilterXxHashPlanTest.class);
    }
}

