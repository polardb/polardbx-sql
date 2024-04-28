package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.ExecutorMode;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

public class ParallelHashJoinFactoryTest extends PlanFragmentTestBase {
    protected String expectedFragment;

    public ParallelHashJoinFactoryTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                       String lineNum, String expectedFragment) {
        super(caseName, FragmentRFScheduleTest.class.getSimpleName(), sqlIndex, sql, expectedPlan, lineNum);

        // don't execute the test method of BasePlannerTest.
        ignoreBaseTest = true;
        this.expectedFragment = expectedFragment;
        this.executorMode = ExecutorMode.MPP;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        // com/alibaba/polardbx/executor/mpp/operator/ParallelHashJoinFactoryTest.yml
        return loadWithFragment(ParallelHashJoinFactoryTest.class);
    }

    @Test
    public void test() {
        localPartitionCount = -1;
        totalPartitionCount = -1;

        tester = new PlanFragmentTopologyChecker(defaultParallelism, taskNumber, localPartitionCount, expectedFragment);
        doPlanTest();
    }

}
