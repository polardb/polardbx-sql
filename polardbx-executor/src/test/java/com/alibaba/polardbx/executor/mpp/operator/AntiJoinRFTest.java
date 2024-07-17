package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.PlanUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

public class AntiJoinRFTest extends PlanFragmentTestBase {
    public AntiJoinRFTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, FragmentRFScheduleTest.class.getSimpleName(), sqlIndex, sql, expectedPlan, lineNum,
            new AntiJoinFragmentTest());
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(AntiJoinRFTest.class);
    }

    private static class AntiJoinFragmentTest extends AbstractPlanFragmentTester {
        public AntiJoinFragmentTest() {
            super(16, 1, 16);
        }

        @Override
        boolean test(List<PipelineFactory> pipelineFactories, ExecutionContext executionContext) {
            StringBuilder builder = new StringBuilder();
            for (PipelineFactory pipelineFactory : pipelineFactories) {
                builder.append(PlanUtils.formatPipelineFragment(executionContext, pipelineFactory,
                    executionContext.getParams().getCurrentParameter()));
            }
            System.out.println(builder);

            for (PipelineFactory pipelineFactory : pipelineFactories) {
                FragmentRFManager manager;
                if ((manager = pipelineFactory.getFragment().getFragmentRFManager()) != null) {

                    Map<FragmentRFItemKey, FragmentRFItem> allItems = manager.getAllItems();
                    allItems.forEach((k, v) -> {
                        System.out.println("itemKey = " + k + " itemVal = " + v);
                    });

                    // no itemKey is generated for anti join.
                    Assert.assertTrue(allItems.size() == 0);
                }
            }

            return true;
        }
    }
}
