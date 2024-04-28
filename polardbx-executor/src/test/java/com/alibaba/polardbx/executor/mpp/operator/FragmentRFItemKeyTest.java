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

public class FragmentRFItemKeyTest extends PlanFragmentTestBase {
    public FragmentRFItemKeyTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, FragmentRFItemKeyTest.class.getSimpleName(), sqlIndex, sql, expectedPlan, lineNum,
            new TypeFragmentRFTester(16, 1, 16));
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(FragmentRFItemKeyTest.class);
    }

    private static class TypeFragmentRFTester extends AbstractPlanFragmentTester {

        public TypeFragmentRFTester(int parallelism, int taskNumber, int localPartitionCount) {
            super(parallelism, taskNumber, localPartitionCount);
        }

        @Override
        boolean test(List<PipelineFactory> pipelineFactories, ExecutionContext executionContext) {
            // hashjoin(condition="ps_comment = l_comment and l_partkey = ps_partkey and l_linenumber = ps_supplycost", type="inner")

            // Don't generate item key for ps_comment = l_comment and  l_linenumber = ps_supplycost
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

                    // itemKey = FragmentRFItemKey{buildColumnName='ps_partkey', probeColumnName='l_partkey', buildIndex=0, probeIndex=1} isReversed = false
                    Assert.assertTrue(allItems.size() == 1);
                }
            }

            return true;
        }
    }
}
