package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.PlanUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class PlanFragmentRFTester extends AbstractPlanFragmentTester {
    public PlanFragmentRFTester() {
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

                // itemKey = FragmentRFItemKey{buildColumnName='ps_suppkey', probeColumnName='l_suppkey', buildIndex=1, probeIndex=2} itemVal = FragmentRFItemImpl{  buildColumnName='ps_suppkey', probeColumnName='l_suppkey', useXXHashInBuild=true, useXXHashInFilter=true, rfType=GLOBAL, buildSideChannel=-1, sourceFilterChannel=-1, sourceRefInFile=0, synchronizerList=null, sourceList=[]}
                // itemKey = FragmentRFItemKey{buildColumnName='o_orderkey', probeColumnName='l_orderkey', buildIndex=0, probeIndex=0} itemVal = FragmentRFItemImpl{  buildColumnName='o_orderkey', probeColumnName='l_orderkey', useXXHashInBuild=true, useXXHashInFilter=true, rfType=LOCAL, buildSideChannel=-1, sourceFilterChannel=-1, sourceRefInFile=0, synchronizerList=null, sourceList=[]}
                // itemKey = FragmentRFItemKey{buildColumnName='ps_partkey', probeColumnName='l_partkey', buildIndex=0, probeIndex=1} itemVal = FragmentRFItemImpl{  buildColumnName='ps_partkey', probeColumnName='l_partkey', useXXHashInBuild=true, useXXHashInFilter=true, rfType=GLOBAL, buildSideChannel=-1, sourceFilterChannel=-1, sourceRefInFile=0, synchronizerList=null, sourceList=[]}
                Assert.assertTrue(allItems.size() == 3);
            }
        }

        return true;
    }
}
