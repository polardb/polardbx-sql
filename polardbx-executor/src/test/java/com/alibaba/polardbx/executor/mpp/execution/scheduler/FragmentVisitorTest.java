/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.RemoteSourceNode;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.when;

public class FragmentVisitorTest {

    @Test
    public void testJoinWithUnion() {

        //   join = build + probe
        //   build = build_union(scan0 + scan1)
        //   probe = probe_union(scan2 + scan3)
        PlanFragment sourceFragment_0 = createScanFragment(0);
        PlanFragment sourceFragment_1 = createScanFragment(1);
        RemoteSourceNode buildRemoteWithUnion = createRemoteNode(sourceFragment_0, sourceFragment_1);
        PlanFragment sourceFragment_2 = createScanFragment(2);
        PlanFragment sourceFragment_3 = createScanFragment(3);
        RemoteSourceNode probeRemoteWithUnion = createRemoteNode(sourceFragment_2, sourceFragment_3);
        PlanFragment joinFragment_4 = createHashJoinPlanFragment(5, buildRemoteWithUnion, probeRemoteWithUnion);

        List<Set<Integer>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(
            sourceFragment_3,
            sourceFragment_2,
            sourceFragment_1,
            sourceFragment_0,
            joinFragment_4));

        assertEquals(ImmutableList.of(
            ImmutableSet.of(joinFragment_4.getId()),
            ImmutableSet.of(sourceFragment_0.getId()),
            ImmutableSet.of(sourceFragment_1.getId()),
            ImmutableSet.of(sourceFragment_2.getId()),
            ImmutableSet.of(sourceFragment_3.getId())),
            phases);
    }

    @Test
    public void testAggJoinWithUnion() {

        //build
        PlanFragment sourceFragment_0 = createScanFragment(0);
        PlanFragment sourceFragment_1 = createScanFragment(1);
        RemoteSourceNode buildRemoteWithUnion = createRemoteNode(sourceFragment_0, sourceFragment_1);

        //probe
        PlanFragment sourceFragment_2 = createScanFragment(2);
        PlanFragment sourceFragment_3 = createScanFragment(3);
        RemoteSourceNode aggRemote = createRemoteNode(sourceFragment_3);
        PlanFragment aggFragment_4 = createHashAggPlanFragment(4, aggRemote);
        RemoteSourceNode remoteWithUnion = createRemoteNode(sourceFragment_2, aggFragment_4);
        PlanFragment aggFragment_5 = createHashAggPlanFragment(5, remoteWithUnion);
        RemoteSourceNode remoteWithagg = createRemoteNode(aggFragment_5);
        PlanFragment aggFragment_6 = createHashAggPlanFragment(6, remoteWithagg);
        RemoteSourceNode probeRemoteWithUnion = createRemoteNode(aggFragment_6);

        PlanFragment joinFragment_7 = createHashJoinPlanFragment(7, buildRemoteWithUnion, probeRemoteWithUnion);
        RemoteSourceNode remoteWithJoin = createRemoteNode(joinFragment_7);

        PlanFragment joinFragment_8 = createHashJoinPlanFragment(8, remoteWithJoin, scanSourceNode());
        List<Set<Integer>> phases = PhasedExecutionSchedule.extractPhases(ImmutableList.of(
            joinFragment_8,
            joinFragment_7,
            aggFragment_6,
            aggFragment_5,
            aggFragment_4,
            sourceFragment_3,
            sourceFragment_2,
            sourceFragment_1,
            sourceFragment_0));

        assertEquals(ImmutableList.of(
            ImmutableSet.of(joinFragment_8.getId()),
            ImmutableSet.of(joinFragment_7.getId()),
            ImmutableSet.of(sourceFragment_0.getId()),
            ImmutableSet.of(sourceFragment_1.getId()),
            ImmutableSet.of(aggFragment_6.getId()),
            ImmutableSet.of(aggFragment_5.getId()),
            ImmutableSet.of(sourceFragment_2.getId()),
            ImmutableSet.of(aggFragment_4.getId()),
            ImmutableSet.of(sourceFragment_3.getId())),
            phases);
    }

    protected static PlanFragment createScanFragment(Integer id) {
        PlanFragment scanFragment = Mockito.mock(PlanFragment.class);
        LogicalView scan = scanSourceNode();
        when(scanFragment.getId()).thenReturn(id);
        when(scanFragment.getRootNode()).thenReturn(scan);
        return scanFragment;
    }

    protected static RemoteSourceNode createRemoteNode(PlanFragment... fragments) {
        RemoteSourceNode remoteSourceNode = Mockito.mock(RemoteSourceNode.class);
        List<Integer> sourceIds = Lists.newArrayList(
            fragments).stream().map(fragment -> fragment.getId()).collect(Collectors.toList());
        when(remoteSourceNode.getSourceFragmentIds()).thenReturn(sourceIds);
        return remoteSourceNode;
    }

    protected static LogicalView scanSourceNode() {
        LogicalView scan = Mockito.mock(LogicalView.class);
        when(scan.getInputs()).thenReturn(Collections.emptyList());
        return scan;
    }

    protected static PlanFragment createHashJoinPlanFragment(Integer id, RelNode build,
                                                             RelNode probe) {
        PlanFragment joinFragment = Mockito.mock(PlanFragment.class);
        HashJoin hashJoin = Mockito.mock(HashJoin.class);
        when(hashJoin.getInner()).thenReturn(build);
        when(hashJoin.getOuter()).thenReturn(probe);
        LogicalProject project = Mockito.mock(LogicalProject.class);
        when(project.getInputs()).thenReturn(Lists.newArrayList(hashJoin));
        when(joinFragment.getId()).thenReturn(id);
        when(joinFragment.getRootNode()).thenReturn(project);
        return joinFragment;
    }

    protected static PlanFragment createHashAggPlanFragment(Integer id, RemoteSourceNode sourceNode) {
        PlanFragment aggFragment = Mockito.mock(PlanFragment.class);
        HashAgg hashAgg = Mockito.mock(HashAgg.class);
        List<RelNode> inputs = Lists.newArrayList(sourceNode);
        when(hashAgg.getInputs()).thenReturn(inputs);
        when(aggFragment.getId()).thenReturn(id);
        when(aggFragment.getRootNode()).thenReturn(hashAgg);
        return aggFragment;
    }
}
