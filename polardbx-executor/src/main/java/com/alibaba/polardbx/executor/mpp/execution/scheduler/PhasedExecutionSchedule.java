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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.execution.StageState;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.RemoteSourceNode;
import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.mpp.execution.StageState.FLUSHING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.RUNNING;
import static com.alibaba.polardbx.executor.mpp.execution.StageState.SCHEDULED;
import static com.alibaba.polardbx.executor.utils.ExecUtils.convertBuildSide;

@NotThreadSafe
public class PhasedExecutionSchedule
    implements ExecutionSchedule {

    private static final Logger log = LoggerFactory.getLogger(PhasedExecutionSchedule.class);

    private final List<Set<SqlStageExecution>> schedulePhases;
    private final List<SqlStageExecution> activeSources = new ArrayList<>();

    public PhasedExecutionSchedule(Collection<SqlStageExecution> stages) {
        List<Set<Integer>> phases = extractPhases(stages.stream().map(SqlStageExecution::getFragment).collect(
            ImmutableCollectors.toImmutableList()));

        Map<Integer, SqlStageExecution> stagesByFragmentId = stages.stream().collect(
            ImmutableCollectors.toImmutableMap(stage -> stage.getFragment().getId()));

        // create a mutable list of mutable sets of stages, so we can remove completed stages
        schedulePhases = new ArrayList<>();
        for (Set<Integer> phase : phases) {
            schedulePhases.add(phase.stream()
                .map(stagesByFragmentId::get)
                .collect(Collectors.toCollection(HashSet::new)));
        }
    }

    @Override
    public List<SqlStageExecution> getStagesToSchedule() {
        removeCompletedStages();
        addPhasesIfNecessary();
        if (isFinished()) {
            return ImmutableList.of();
        }
        return activeSources;
    }

    private void removeCompletedStages() {
        for (Iterator<SqlStageExecution> stageIterator = activeSources.iterator(); stageIterator.hasNext(); ) {
            StageState state = stageIterator.next().getState();
            if (state == SCHEDULED || state == RUNNING || state == FLUSHING || state.isDone()) {
                stageIterator.remove();
            }
        }
    }

    private void addPhasesIfNecessary() {
        // we want at least one source distributed phase in the active sources
        if (hasSourceDistributedStage(activeSources)) {
            return;
        }

        while (!schedulePhases.isEmpty()) {
            Set<SqlStageExecution> phase = schedulePhases.remove(0);
            for (SqlStageExecution one : phase) {
                activeSources.add(one);
            }
            if (hasSourceDistributedStage(phase)) {
                return;
            }
        }
    }

    private static boolean hasSourceDistributedStage(Collection<SqlStageExecution> phase) {
        return phase.stream().anyMatch(stage -> !stage.getFragment().getPartitionedSources().isEmpty());
    }

    @Override
    public boolean isFinished() {
        return activeSources.isEmpty() && schedulePhases.isEmpty();
    }

    @VisibleForTesting
    static List<Set<Integer>> extractPhases(Collection<PlanFragment> fragments) {
        // Build a graph where the plan fragments are vertexes and the edges represent
        // a before -> after relationship.  For example, a join hash build has an edge
        // to the join probe.
        DirectedGraph<Integer, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);
        fragments.forEach(fragment -> graph.addVertex(fragment.getId()));

        FragmentVisitor visitor = new FragmentVisitor(fragments, graph);
        for (PlanFragment fragment : fragments) {
            visitor.processFragment(fragment.getId());
        }

        // Computes all the strongly connected components of the directed graph.
        // These are the "phases" which hold the set of fragments that must be started
        // at the same time to avoid deadlock.
        List<Set<Integer>> components = new StrongConnectivityInspector<>(graph).stronglyConnectedSets();

        Map<Integer, Set<Integer>> componentMembership = new HashMap<>();
        for (Set<Integer> component : components) {
            for (Integer planFragmentId : component) {
                componentMembership.put(planFragmentId, component);
            }
        }

        // build graph of components (phases)
        DirectedGraph<Set<Integer>, DefaultEdge> componentGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
        components.forEach(componentGraph::addVertex);
        for (DefaultEdge edge : graph.edgeSet()) {
            Integer source = graph.getEdgeSource(edge);
            Integer target = graph.getEdgeTarget(edge);

            Set<Integer> from = componentMembership.get(source);
            Set<Integer> to = componentMembership.get(target);
            if (!from.equals(to)) {
                // the topological order iterator below doesn't include vertices that have self-edges, so don't add them
                componentGraph.addEdge(from, to);
            }
        }

        List<Set<Integer>> schedulePhases = ImmutableList.copyOf(new TopologicalOrderIterator<>(componentGraph));
        return schedulePhases;
    }

    private static class FragmentVisitor {

        private final Map<Integer, PlanFragment> fragments;
        private final DirectedGraph<Integer, DefaultEdge> graph;
        private final Map<Integer, Set<Integer>> fragmentSources = new HashMap<>();

        public FragmentVisitor(Collection<PlanFragment> fragments, DirectedGraph<Integer, DefaultEdge> graph) {
            this.fragments = fragments.stream()
                .collect(ImmutableCollectors.toImmutableMap(PlanFragment::getId));
            this.graph = graph;
        }

        public Set<Integer> processFragment(Integer planFragmentId) {
            if (!fragmentSources.containsKey(planFragmentId)) {
                Set<Integer> rets = processFragment(fragments.get(planFragmentId));
                fragmentSources.put(planFragmentId, rets);
                return rets;
            } else {
                return fragmentSources.get(planFragmentId);
            }
        }

        private Set<Integer> processFragment(PlanFragment fragment) {
            Set<Integer> sources = this.go(fragment.getRootNode(), fragment.getId());
            return ImmutableSet.<Integer>builder().add(fragment.getId()).addAll(sources).build();
        }

        public Set<Integer> visit(RelNode node, int currentFragmentId) {
            if (node instanceof Join) {
                if (node instanceof BKAJoin || node instanceof SemiBKAJoin) {
                    //针对于BKAJoin，我们先执行outer, 在执行inner的
                    RelNode inner = ((Join) node).getInner();
                    RelNode outer = ((Join) node).getOuter();
                    Set<Integer> outerSources = visit(outer, currentFragmentId);
                    Set<Integer> innerSources = visit(inner, currentFragmentId);
                    if (innerSources.contains(currentFragmentId)) {
                        //inner node is chained with the join.
                        for (Integer outerSource : outerSources) {
                            log.debug("addEdge " + currentFragmentId + "-" + outerSource);
                            graph.addEdge(currentFragmentId, outerSource);
                        }
                    } else {
                        for (Integer outerSource : outerSources) {
                            for (Integer innerSource : innerSources) {
                                log.debug("addEdge " + outerSource + "-" + innerSource);
                                graph.addEdge(outerSource, innerSource);
                            }
                        }
                    }
                    return ImmutableSet.<Integer>builder().addAll(outerSources).addAll(innerSources).build();
                } else {

                    boolean convertBuildSide = convertBuildSide((Join) node);
                    RelNode build;
                    RelNode probe;
                    if (convertBuildSide) {
                        probe = ((Join) node).getInner();
                        build = ((Join) node).getOuter();
                    } else {
                        build = ((Join) node).getInner();
                        probe = ((Join) node).getOuter();
                    }

                    Set<Integer> buildSources = visit(build, currentFragmentId);
                    Set<Integer> probeSources = visit(probe, currentFragmentId);
                    if (probeSources.size() == 1 && probeSources.contains(currentFragmentId)) {
                        //probe node is chained with the join.
                        for (Integer buildSource : buildSources) {
                            log.debug("addEdge " + currentFragmentId + "-" + buildSource);
                            graph.addEdge(currentFragmentId, buildSource);
                        }
                    } else {
                        for (Integer buildSource : buildSources) {
                            for (Integer probeSource : probeSources) {
                                log.debug("addEdge " + buildSource + "-" + probeSource);
                                graph.addEdge(buildSource, probeSource);
                            }
                        }
                    }
                    return ImmutableSet.<Integer>builder().addAll(buildSources).addAll(probeSources).build();
                }
            } else if (node instanceof RemoteSourceNode) {
                ImmutableSet.Builder<Integer> sources = ImmutableSet.builder();
                Set<Integer> previousFragmentSources = ImmutableSet.of();
                List<Integer> sourceIds = ((RemoteSourceNode) node).getSourceFragmentIds();

                for (Integer remoteFragment : sourceIds) {
                    // this current fragment depends on the remote fragment
                    graph.addEdge(currentFragmentId, remoteFragment);
                    log.debug("addEdge " + currentFragmentId + "-" + remoteFragment);
                    // get all sources for the remote fragment
                    Set<Integer> remoteFragmentSources = processFragment(remoteFragment);
                    sources.addAll(remoteFragmentSources);
                    // For UNION there can be multiple sources.
                    // Link the previous source to the current source, so we only
                    // schedule one at a time.
                    addEdges(previousFragmentSources, remoteFragmentSources);

                    previousFragmentSources = remoteFragmentSources;
                }
                return sources.build();
            } else if (node instanceof LogicalCorrelate) {
                return visit(((LogicalCorrelate) node).getLeft(), currentFragmentId);
            } else {
                if (node.getInputs().size() == 2) {
                    ImmutableSet.Builder<Integer> allSources = ImmutableSet.builder();
                    Set<Integer> previousSources = ImmutableSet.of();
                    for (RelNode subPlanNode : node.getInputs()) {
                        Set<Integer> currentSources = visit(subPlanNode, currentFragmentId);
                        allSources.addAll(currentSources);
                        addEdges(previousSources, currentSources);
                        previousSources = currentSources;
                    }
                    return allSources.build();
                } else if (node.getInputs().size() == 1) {
                    return visit(node.getInputs().get(0), currentFragmentId);
                } else {
                    return ImmutableSet.of(currentFragmentId);
                }
            }
        }

        /**
         * Starts an iteration.
         */
        public Set<Integer> go(RelNode p, int currentFragmentId) {
            return visit(p, currentFragmentId);
        }

        private void addEdges(Set<Integer> sourceFragments, Set<Integer> targetFragments) {
            for (Integer targetFragment : targetFragments) {
                for (Integer sourceFragment : sourceFragments) {
                    log.debug("addEdge " + sourceFragment + "-" + targetFragment);
                    graph.addEdge(sourceFragment, targetFragment);
                }
            }
        }
    }
}
