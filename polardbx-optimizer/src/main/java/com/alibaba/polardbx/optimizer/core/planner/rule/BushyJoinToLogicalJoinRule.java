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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.BushyJoin;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BushyJoinToLogicalJoinRule extends RelOptRule {
    public static final BushyJoinToLogicalJoinRule INSTANCE =
        new BushyJoinToLogicalJoinRule(RelFactories.LOGICAL_BUILDER);

    private final PrintWriter pw = CalcitePrepareImpl.DEBUG
        ? Util.printWriter(System.out)
        : null;

    /**
     * Creates an MultiJoinOptimizeBushyRule.
     */
    public BushyJoinToLogicalJoinRule(RelBuilderFactory relBuilderFactory) {
        super(operand(BushyJoin.class, any()), relBuilderFactory, null);
    }

    /**
     * return true for a successful insert, otherwise false
     */
    private boolean checkNotDuplicateAndAdd(List<RexNode> conditions, LoptMultiJoin.Edge edge) {
        for (RexNode condition : conditions) {
            if (condition.toString().equals(edge.getCondition().toString())) {
                return false;
            }
        }
        conditions.add(edge.getCondition());
        return true;
    }

    private Vertex treeBuildHelper(List<RelNode> layerNode, HashMap<RelNode, Vertex> nodeToVertex,
                                   List<Vertex> vertexes, LoptMultiJoin loptMultiJoin) {
        Vertex leftVertex = null;
        Vertex rightVertex = null;
        for (RelNode leafNode : layerNode) {
            assert nodeToVertex.containsKey(leafNode);
            Vertex v = nodeToVertex.get(leafNode);
            if (leftVertex == null) {
                leftVertex = v;
            } else if (rightVertex == null) {
                rightVertex = v;
                final List<RexNode> conditions = Lists.newArrayList();
                final ImmutableBitSet newFactors =
                    leftVertex.factors
                        .rebuild()
                        .addAll(rightVertex.factors)
                        .set(vertexes.size())
                        .build();
                List<RexNode> removeList = new ArrayList<>();
                for (RexNode filter : loptMultiJoin.getJoinFilters()) {
                    LoptMultiJoin.Edge edge = loptMultiJoin.createEdge(filter);
                    if (newFactors.contains(edge.getFactors())) {
                        removeList.add(filter);
                        checkNotDuplicateAndAdd(conditions, edge);
                    }
                }
                for (RexNode each : removeList) {
                    loptMultiJoin.getJoinFilters().remove(each);
                }
                Vertex jv = new JoinVertex(vertexes.size(), leftVertex.id, rightVertex.id, newFactors,
                    ImmutableList.copyOf(conditions));
                vertexes.add(jv);
                leftVertex = jv;
                rightVertex = null;
            }
        }
        assert leftVertex != null;
        return leftVertex;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_JOIN_CLUSTERING);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BushyJoin bushyJoin = call.rel(0);
        final RexBuilder rexBuilder = bushyJoin.getCluster().getRexBuilder();
        final RelBuilder relBuilder = call.builder();

        List<List<RelNode>> finalEquiGroups = bushyJoin.getFinalEquiGroups();
        if (finalEquiGroups == null) {
            return;   //MultiJoin Conversion not ready
        }

        final LoptMultiJoin loptMultiJoin = new LoptMultiJoin(bushyJoin);

        HashMap<RelNode, Vertex> nodeToVertex = new HashMap<>();

        final List<Vertex> vertexes = Lists.newArrayList();
        int x = 0;
        for (int i = 0; i < loptMultiJoin.getNumJoinFactors(); i++) {
            final RelNode rel = loptMultiJoin.getJoinFactor(i);
            LeafVertex leafVertex = new LeafVertex(i, rel, x);
            vertexes.add(leafVertex);
            nodeToVertex.put(rel, leafVertex);
            x += rel.getRowType().getFieldCount();
        }
        assert x == loptMultiJoin.getNumTotalFields();

        Vertex leftVertex = null;
        Vertex rightVertex = null;

        for (List<RelNode> group : finalEquiGroups) {
            Vertex v = treeBuildHelper(group, nodeToVertex, vertexes, loptMultiJoin);
            if (leftVertex == null) {
                leftVertex = v;
            } else if (rightVertex == null) {
                final List<RexNode> conditions = Lists.newArrayList();
                rightVertex = v;
                final ImmutableBitSet newFactors =
                    leftVertex.factors
                        .rebuild()
                        .addAll(rightVertex.factors)
                        .set(vertexes.size())
                        .build();

                List<RexNode> removeList = new ArrayList<>();
                for (RexNode filter : loptMultiJoin.getJoinFilters()) {
                    LoptMultiJoin.Edge edge = loptMultiJoin.createEdge(filter);
                    if (newFactors.contains(edge.getFactors())) {
                        removeList.add(filter);
                        checkNotDuplicateAndAdd(conditions, edge);
                    }
                }
                for (RexNode each : removeList) {
                    loptMultiJoin.getJoinFilters().remove(each);
                }
                Vertex jv = new JoinVertex(vertexes.size(), leftVertex.id, rightVertex.id, newFactors,
                    ImmutableList.copyOf(conditions));
                vertexes.add(jv);
                leftVertex = jv;
                rightVertex = null;
            }
        }

//        final Vertex newVertex =
//                new JoinVertex(v, majorFactor, minorFactor, newFactors,
//                        ImmutableList.copyOf(conditions));

        List<Pair<RelNode, Mappings.TargetMapping>> relNodes = Lists.newArrayList();
        for (Vertex vertex : vertexes) {
            if (vertex instanceof LeafVertex) {
                LeafVertex leafVertex = (LeafVertex) vertex;
                final Mappings.TargetMapping mapping =
                    Mappings.offsetSource(
                        Mappings.createIdentity(
                            leafVertex.rel.getRowType().getFieldCount()),
                        leafVertex.fieldOffset,
                        loptMultiJoin.getNumTotalFields());
                relNodes.add(Pair.of(leafVertex.rel, mapping));
            } else {
                JoinVertex joinVertex = (JoinVertex) vertex;
                final Pair<RelNode, Mappings.TargetMapping> leftPair =
                    relNodes.get(joinVertex.leftFactor);
                RelNode left = leftPair.left;
                final Mappings.TargetMapping leftMapping = leftPair.right;
                final Pair<RelNode, Mappings.TargetMapping> rightPair =
                    relNodes.get(joinVertex.rightFactor);
                RelNode right = rightPair.left;
                final Mappings.TargetMapping rightMapping = rightPair.right;
                final Mappings.TargetMapping mapping =
                    Mappings.merge(leftMapping,
                        Mappings.offsetTarget(rightMapping,
                            left.getRowType().getFieldCount()));
                if (pw != null) {
                    pw.println("left: " + leftMapping);
                    pw.println("right: " + rightMapping);
                    pw.println("combined: " + mapping);
                    pw.println();
                }
                final RexVisitor<RexNode> shuttle =
                    new RexPermuteInputsShuttle(mapping, left, right);
                final RexNode condition =
                    RexUtil.composeConjunction(rexBuilder, joinVertex.conditions,
                        false);

                final RelNode join = relBuilder
                    .push(left)
                    .push(right)
                    .join(JoinRelType.INNER, condition.accept(shuttle))
                    .build();
                relNodes.add(Pair.of(join, mapping));
            }
            if (pw != null) {
                pw.println(Util.last(relNodes));
            }
        }

        final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
        relBuilder.push(top.left)
            .project(relBuilder.fields(top.right));
        relBuilder.rename(bushyJoin.getRowType().getFieldNames());
        call.transformTo(relBuilder.build());
    }

    /**
     * Participant in a join (relation or join).
     */
    abstract static class Vertex {
        protected final ImmutableBitSet factors;
        final int id;

        Vertex(int id, ImmutableBitSet factors) {
            this.id = id;
            this.factors = factors;
        }
    }

    /**
     * Relation participating in a join.
     */
    static class LeafVertex extends Vertex {
        final int fieldOffset;
        private final RelNode rel;

        LeafVertex(int id, RelNode rel, int fieldOffset) {
            super(id, ImmutableBitSet.of(id));
            this.rel = rel;
            this.fieldOffset = fieldOffset;
        }

        @Override
        public String toString() {
            return "LeafVertex(id: " + id
                + ", factors: " + factors
                + ", fieldOffset: " + fieldOffset
                + ")";
        }
    }

    /**
     * Participant in a join which is itself a join.
     */
    static class JoinVertex extends Vertex {
        /**
         * Zero or more join conditions. All are in terms of the original input
         * columns (not in terms of the outputs of left and right input factors).
         */
        final ImmutableList<RexNode> conditions;
        private final int leftFactor;
        private final int rightFactor;

        JoinVertex(int id, int leftFactor, int rightFactor, ImmutableBitSet factors,
                   ImmutableList<RexNode> conditions) {
            super(id, factors);
            this.leftFactor = leftFactor;
            this.rightFactor = rightFactor;
            this.conditions = Preconditions.checkNotNull(conditions);
        }

        @Override
        public String toString() {
            return "JoinVertex(id: " + id
                + ", factors: " + factors
                + ", leftFactor: " + leftFactor
                + ", rightFactor: " + rightFactor
                + ")";
        }
    }
}

// End MultiJoinOptimizeBushyRule.java
