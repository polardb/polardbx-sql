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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

/**
 * copy from calcite {@code SemiJoinRule}
 */
public abstract class LogicalSemiJoinRule extends RelOptRule {
    private static final Predicate<Join> IS_LEFT_OR_INNER =
        new PredicateImpl<Join>() {
            public boolean test(Join input) {
                switch (input.getJoinType()) {
                case INNER:
                    return true;
                default:
                    return false;
                }
            }
        };

    /* Tests if an Aggregate always produces 1 row and 0 columns. */
    private static final Predicate<Aggregate> IS_EMPTY_AGGREGATE =
        new PredicateImpl<Aggregate>() {
            public boolean test(Aggregate input) {
                return input.getRowType().getFieldCount() == 0;
            }
        };

    public static final LogicalSemiJoinRule PROJECT =
        new ProjectToLogicalSemiJoinRule(Project.class, Join.class, Aggregate.class,
            RelFactories.LOGICAL_BUILDER, "LogicalSemiJoinRule:project");

    public static final LogicalSemiJoinRule JOIN =
        new JoinToLogicalSemiJoinRule(Join.class, Aggregate.class,
            RelFactories.LOGICAL_BUILDER, "LogicalSemiJoinRule:join");

    protected LogicalSemiJoinRule(Class<Project> projectClass, Class<Join> joinClass,
                                  Class<Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory,
                                  String description) {
        super(
            operand(projectClass,
                some(
                    operand(joinClass, null, IS_LEFT_OR_INNER,
                        some(operand(RelNode.class, any()),
                            operand(aggregateClass, any()))))),
            relBuilderFactory, description);
    }

    protected LogicalSemiJoinRule(Class<Join> joinClass, Class<Aggregate> aggregateClass,
                                  RelBuilderFactory relBuilderFactory, String description) {
        super(
            operand(joinClass, null, IS_LEFT_OR_INNER,
                some(operand(RelNode.class, any()),
                    operand(aggregateClass, null, IS_EMPTY_AGGREGATE, any()))),
            relBuilderFactory, description);
    }

    protected void perform(RelOptRuleCall call, Project project,
                           Join join, RelNode left, Aggregate aggregate) {
        final RelOptCluster cluster = join.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if (project != null) {
            final ImmutableBitSet bits =
                RelOptUtil.InputFinder.bits(project.getProjects(), null);
            final ImmutableBitSet rightBits =
                ImmutableBitSet.range(left.getRowType().getFieldCount(),
                    join.getRowType().getFieldCount());
            if (bits.intersects(rightBits)) {
                return;
            }
        }
        final JoinInfo joinInfo = join.analyzeCondition();
        if (!joinInfo.rightSet().equals(
            ImmutableBitSet.range(aggregate.getGroupCount()))) {
            // Rule requires that aggregate key to be the same as the join key.
            // By the way, neither a super-set nor a sub-set would work.
            return;
        }
        if (!joinInfo.isEqui()) {
            return;
        }
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(left);
        switch (join.getJoinType()) {
        case INNER:
            final List<Integer> newRightKeyBuilder = Lists.newArrayList();
            final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
            for (int key : joinInfo.rightKeys) {
                newRightKeyBuilder.add(aggregateKeys.get(key));
            }
            final ImmutableIntList newRightKeys = ImmutableIntList.copyOf(newRightKeyBuilder);
            relBuilder.push(aggregate.getInput());
            final RexNode newCondition =
                RelOptUtil.createEquiJoinCondition(relBuilder.peek(2, 0),
                    joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
                    rexBuilder);
            relBuilder.logicalSemiJoin(ImmutableList.of(newCondition), join.getHints());
            break;
        default:
            throw new AssertionError(join.getJoinType());
        }
        if (project != null) {
            relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
        }
        call.transformTo(relBuilder.build());
    }

    /**
     * SemiJoinRule that matches a Project on top of a Join with an Aggregate
     * as its right child.
     */
    public static class ProjectToLogicalSemiJoinRule extends LogicalSemiJoinRule {

        /**
         * Creates a ProjectToSemiJoinRule.
         */
        public ProjectToLogicalSemiJoinRule(Class<Project> projectClass,
                                            Class<Join> joinClass, Class<Aggregate> aggregateClass,
                                            RelBuilderFactory relBuilderFactory, String description) {
            super(projectClass, joinClass, aggregateClass,
                relBuilderFactory, description);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final Project project = call.rel(0);
            final Join join = call.rel(1);
            final RelNode left = call.rel(2);
            final Aggregate aggregate = call.rel(3);
            perform(call, project, join, left, aggregate);
        }
    }

    /**
     * SemiJoinRule that matches a Join with an empty Aggregate as its right
     * child.
     */
    public static class JoinToLogicalSemiJoinRule extends LogicalSemiJoinRule {

        /**
         * Creates a JoinToSemiJoinRule.
         */
        public JoinToLogicalSemiJoinRule(
            Class<Join> joinClass, Class<Aggregate> aggregateClass,
            RelBuilderFactory relBuilderFactory, String description) {
            super(joinClass, aggregateClass, relBuilderFactory, description);
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final RelNode left = call.rel(1);
        final Aggregate aggregate = call.rel(2);
        perform(call, null, join, left, aggregate);
    }
}

// End SemiJoinRule.java
