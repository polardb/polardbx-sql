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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

public class InnerJoinLAsscomRule extends AbstractJoinLAsscomRule {

    public static final InnerJoinLAsscomRule INSTANCE = new InnerJoinLAsscomRule(
        operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
        RelFactories.LOGICAL_BUILDER,
        "InnerJoinReorderRule:InnerJoinLAsscomRule");

    public static final InnerJoinLAsscomRule PROJECT_INSTANCE = new InnerJoinLAsscomRule(
        operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
        RelFactories.LOGICAL_BUILDER,
        "InnerJoinReorderRule:InnerJoinLAsscomRule:Project");

    public InnerJoinLAsscomRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalJoin topJoin = call.rel(0);
        final LogicalJoin bottomJoin;
        if (call.getRule() == INSTANCE) {
            bottomJoin = call.rel(1);
        } else if (call.getRule() == PROJECT_INSTANCE) {
            bottomJoin = call.rel(2);
        } else {
            return false;
        }

        if (bottomJoin.getJoinReorderContext().isHasCommuteZigZag()) {
            return false;
        }

        if (topJoin.getJoinReorderContext().isHasTopPushThrough()) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.getRule() == INSTANCE) {
            onMatchInstance(call);
        } else if (call.getRule() == PROJECT_INSTANCE) {
            onMatchProjectInstance(call);
        }
    }

    protected RelNode transform(final LogicalJoin topJoin, final LogicalJoin bottomJoin, RelBuilder relBuilder) {
        final RelNode relC = topJoin.getRight();
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();
        final RelOptCluster cluster = topJoin.getCluster();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final ImmutableBitSet bBitSet =
            ImmutableBitSet.range(aCount, aCount + bCount);

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  B
        //    /    \
        //   A      C

        // If either join is not inner, we cannot proceed.
        // (Is this too strict?)
        if (topJoin.getJoinType() != JoinRelType.INNER
            || bottomJoin.getJoinType() != JoinRelType.INNER) {
            return null;
        }

        // Split the condition of topJoin into a conjunction. Each of the
        // parts that does not use columns from B can be pushed down.
        final List<RexNode> intersecting = new ArrayList<>();
        final List<RexNode> nonIntersecting = new ArrayList<>();
        JoinPushThroughJoinRule.split(topJoin.getCondition(), bBitSet, intersecting, nonIntersecting);

        // If there's nothing to push down, it's not worth proceeding.
        if (nonIntersecting.isEmpty()) {
            return null;
        }

        // Split the condition of bottomJoin into a conjunction. Each of the
        // parts that use columns from B will need to be pulled up.
        final List<RexNode> bottomIntersecting = new ArrayList<>();
        final List<RexNode> bottomNonIntersecting = new ArrayList<>();
        JoinPushThroughJoinRule.split(bottomJoin.getCondition(), bBitSet, bottomIntersecting, bottomNonIntersecting);

        // target: | A       | C      |
        // source: | A       | B | C      |
        final Mappings.TargetMapping bottomMapping =
            Mappings.createShiftMapping(
                aCount + bCount + cCount,
                0, 0, aCount,
                aCount, aCount + bCount, cCount);
        final List<RexNode> newBottomList = new ArrayList<>();
        new RexPermuteInputsShuttle(bottomMapping, relA, relC)
            .visitList(nonIntersecting, newBottomList);
        new RexPermuteInputsShuttle(bottomMapping, relA, relC)
            .visitList(bottomNonIntersecting, newBottomList);
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode newBottomCondition =
            RexUtil.composeConjunction(rexBuilder, newBottomList, false);
        final Join newBottomJoin =
            bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relA,
                relC, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone());

        // target: | A       | C      | B |
        // source: | A       | B | C      |
        final Mappings.TargetMapping topMapping =
            Mappings.createShiftMapping(
                aCount + bCount + cCount,
                0, 0, aCount,
                aCount + cCount, aCount, bCount,
                aCount, aCount + bCount, cCount);
        final List<RexNode> newTopList = new ArrayList<>();
        new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
            .visitList(intersecting, newTopList);
        new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
            .visitList(bottomIntersecting, newTopList);
        RexNode newTopCondition =
            RexUtil.composeConjunction(rexBuilder, newTopList, false);
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin =
            topJoin.copy(topJoin.getTraitSet(), newTopCondition, newBottomJoin,
                relB, topJoin.getJoinType(), topJoin.isSemiJoinDone());
        if (newTopJoin instanceof LogicalJoin) {
            ((LogicalJoin) newTopJoin).getJoinReorderContext().setHasTopPushThrough(true);
        }

        if (newBottomJoin instanceof LogicalJoin) {
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasTopPushThrough(false);
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasCommute(false);
        }
        assert !Mappings.isIdentity(topMapping);
        relBuilder.push(newTopJoin);
        relBuilder.project(relBuilder.fields(topMapping));
        return relBuilder.build();
    }
}
