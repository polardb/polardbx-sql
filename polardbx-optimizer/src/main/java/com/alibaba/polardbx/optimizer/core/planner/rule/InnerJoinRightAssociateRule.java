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

import com.google.common.collect.Lists;
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

import java.util.List;

/**
 * @author dylan
 */
public class InnerJoinRightAssociateRule extends AbstractInnerJoinRightAssociateRule {

    public static final InnerJoinRightAssociateRule INSTANCE =
        new InnerJoinRightAssociateRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())), RelFactories.LOGICAL_BUILDER,
            "InnerJoinRightAssociateRule");

    public static final InnerJoinRightAssociateRule PROJECT_INSTANCE =
        new InnerJoinRightAssociateRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())), RelFactories.LOGICAL_BUILDER,
            "InnerJoinRightAssociateRule:Project");

    public InnerJoinRightAssociateRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                                       String description) {
        super(operand, relBuilderFactory, description);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final LogicalJoin topJoin = call.rel(0);
        if (topJoin.getJoinReorderContext().isHasCommute()
            || topJoin.getJoinReorderContext().isHasLeftAssociate()
            || topJoin.getJoinReorderContext().isHasRightAssociate()
            || topJoin.getJoinReorderContext().isHasExchange()) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        if (call.getRule() == INSTANCE) {
            onMatchInstance(call);
        } else if (call.getRule() == PROJECT_INSTANCE) {
            onMatchProjectInstance(call);
        }
    }

    @Override
    protected RelNode transform(final LogicalJoin topJoin, final LogicalJoin bottomJoin, RelBuilder relBuilder) {
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();
        final RelNode relC = topJoin.getRight();
        final RelOptCluster cluster = topJoin.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final ImmutableBitSet aBitSet = ImmutableBitSet.range(0, aCount);
        final ImmutableBitSet bBitSet =
            ImmutableBitSet.range(aCount, aCount + bCount);

        if (!topJoin.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return null;
        }

        // If either join is not inner, we cannot proceed.
        // (Is this too strict?)
        if (topJoin.getJoinType() != JoinRelType.INNER
            || bottomJoin.getJoinType() != JoinRelType.INNER) {
            return null;
        }

        // Goal is to transform to
        //
        //       newTopJoin
        //        /     \
        //       A   newBottomJoin
        //               /    \
        //              B      C

        // Split the condition of topJoin and bottomJoin into a conjunctions. A
        // condition can be pushed down if it does not use columns from A.
        final List<RexNode> top = Lists.newArrayList();
        final List<RexNode> bottom = Lists.newArrayList();
        JoinPushThroughJoinRule.split(topJoin.getCondition(), aBitSet, top, bottom);
        JoinPushThroughJoinRule.split(bottomJoin.getCondition(), aBitSet, top,
            bottom);

        // Mapping for moving conditions from topJoin or bottomJoin to
        // newBottomJoin.
        // target: | B | C      |
        // source: | A       | B | C      |
        final Mappings.TargetMapping bottomMapping =
            Mappings.createShiftMapping(
                aCount + bCount + cCount,
                0, aCount, bCount,
                bCount, aCount + bCount, cCount);
        final List<RexNode> newBottomList = Lists.newArrayList();
        new RexPermuteInputsShuttle(bottomMapping, relB, relC)
            .visitList(bottom, newBottomList);
        RexNode newBottomCondition =
            RexUtil.composeConjunction(rexBuilder, newBottomList, false);

        final Join newBottomJoin =
            bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relB,
                relC, JoinRelType.INNER, false);

        // Condition for newTopJoin consists of pieces from bottomJoin and topJoin.
        // Field ordinals do not need to be changed.
        RexNode newTopCondition =
            RexUtil.composeConjunction(rexBuilder, top, false);
        final Join newTopJoin =
            topJoin.copy(topJoin.getTraitSet(), newTopCondition, relA,
                newBottomJoin, JoinRelType.INNER, false);

        if (newTopJoin instanceof LogicalJoin) {
            ((LogicalJoin) newTopJoin).getJoinReorderContext().setHasRightAssociate(true);
            ((LogicalJoin) newTopJoin).getJoinReorderContext().setHasCommute(false);
        }

        if (newBottomJoin instanceof LogicalJoin) {
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasCommute(false);
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasRightAssociate(false);
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasLeftAssociate(false);
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasExchange(false);
        }

        if (newTopCondition.isAlwaysTrue() || newBottomCondition.isAlwaysTrue()) {
            return null;
        }

        return newTopJoin;
    }
}