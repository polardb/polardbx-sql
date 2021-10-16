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
public class InnerJoinLeftAssociateRule extends AbstractInnerJoinLeftAssociateRule {

    public static final InnerJoinLeftAssociateRule INSTANCE =
        new InnerJoinLeftAssociateRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            RelFactories.LOGICAL_BUILDER, "InnerJoinLeftAssociateRule");

    public static final InnerJoinLeftAssociateRule PROJECT_INSTANCE =
        new InnerJoinLeftAssociateRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER, "InnerJoinLeftAssociateRule:Project");

    public InnerJoinLeftAssociateRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
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
        final RelNode relA = topJoin.getLeft();
        final RelNode relB = bottomJoin.getLeft();
        final RelNode relC = bottomJoin.getRight();
        final RelOptCluster cluster = topJoin.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        //        topJoin
        //        /     \
        //       A    bottomJoin
        //               /    \
        //              B      C

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final ImmutableBitSet cBitSet = ImmutableBitSet.range(aCount + bCount, aCount + bCount + cCount);
        final ImmutableBitSet bBitSet = ImmutableBitSet.range(aCount, aCount + bCount);

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
        //        newTopJoin
        //        /        \
        //   newBottomJoin  C
        //    /    \
        //   A      B

        // shift bottom condition right with left field count offset
        // target: | A       | B | C      |
        // source: | B | C      |
        final Mappings.TargetMapping shiftBottomMapping =
            Mappings.offsetTarget(Mappings.createIdentity(bCount + cCount), aCount);
        final List<RexNode> shiftBottomList = Lists.newArrayList();
        new RexPermuteInputsShuttle(shiftBottomMapping, relA, relB)
            .visitList(RelOptUtil.conjunctions(bottomJoin.getCondition()), shiftBottomList);
        RexNode shiftBottomCondition =
            RexUtil.composeConjunction(rexBuilder, shiftBottomList, false);

        // Split the condition of topJoin and bottomJoin into a conjunctions. A
        // condition can be pushed down if it does not use columns from C.
        final List<RexNode> top = Lists.newArrayList();
        final List<RexNode> bottom = Lists.newArrayList();
        JoinPushThroughJoinRule.split(topJoin.getCondition(), cBitSet, top, bottom);
        JoinPushThroughJoinRule.split(shiftBottomCondition, cBitSet, top, bottom);

        // Mapping for moving conditions from topJoin or bottomJoin to
        // newBottomJoin.
        // target: | A       | B |
        // source: | A       | B | C      |
        final Mappings.TargetMapping bottomMapping =
            Mappings.createShiftMapping(
                aCount + bCount + cCount,
                0, 0, aCount,
                aCount, aCount, bCount);
        final List<RexNode> newBottomList = Lists.newArrayList();
        new RexPermuteInputsShuttle(bottomMapping, relA, relB)
            .visitList(bottom, newBottomList);
        RexNode newBottomCondition =
            RexUtil.composeConjunction(rexBuilder, newBottomList, false);

        final Join newBottomJoin =
            bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relA,
                relB, JoinRelType.INNER, false);

        // Condition for newTopJoin consists of pieces from bottomJoin and topJoin.
        // Field ordinals do not need to be changed.
        RexNode newTopCondition =
            RexUtil.composeConjunction(rexBuilder, top, false);
        final Join newTopJoin =
            topJoin.copy(topJoin.getTraitSet(), newTopCondition, newBottomJoin,
                relC, JoinRelType.INNER, false);

        if (newTopJoin instanceof LogicalJoin) {
            ((LogicalJoin) newTopJoin).getJoinReorderContext().setHasLeftAssociate(true);
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
