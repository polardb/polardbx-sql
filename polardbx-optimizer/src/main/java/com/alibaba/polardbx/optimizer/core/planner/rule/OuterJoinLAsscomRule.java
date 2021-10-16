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
import org.apache.calcite.plan.Strong;
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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OuterJoinLAsscomRule extends AbstractJoinLAsscomRule {
    public static final OuterJoinLAsscomRule INSTANCE = new OuterJoinLAsscomRule(
        operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
        RelFactories.LOGICAL_BUILDER,
        "OuterJoinReorderRule:OuterJoinLAsscomRule");

    public static final OuterJoinLAsscomRule PROJECT_INSTANCE = new OuterJoinLAsscomRule(
        operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
        RelFactories.LOGICAL_BUILDER,
        "OuterJoinReorderRule:OuterJoinLAsscomRule:Project");

    static Map<Pair<JoinRelType, JoinRelType>, Pair<JoinRelType, JoinRelType>> lAsscomTable;

    static {
        lAsscomTable = new HashMap<>();
        lAsscomTable.put(Pair.of(JoinRelType.INNER, JoinRelType.LEFT), Pair.of(JoinRelType.LEFT, JoinRelType.INNER));
        lAsscomTable.put(Pair.of(JoinRelType.LEFT, JoinRelType.INNER), Pair.of(JoinRelType.INNER, JoinRelType.LEFT));
        lAsscomTable.put(Pair.of(JoinRelType.LEFT, JoinRelType.LEFT), Pair.of(JoinRelType.LEFT, JoinRelType.LEFT));
    }

    public OuterJoinLAsscomRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
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

        if (topJoin.getJoinReorderContext().isHasTopPushThrough()
            /** we enable isHasCommute for lacking of OuterJoinAssocRule:Left */
//                || topJoin.getJoinReorderContext().isHasCommute()
            || topJoin.getJoinReorderContext().isHasLeftAssociate()
            || topJoin.getJoinReorderContext().isHasRightAssociate()
            || topJoin.getJoinReorderContext().isHasExchange()) {
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
        final ImmutableBitSet aBitSet =
            ImmutableBitSet.range(0, aCount);
        final ImmutableBitSet bBitSet =
            ImmutableBitSet.range(aCount, aCount + bCount);
        final ImmutableBitSet cBitSet =
            ImmutableBitSet.range(aCount + bCount, aCount + bCount + cCount);

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  B
        //    /    \
        //   A      C

        Pair<JoinRelType, JoinRelType> newJoinTypePair =
            lAsscomTable.get(Pair.of(bottomJoin.getJoinType(), topJoin.getJoinType()));
        if (newJoinTypePair == null) {
            return null;
        }

        // Split the condition of topJoin into a conjunction. Each of the
        // parts that use columns from both A and C can be pushed down.
        final List<RexNode> intersecting = new ArrayList<>();
        final List<RexNode> nonIntersecting = new ArrayList<>();
        split(topJoin.getCondition(), aBitSet, cBitSet, intersecting, nonIntersecting);

        /** top.condition intersecting with B, abort */
        if (!nonIntersecting.isEmpty()) {
            // wow, top inner join refer bottom left join right input and top join condition null reject bottom left join right input
            // so we can simplify bottom left join to inner join.
            if (bottomJoin.getJoinType() == JoinRelType.LEFT && topJoin.getJoinType() == JoinRelType.INNER &&
                Strong.isNotTrue(topJoin.getCondition(), bBitSet)) {
                LogicalJoin newBottomJoin = bottomJoin
                    .copy(bottomJoin.getTraitSet(), bottomJoin.getCondition(), bottomJoin.getLeft(),
                        bottomJoin.getRight(), JoinRelType.INNER, bottomJoin.isSemiJoinDone());
                LogicalJoin newTopJoin = topJoin
                    .copy(topJoin.getTraitSet(), topJoin.getCondition(), newBottomJoin, topJoin.getRight(),
                        topJoin.getJoinType(), topJoin.isSemiJoinDone());
                // clear the join reorder context, so by pass duplicate rule set restriction
                newBottomJoin.getJoinReorderContext().clear();
                newTopJoin.getJoinReorderContext().clear();
                return newTopJoin;
            }
            return null;
        }
        // If there's nothing to push down, it's not worth proceeding.
        if (intersecting.isEmpty()) {
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
            .visitList(intersecting, newBottomList);
        new RexPermuteInputsShuttle(bottomMapping, relA, relC)
            .visitList(bottomNonIntersecting, newBottomList);
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode newBottomCondition =
            RexUtil.composeConjunction(rexBuilder, newBottomList, false);
        final Join newBottomJoin =
            bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relA,
                relC, newJoinTypePair.getKey(), bottomJoin.isSemiJoinDone());

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
            .visitList(nonIntersecting, newTopList);
        new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
            .visitList(bottomIntersecting, newTopList);
        RexNode newTopCondition =
            RexUtil.composeConjunction(rexBuilder, newTopList, false);
        @SuppressWarnings("SuspiciousNameCombination") final Join newTopJoin =
            topJoin.copy(topJoin.getTraitSet(), newTopCondition, newBottomJoin,
                relB, newJoinTypePair.getValue(), topJoin.isSemiJoinDone());

        if (newTopJoin instanceof LogicalJoin) {
            ((LogicalJoin) newTopJoin).getJoinReorderContext().setHasTopPushThrough(true);
        }

        if (newBottomJoin instanceof LogicalJoin) {
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasTopPushThrough(false);
            ((LogicalJoin) newBottomJoin).getJoinReorderContext().setHasCommute(false);
        }

        if (newTopCondition.isAlwaysTrue() || newBottomCondition.isAlwaysTrue()) {
            return null;
        }

        assert !Mappings.isIdentity(topMapping);
        relBuilder.push(newTopJoin);
        relBuilder.project(relBuilder.fields(topMapping));
        return relBuilder.build();
    }

    /**
     * Splits a condition into conjunctions that do or do not intersect with
     * both 2 given bit set.
     */
    public static void split(
        RexNode condition,
        ImmutableBitSet bitSet,
        ImmutableBitSet bitSet2,
        List<RexNode> intersecting,
        List<RexNode> nonIntersecting) {
        ImmutableBitSet union = bitSet.union(bitSet2);
        for (RexNode node : RelOptUtil.conjunctions(condition)) {
            ImmutableBitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
            if (bitSet.intersects(inputBitSet) && bitSet2.intersects(inputBitSet) && union.contains(inputBitSet)) {
                intersecting.add(node);
            } else {
                nonIntersecting.add(node);
            }
        }
    }
}