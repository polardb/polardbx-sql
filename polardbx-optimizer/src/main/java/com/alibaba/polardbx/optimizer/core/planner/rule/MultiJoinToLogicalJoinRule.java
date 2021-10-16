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

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.LoptJoinTree;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.LoptSemiJoinOptimizer;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.BitSets;

import java.util.BitSet;
import java.util.List;

/**
 * @author shengyu
 */
public class MultiJoinToLogicalJoinRule extends LoptOptimizeJoinRule {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public static final MultiJoinToLogicalJoinRule INSTANCE =
        new MultiJoinToLogicalJoinRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a LoptOptimizeJoinRule.
     */
    public MultiJoinToLogicalJoinRule(RelBuilderFactory relBuilderFactory) {
        super(relBuilderFactory);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).isShouldUseHeuOrder();
    }

    @Override
    protected int getBestNextFactor(
        RelMetadataQuery mq,
        RelBuilder relBuilder,
        LoptMultiJoin multiJoin,
        BitSet factorsToAdd,
        BitSet factorsAdded,
        LoptSemiJoinOptimizer semiJoinOpt,
        LoptJoinTree joinTree,
        List<RexNode> filtersToAdd) {
        // iterate through the remaining factors and determine the
        // best one to add next
        int nextFactor = -1;
        int bestWeight = 0;
        RelOptCost bestCost = null;
        int[][] factorWeights = multiJoin.getFactorWeights();
        for (int factor : BitSets.toIter(factorsToAdd)) {
            Integer factIdx = multiJoin.getJoinRemovalFactor(factor);
            if (factIdx != null) {
                if (!factorsAdded.get(factIdx)) {
                    continue;
                }
            }

            if (multiJoin.isNullGenerating(factor)
                && !BitSets.contains(factorsAdded,
                multiJoin.getOuterJoinFactors(factor))) {
                continue;
            }

            int dimWeight = 0;
            for (int prevFactor : BitSets.toIter(factorsAdded)) {
                if (factorWeights[prevFactor][factor] > dimWeight) {
                    dimWeight = factorWeights[prevFactor][factor];
                }
            }

            RelOptPlanner planner = multiJoin.getMultiJoinRel().getCluster().getPlanner();
            RelOptCost cost = planner.getCostFactory().makeHugeCost();
            if ((dimWeight > 0) && (dimWeight >= bestWeight)) {
                cost = getCost(
                    mq,
                    relBuilder,
                    multiJoin,
                    planner,
                    semiJoinOpt,
                    factorsAdded,
                    joinTree,
                    filtersToAdd,
                    factor
                );
            }

            boolean shouldUpdate = ((dimWeight > bestWeight) ||
                ((dimWeight == bestWeight) &&
                    (bestCost == null || cost.isLt(bestCost))));
            if (shouldUpdate) {
                nextFactor = factor;
                bestWeight = dimWeight;
                bestCost = cost;
            }
        }

        return nextFactor;
    }

    /**
     * we consider physical join rather than logical join here
     */
    @Override
    protected RelOptCost getCost(
        RelMetadataQuery mq,
        LoptMultiJoin multiJoin,
        RelBuilder relBuilder,
        RelOptPlanner planner,
        RelNode node) {
        RelOptCost cost = planner.getCostFactory().makeHugeCost();
        if (node == null) {
            return cost;
        }
        //return the cost if already calculated
        RelOptCost tmpCost = multiJoin.getMapCost(node.getId());
        if (tmpCost != null) {
            return tmpCost;
        }
        //the cost of the subtree has not been computed
        if (node instanceof LogicalProject) {
            cost = getCost(mq, multiJoin, relBuilder, planner, ((Project) node).getInput());
        } else if (node instanceof LogicalFilter) {
            cost = getCost(mq, multiJoin, relBuilder, planner, ((Filter) node).getInput());
        } else if (node instanceof LogicalJoin) {
            cost = findMinCost(cost, chooseMinJoinCost(mq, planner, node));

            //now we switch the two factors
            Join join = (Join) node;
            RelNode left = join.getLeft();
            RelNode right = join.getRight();
            JoinRelType joinType = join.getJoinType().swap();
            RexBuilder rexBuilder =
                multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
            RexNode condition =
                swapFilter(
                    rexBuilder,
                    multiJoin,
                    left,
                    right,
                    join.getCondition());

            relBuilder.push(right)
                .push(left)
                .join(joinType, condition);

            RelNode swapJoin = relBuilder.build();
            cost = findMinCost(cost, chooseMinJoinCost(mq, planner, swapJoin));

            assert left == ((LogicalJoin) swapJoin).getRight();
            //cumulate the join cost
            cost = cost.plus(getCost(mq, multiJoin, relBuilder, planner, left));
            cost = cost.plus(getCost(mq, multiJoin, relBuilder, planner, right));
        } else {
            //other relnodes should not be reached
            cost = planner.getCostFactory().makeHugeCost();
            logger.debug("Expecting {LogicalProject,LogicalFilter,LogicalJoin}, but " + node.getClass());
        }
        //use map to record the cumulative cost
        multiJoin.putMapCost(node.getId(), cost);
        return cost;
    }

    private RexNode swapFilter(
        RexBuilder rexBuilder,
        LoptMultiJoin multiJoin,
        RelNode origLeft,
        RelNode origRight,
        RexNode condition) {
        int nFieldsOnLeft =
            origLeft.getRowType().getFieldCount();
        int nFieldsOnRight =
            origRight.getRowType().getFieldCount();
        int[] adjustments = new int[nFieldsOnLeft + nFieldsOnRight];

        for (int i = 0; i < nFieldsOnLeft; i++) {
            adjustments[i] = nFieldsOnRight;
        }
        for (int i = nFieldsOnLeft; i < (nFieldsOnLeft + nFieldsOnRight); i++) {
            adjustments[i] = -nFieldsOnLeft;
        }

        condition =
            condition.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    getJoinFields(multiJoin, origLeft, origRight),
                    getJoinFields(multiJoin, origRight, origLeft),
                    adjustments));

        return condition;
    }

    static List<RelDataTypeField> getJoinFields(
        LoptMultiJoin multiJoin,
        RelNode left,
        RelNode right) {
        RelDataTypeFactory factory = multiJoin.getMultiJoinRel().getCluster().getTypeFactory();
        RelDataType rowType =
            factory.createJoinType(
                left.getRowType(),
                right.getRowType());
        return rowType.getFieldList();
    }

    /**
     * @param multiJoin join factors being optimized
     * @param factorsAdded factors in joinTree
     * @param joinTree current join tree
     * @param filtersToAdd filters not included in joinTree
     * @param factorIdx the factor to be added
     * @return the min cost of physical join(joinTree,factorIdx)
     * and join(factorIdx, joinTree)
     */
    private RelOptCost getCost(RelMetadataQuery mq,
                               RelBuilder relBuilder,
                               LoptMultiJoin multiJoin,
                               RelOptPlanner planner,
                               LoptSemiJoinOptimizer semiJoinOpt,
                               BitSet factorsAdded,
                               LoptJoinTree joinTree,
                               List<RexNode> filtersToAdd,
                               int factorIdx) {
        BitSet factorsNeeded =
            multiJoin.getFactorsRefByFactor(factorIdx).toBitSet();
        if (multiJoin.isNullGenerating(factorIdx)) {
            factorsNeeded.or(multiJoin.getOuterJoinFactors(factorIdx).toBitSet());
        }
        factorsNeeded.and(factorsAdded);

        // get join type, note that outer join's type is always left
        // because we consider the non-outer part first
        JoinRelType joinType;
        if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
            assert multiJoin.getNumJoinFactors() == 2;
            joinType = JoinRelType.FULL;
        } else if (multiJoin.isNullGenerating(factorIdx)) {
            joinType = JoinRelType.LEFT;
        } else {
            joinType = JoinRelType.INNER;
        }

        //get right subtree
        LoptJoinTree rightTree =
            new LoptJoinTree(
                semiJoinOpt.getChosenSemiJoin(factorIdx),
                factorIdx);

        //get condition
        RexNode condition;
        if (joinType == JoinRelType.LEFT) {
            condition = multiJoin.getOuterJoinCond(factorIdx);
        } else {
            condition =
                addFilters(
                    multiJoin,
                    joinTree,
                    -1,
                    rightTree,
                    Lists.newArrayList(filtersToAdd),
                    false);
        }

        LoptJoinTree topTreeLeft = createJoinSubtree(
            mq,
            relBuilder,
            multiJoin,
            joinTree,
            rightTree,
            condition,
            joinType,
            Lists.newArrayList(filtersToAdd),
            true,
            false);

        RelOptCost cost = planner.getCostFactory().makeHugeCost();
        cost = findMinCost(cost,
            chooseMinJoinCost(mq, planner, topTreeLeft.getJoinTree()));

        //now we switch the two join factor
        if (joinType == JoinRelType.LEFT) {
            condition = multiJoin.getOuterJoinCond(factorIdx);
            joinType = JoinRelType.RIGHT;
        } else {
            condition = addFilters(
                multiJoin,
                joinTree,
                -1,
                rightTree,
                Lists.newArrayList(filtersToAdd),
                false);
        }

        LoptJoinTree topTreeRight = createJoinSubtree(
            mq,
            relBuilder,
            multiJoin,
            rightTree,
            joinTree,
            condition,
            joinType,
            Lists.newArrayList(filtersToAdd),
            true,
            false);

        assert topTreeLeft.getLeft().getJoinTree() == topTreeRight.getRight().getJoinTree();
        cost = findMinCost(cost,
            chooseMinJoinCost(mq, planner, topTreeRight.getJoinTree()));
        return cost;
    }

    /**
     * we only consider the cost of join on the top, because the
     * cumulative cost below remains unchanged
     *
     * @param joinTree the join tree to be computed
     * @return the min cost of the join
     */
    private RelOptCost chooseMinJoinCost(
        RelMetadataQuery mq,
        RelOptPlanner planner,
        RelNode joinTree) {
        //get the volcano planner for computeSelfCost
        assert planner instanceof VolcanoPlanner;

        while (joinTree instanceof LogicalFilter || joinTree instanceof LogicalProject) {
            //outer join may have project on top of the join
            //removable join may have project on top of the join
            joinTree = ((SingleRel) joinTree).getInput();
        }

        if (!(joinTree instanceof LogicalJoin)) {
            logger.debug("we should have a logical join, but " + joinTree.getClass());
            return planner.getCostFactory().makeHugeCost();
        }

        final LogicalJoin join = (LogicalJoin) joinTree;

        RelOptCost cost = planner.getCostFactory().makeHugeCost();
        //hash join
        RelNode hashJoinResult = transformHashJoin(join);
        if (hashJoinResult instanceof HashJoin) {
            RelOptCost relCost = hashJoinResult.computeSelfCost(planner, mq);
            cost = findMinCost(cost, relCost);
        }

        //bka join
        RelNode bkaJoinResult = transformBKAJoin(join);
        if (bkaJoinResult instanceof BKAJoin) {
            RelOptCost relCost = bkaJoinResult.computeSelfCost(planner, mq);
            cost = findMinCost(cost, relCost);
        }

        //nested loop join
        RelNode nlJoinResult = transformNLJoin(join);
        if (nlJoinResult instanceof NLJoin) {
            RelOptCost relCost = nlJoinResult.computeSelfCost(planner, mq);
            cost = findMinCost(cost, relCost);
        }

        //TODO: consider sort merge join
        return cost;
    }

    private RelOptCost findMinCost(RelOptCost cost1, RelOptCost cost2) {
        return (cost1.isLt(cost2)) ? cost1 : cost2;
    }

    private RelNode transformHashJoin(LogicalJoin join) {
        CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();
        if (!CBOUtil.checkHashJoinCondition(join, join.getCondition(), join.getLeft().getRowType().getFieldCount(),
            equalConditionHolder, otherConditionHolder)) {
            return null;
        }

        HashJoin hashJoin = HashJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
            join.getLeft(),
            join.getRight(),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints(),
            equalConditionHolder.getRexNode(),
            otherConditionHolder.getRexNode(),
            false);
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_HASH_JOIN);
        if (fixedCost != null) {
            hashJoin.setFixedCost(fixedCost);
        }
        return hashJoin;
    }

    /**
     * copied from LogicalJoinToBKAJoinRule
     */
    private RelNode transformBKAJoin(LogicalJoin join) {
        RexUtils.RestrictType restrictType;
        switch (join.getJoinType()) {
        case LEFT:
        case INNER:
            restrictType = RexUtils.RestrictType.RIGHT;
            break;
        case RIGHT:
            restrictType = RexUtils.RestrictType.LEFT;
            break;
        default:
            return null;
        }

        if (!RexUtils.isBatchKeysAccessCondition(join, join.getCondition(), join.getLeft().getRowType().getFieldCount(),
            restrictType,
            (Pair<RelDataType, RelDataType> relDataTypePair) -> CBOUtil.bkaTypeCheck(relDataTypePair))) {
            return null;
        }
        if (!CBOUtil.canBKAJoin(join)) {
            return null;
        }

        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        BKAJoin bkaJoin = BKAJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
            left,
            right,
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints());
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_BKA_JOIN);
        if (fixedCost != null) {
            bkaJoin.setFixedCost(fixedCost);
        }
        return bkaJoin;
    }

    private RelNode transformNLJoin(LogicalJoin join) {
        NLJoin nlJoin = NLJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
            join.getLeft(),
            join.getRight(),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints());
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_NL_JOIN);
        if (fixedCost != null) {
            nlJoin.setFixedCost(fixedCost);
        }
        return nlJoin;
    }
}