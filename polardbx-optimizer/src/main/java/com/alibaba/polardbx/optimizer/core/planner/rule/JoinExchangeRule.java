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

import com.alibaba.polardbx.optimizer.core.planner.OneStepTransformer;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
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

public class JoinExchangeRule extends RelOptRule {

    public static final JoinExchangeRule INSTANCE =
        new JoinExchangeRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            RelFactories.LOGICAL_BUILDER, "JoinExchangeRule");

    public static final JoinExchangeRule BOTH_PROJECT =
        new JoinExchangeRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER, "JoinExchangeRule:BOTH_PROJECT");

    public static final JoinExchangeRule LEFT_PROJECT =
        new JoinExchangeRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            RelFactories.LOGICAL_BUILDER, "JoinExchangeRule:LEFT_PROJECT");

    public static final JoinExchangeRule RIGHT_PROJECT =
        new JoinExchangeRule(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER, "JoinExchangeRule:RIGHT_PROJECT");

    public JoinExchangeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final LogicalJoin topJoin = call.rel(0);
        if (topJoin.getJoinReorderContext().isHasCommute() || topJoin.getJoinReorderContext().isHasLeftAssociate()
            || topJoin.getJoinReorderContext().isHasRightAssociate() || topJoin.getJoinReorderContext()
            .isHasExchange()) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        if (call.getRule() == INSTANCE) {
            onMatchInstance(call);
        } else if (call.getRule() == BOTH_PROJECT) {
            onMatchBothProjectInstance(call);
        } else if (call.getRule() == LEFT_PROJECT) {
            onMatchLeftProjectInstance(call);
        } else if (call.getRule() == RIGHT_PROJECT) {
            onMatchRightProjectInstance(call);
        }
    }

    protected void onMatchInstance(RelOptRuleCall call) {
        final LogicalJoin topJoin = call.rel(0);
        final LogicalJoin leftJoin = call.rel(1);
        final LogicalJoin rightJoin = call.rel(2);
        RelNode output = transform(topJoin, leftJoin, rightJoin, call.builder());
        if (output != null) {
            call.transformTo(output);
        }
    }

    protected void onMatchBothProjectInstance(RelOptRuleCall call) {
        final LogicalJoin inputTopJoin = call.rel(0);
        final LogicalProject leftLogicalProject = call.rel(1);
        final LogicalJoin leftJoin = call.rel(2);
        final LogicalProject rightLogicalProject = call.rel(3);
        final LogicalJoin rightJoin = call.rel(4);

        LogicalJoin beforeProjectPullUpJoin = inputTopJoin.copy(
            inputTopJoin.getTraitSet(),
            inputTopJoin.getCondition(),
            leftLogicalProject,
            rightLogicalProject,
            inputTopJoin.getJoinType(),
            inputTopJoin.isSemiJoinDone());

        RelNode afterProjectPullUpJoin =
            OneStepTransformer.transform(beforeProjectPullUpJoin, JoinProjectTransposeRule.BOTH_PROJECT);
        if (afterProjectPullUpJoin == beforeProjectPullUpJoin) {
            return;
        }

        assert afterProjectPullUpJoin instanceof LogicalProject;

        LogicalProject newLogicalProject = (LogicalProject) afterProjectPullUpJoin;

        LogicalJoin topJoin = (LogicalJoin) ((LogicalProject) afterProjectPullUpJoin).getInput();

        RelNode transformResult = transform(topJoin, leftJoin, rightJoin, call.builder());

        if (transformResult != null) {
            newLogicalProject.replaceInput(0, transformResult);
            RelNode output = OneStepTransformer.transform(newLogicalProject, ProjectMergeRule.INSTANCE);
            output = OneStepTransformer.transform(output, ProjectRemoveRule.INSTANCE);
            call.transformTo(output);
        }
    }

    protected void onMatchLeftProjectInstance(RelOptRuleCall call) {
        final LogicalJoin inputTopJoin = call.rel(0);
        final LogicalProject leftLogicalProject = call.rel(1);
        final LogicalJoin leftJoin = call.rel(2);
        final LogicalJoin rightJoin = call.rel(3);

        LogicalJoin beforeProjectPullUpJoin = inputTopJoin.copy(
            inputTopJoin.getTraitSet(),
            inputTopJoin.getCondition(),
            leftLogicalProject,
            rightJoin,
            inputTopJoin.getJoinType(),
            inputTopJoin.isSemiJoinDone());

        RelNode afterProjectPullUpJoin =
            OneStepTransformer.transform(beforeProjectPullUpJoin, JoinProjectTransposeRule.LEFT_PROJECT);
        if (afterProjectPullUpJoin == beforeProjectPullUpJoin) {
            return;
        }

        assert afterProjectPullUpJoin instanceof LogicalProject;

        LogicalProject newLogicalProject = (LogicalProject) afterProjectPullUpJoin;

        LogicalJoin topJoin = (LogicalJoin) ((LogicalProject) afterProjectPullUpJoin).getInput();

        RelNode transformResult = transform(topJoin, leftJoin, rightJoin, call.builder());

        if (transformResult != null) {
            newLogicalProject.replaceInput(0, transformResult);
            RelNode output = OneStepTransformer.transform(newLogicalProject, ProjectMergeRule.INSTANCE);
            output = OneStepTransformer.transform(output, ProjectRemoveRule.INSTANCE);
            call.transformTo(output);
        }
    }

    protected void onMatchRightProjectInstance(RelOptRuleCall call) {
        final LogicalJoin inputTopJoin = call.rel(0);
        final LogicalJoin leftJoin = call.rel(1);
        final LogicalProject rightLogicalProject = call.rel(2);
        final LogicalJoin rightJoin = call.rel(3);

        LogicalJoin beforeProjectPullUpJoin = inputTopJoin.copy(
            inputTopJoin.getTraitSet(),
            inputTopJoin.getCondition(),
            leftJoin,
            rightLogicalProject,
            inputTopJoin.getJoinType(),
            inputTopJoin.isSemiJoinDone());

        RelNode afterProjectPullUpJoin =
            OneStepTransformer.transform(beforeProjectPullUpJoin, JoinProjectTransposeRule.RIGHT_PROJECT);
        if (afterProjectPullUpJoin == beforeProjectPullUpJoin) {
            return;
        }

        assert afterProjectPullUpJoin instanceof LogicalProject;

        LogicalProject newLogicalProject = (LogicalProject) afterProjectPullUpJoin;

        LogicalJoin topJoin = (LogicalJoin) ((LogicalProject) afterProjectPullUpJoin).getInput();

        RelNode transformResult = transform(topJoin, leftJoin, rightJoin, call.builder());

        if (transformResult != null) {
            newLogicalProject.replaceInput(0, transformResult);
            RelNode output = OneStepTransformer.transform(newLogicalProject, ProjectMergeRule.INSTANCE);
            output = OneStepTransformer.transform(output, ProjectRemoveRule.INSTANCE);
            call.transformTo(output);
        }
    }

    protected RelNode transform(final LogicalJoin topJoin, final LogicalJoin leftJoin, final LogicalJoin rightJoin,
                                RelBuilder relBuilder) {
        final RelNode relA = leftJoin.getLeft();
        final RelNode relB = leftJoin.getRight();
        final RelNode relC = rightJoin.getLeft();
        final RelNode relD = rightJoin.getRight();

        final RelOptCluster cluster = topJoin.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        //        topJoin
        //        /      \
        //   leftJoin  rightJoin
        //    /    \    /    \
        //   A      B  C      D

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final int dCount = relD.getRowType().getFieldCount();
        final ImmutableBitSet acBitSet =
            ImmutableBitSet.range(0, aCount).union(ImmutableBitSet.range(aCount + bCount, aCount + bCount + cCount));
        final ImmutableBitSet bdBitSet = ImmutableBitSet.range(aCount, aCount + bCount)
            .union(ImmutableBitSet.range(aCount + bCount + cCount, aCount + bCount + cCount + dCount));

        if (!topJoin.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return null;
        }

        // If either join is not inner, we cannot proceed.
        // (Is this too strict?)
        if (topJoin.getJoinType() != JoinRelType.INNER
            || leftJoin.getJoinType() != JoinRelType.INNER
            || rightJoin.getJoinType() != JoinRelType.INNER) {
            return null;
        }

        // Goal is to transform to
        //
        //         newTopJoin
        //          /      \
        //   newLeftJoin newRightJoin
        //    /    \        /    \
        //   A      C      B      D

        final List<RexNode> left = Lists.newArrayList();
        final List<RexNode> right = Lists.newArrayList();
        final List<RexNode> top = Lists.newArrayList();

        final List<RexNode> allConditionList = Lists.newArrayList();
        allConditionList.add(topJoin.getCondition());
        allConditionList.add(leftJoin.getCondition());

        // shift right join condition right with left join field count offset
        // target: | A       | B | C      | D   |
        // source: | C      | D   |
        final Mappings.TargetMapping shiftRightJoinMapping =
            Mappings.offsetTarget(Mappings.createIdentity(cCount + dCount), aCount + bCount);
        final List<RexNode> shiftRightJoinList = Lists.newArrayList();
        new RexPermuteInputsShuttle(shiftRightJoinMapping, relA, relB)
            .visitList(RelOptUtil.conjunctions(rightJoin.getCondition()), shiftRightJoinList);
        RexNode shiftRightJoinCondition =
            RexUtil.composeConjunction(rexBuilder, shiftRightJoinList, false);

        allConditionList.add(shiftRightJoinCondition);
        RexNode allCondition = RexUtil.composeConjunction(rexBuilder, allConditionList, false);

        for (RexNode node : RelOptUtil.conjunctions(allCondition)) {
            ImmutableBitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
            if (acBitSet.contains(inputBitSet)) {
                left.add(node);
            } else if (bdBitSet.contains(inputBitSet)) {
                right.add(node);
            } else {
                top.add(node);
            }
        }

        // Mapping for moving conditions from allCondtion to left.
        // target: | A       | C      |
        // source: | A       | B | C      | D   |

        final Mappings.TargetMapping leftMapping =
            Mappings.createShiftMapping(
                aCount + bCount + cCount + dCount,
                0, 0, aCount,
                aCount, aCount + bCount, cCount);
        final List<RexNode> newLeftList = Lists.newArrayList();
        new RexPermuteInputsShuttle(leftMapping, relA, relC)
            .visitList(left, newLeftList);
        RexNode newLeftCondition =
            RexUtil.composeConjunction(rexBuilder, newLeftList, false);

        final Join newLeftJoin =
            leftJoin.copy(leftJoin.getTraitSet(), newLeftCondition, relA,
                relC, JoinRelType.INNER, false);

        // Mapping for moving conditions from allCondtion to right.
        // target: | B | D   |
        // source: | A       | B | C      | D   |

        final Mappings.TargetMapping rightMapping =
            Mappings.createShiftMapping(
                aCount + bCount + cCount + dCount,
                0, aCount, bCount,
                bCount, aCount + bCount + cCount, dCount);
        final List<RexNode> newRightList = Lists.newArrayList();
        new RexPermuteInputsShuttle(rightMapping, relB, relD)
            .visitList(right, newRightList);
        RexNode newRightCondition =
            RexUtil.composeConjunction(rexBuilder, newRightList, false);

        final Join newRightJoin =
            leftJoin.copy(rightJoin.getTraitSet(), newRightCondition, relB,
                relD, JoinRelType.INNER, false);

        // Mapping for moving conditions from allCondtion to right.
        // target: | A       | C      | B | D   |
        // source: | A       | B | C      | D   |

        final Mappings.TargetMapping topMapping =
            Mappings.createShiftMapping(
                aCount + bCount + cCount + dCount,
                0, 0, aCount,
                aCount, aCount + bCount, cCount,
                aCount + cCount, aCount, bCount,
                aCount + bCount + cCount, aCount + bCount + cCount, dCount);
        final List<RexNode> newTopList = Lists.newArrayList();
        new RexPermuteInputsShuttle(topMapping, newLeftJoin, newRightJoin)
            .visitList(top, newTopList);

        RexNode newTopCondition =
            RexUtil.composeConjunction(rexBuilder, newTopList, false);
        final Join newTopJoin =
            topJoin.copy(topJoin.getTraitSet(), newTopCondition, newLeftJoin,
                newRightJoin, JoinRelType.INNER, false);

        if (newTopJoin instanceof LogicalJoin) {
            ((LogicalJoin) newTopJoin).getJoinReorderContext().setHasExchange(true);
        }

        if (newLeftJoin instanceof LogicalJoin) {
            ((LogicalJoin) newLeftJoin).getJoinReorderContext().setHasCommute(false);
            ((LogicalJoin) newLeftJoin).getJoinReorderContext().setHasLeftAssociate(false);
            ((LogicalJoin) newLeftJoin).getJoinReorderContext().setHasRightAssociate(false);
            ((LogicalJoin) newLeftJoin).getJoinReorderContext().setHasExchange(false);
        }

        if (newRightJoin instanceof LogicalJoin) {
            ((LogicalJoin) newRightJoin).getJoinReorderContext().setHasCommute(false);
            ((LogicalJoin) newRightJoin).getJoinReorderContext().setHasLeftAssociate(false);
            ((LogicalJoin) newRightJoin).getJoinReorderContext().setHasRightAssociate(false);
            ((LogicalJoin) newRightJoin).getJoinReorderContext().setHasExchange(false);
        }

        if (newTopCondition.isAlwaysTrue() || newLeftCondition.isAlwaysTrue() || newRightCondition.isAlwaysTrue()) {
            return null;
        }

        assert !Mappings.isIdentity(topMapping);
        relBuilder.push(newTopJoin);
        relBuilder.project(relBuilder.fields(topMapping));

        RelNode output = relBuilder.build();
        return output;
    }
}
