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

import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.utils.CalciteUtils.shiftRightFilter;

/**
 * 将JION之上的Filter下推至JOIN的条件中
 *
 * @author lingce.ldm 2017-07-26 16:30
 */
public class TddlFilterJoinRule extends FilterJoinRule {

    public static final FilterJoinRule TDDL_FILTER_ON_JOIN = new TddlFilterJoinRule();

    protected TddlFilterJoinRule() {
        super(operand(Filter.class, operand(Join.class, RelOptRule.any())),
            "TddlFilterJoinRule",
            true,
            RelFactories.LOGICAL_BUILDER,
            TRUE_PREDICATE);
    }

    protected TddlFilterJoinRule(RelOptRuleOperand operand, String id) {
        super(operand, id, true, RelFactories.LOGICAL_BUILDER, TRUE_PREDICATE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Join join = call.rel(1);
        perform(call, filter, join);
    }

    @Override
    protected void perform(RelOptRuleCall call, Filter filter, Join join) {
        final List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        final List<RexNode> originJoinFilters = ImmutableList.copyOf(joinFilters);

        final List<RexNode> aboveFilters = RelOptUtil.conjunctions(filter.getCondition());
        final ImmutableList<RexNode> origAboveFilters = ImmutableList.copyOf(aboveFilters);

        /**
         * Simplify join type
         */
        JoinRelType joinType = join.getJoinType();
        if (!origAboveFilters.isEmpty() && joinType != JoinRelType.INNER && !(join instanceof SemiJoin)) {
            joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
        }

        // 如果左表仍然是一个JOIN节点，则需要将leftFilter下推
        boolean pushFilterToLeft = false;
        if (((HepRelVertex) join.getLeft()).getCurrentRel() instanceof Join) {
            pushFilterToLeft = true;
        }

        boolean filterPushed = false;
        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();
        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        if (RelOptUtil.classifyFilters(join,
            aboveFilters,
            joinType,
            true,
            !joinType.generatesNullsOnLeft(),
            !joinType.generatesNullsOnRight(),
            joinFilters,
            leftFilters,
            rightFilters)) {
            filterPushed = true;
        }

        validateJoinFilters(aboveFilters, joinFilters, join, joinType);

        if (leftFilters.isEmpty() && rightFilters.isEmpty() && joinFilters.size() == originJoinFilters.size()) {
            if (Sets.newHashSet(joinFilters).equals(Sets.newHashSet(originJoinFilters))) {
                filterPushed = false;
            }
        }

//        if (RexUtil.containsCorrelation(filter.getCondition()) || (null != filter.getVariablesSet()
//                                                                   && filter.getVariablesSet().size() > 0)) {
//            // If there is a correlation condition anywhere in the filter, don't
//            // push this filter past join since in some cases it can prevent a
//            // Correlate from being de-correlated.
//            return;
//        }

        if (checkScalarExists(filter)) {
            return;
        }

        if ((!filterPushed && joinType == join.getJoinType())
            || (joinFilters.isEmpty() && rightFilters.isEmpty() && leftFilters.isEmpty())) {
            return;
        }

        final RelBuilder relBuilder = call.builder();
        RelNode leftRel = join.getLeft();
        RelNode rightRel = join.getRight();
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        /**
         * Create filter for left/right join.
         */
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.RIGHT) {
            if (leftFilters.isEmpty() && rightFilters.isEmpty()) {
                return;
            }

            if (!leftFilters.isEmpty()) {
                leftRel = relBuilder.push(join.getLeft()).filter(leftFilters).build();
            }

            if (!rightFilters.isEmpty()) {
                rightRel = relBuilder.push(join.getRight()).filter(rightFilters).build();
            }

            final RexNode joinFilter = RexUtil.composeConjunction(rexBuilder, originJoinFilters, false);
            RelNode newJoinRel = join
                .copy(join.getTraitSet(), joinFilter, leftRel, rightRel, joinType, join.isSemiJoinDone());
            call.getPlanner().onCopy(join, newJoinRel);

            if (!leftFilters.isEmpty()) {
                call.getPlanner().onCopy(filter, leftRel);
            }

            if (!rightFilters.isEmpty()) {
                call.getPlanner().onCopy(filter, rightRel);
            }

            relBuilder.push(newJoinRel);

            // create a FilterRel on top of the join if needed
            relBuilder.filter(
                RexUtil.fixUp(rexBuilder, aboveFilters, RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));

            call.transformTo(relBuilder.build());
            return;
        }

        /**
         * 等价条件推导
         */
        RexBuilder rB = join.getCluster().getRexBuilder();
        RelMdPredicates.JoinConditionBasedPredicateInference jI =
            new RelMdPredicates.JoinConditionBasedPredicateInference(
                join,
                RexUtil.composeConjunction(rB, leftFilters, false),
                RexUtil.composeConjunction(rB, rightFilters, false));

        RelOptPredicateList preds = jI.inferPredicates(false);
        if (preds.leftInferredPredicates.size() > 0) {
            leftFilters.addAll(preds.leftInferredPredicates);
        }
        if (preds.rightInferredPredicates.size() > 0) {
            rightFilters.addAll(preds.rightInferredPredicates);
        }

        // 没有条件可以下推至左表
        if (!leftFilters.isEmpty() && pushFilterToLeft) {
            leftRel = relBuilder.push(join.getLeft()).filter(leftFilters).build();
        } else {
            leftRel = join.getLeft();
        }

        final ImmutableList<RelDataType> fieldType = ImmutableList.<RelDataType>builder()
            .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
            .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType()))
            .build();

        final RexNode joinFilter = RexUtil.composeConjunction(rexBuilder,
            RexUtil.fixUp(rexBuilder,
                buildJoinFilter(joinFilters, leftFilters, rightFilters, join, leftRel.getRowType().getFieldCount()),
                fieldType),
            false);

        if (joinFilter.isAlwaysTrue() && joinType == join.getJoinType()) {
            return;
        }

        RelNode newJoinRel = join
            .copy(join.getTraitSet(), joinFilter, leftRel, rightRel, joinType, join.isSemiJoinDone());
        call.getPlanner().onCopy(join, newJoinRel);

        if (pushFilterToLeft) {
            call.getPlanner().onCopy(filter, leftRel);
        }

        relBuilder.push(newJoinRel);

        // create a FilterRel on top of the join if needed
        relBuilder.filter(
            RexUtil.fixUp(rexBuilder, aboveFilters, RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));

        call.transformTo(relBuilder.build());
    }

    public static boolean checkScalarExists(Filter filter) {
        if (filter != null && filter.getCondition() != null) {
            OptimizerUtils.DynamicDeepFinder dynamicDeepFinder =
                new OptimizerUtils.DynamicDeepFinder(Lists.newLinkedList());
            filter.getCondition().accept(dynamicDeepFinder);
            if (dynamicDeepFinder.getScalar().size() > 0) {
                return true;
            }
        }
        return false;
    }

    protected List<RexNode> buildJoinFilter(List<RexNode> joinFilters, List<RexNode> leftFilters,
                                            List<RexNode> rightFilters, Join join, int offset) {
        List<RexNode> newRightFilters = shiftRightFilter(rightFilters,
            offset,
            join.getRight().getRowType(),
            SqlValidatorUtil.deriveJoinRowType(join.getLeft().getRowType(),
                join.getRight().getRowType(),
                join.getJoinType(),
                join.getCluster().getTypeFactory(),
                null,
                join.getSystemFieldList()),
            join.getCluster().getRexBuilder());
        joinFilters.addAll(leftFilters);
        joinFilters.addAll(newRightFilters);
        return joinFilters;
    }
}
