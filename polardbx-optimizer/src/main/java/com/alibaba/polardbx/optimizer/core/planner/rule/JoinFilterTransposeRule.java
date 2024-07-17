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

import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Planner rule that matches a
 * {@link Join}, one of whose inputs have
 * {@link LogicalFilter}, and
 * pulls the filter(s) up.
 *
 * <p>Filter will not be pulled up in the following two cases:
 * 1. the filter originates from a null generating input in an outer join
 * 2. the filter contains sueQuery
 *
 * @author shengyu
 */
public class JoinFilterTransposeRule extends RelOptRule {
    //~ Static fields/initializers ---------------------------------------------

    public static final JoinFilterTransposeRule BOTH_FILTER =
        new JoinFilterTransposeRule(
            operand(LogicalJoin.class,
                operand(LogicalFilter.class, any()),
                operand(LogicalFilter.class, any())),
            "JoinFilterTransposeRule(Filter-Filter)");

    public static final JoinFilterTransposeRule LEFT_FILTER =
        new JoinFilterTransposeRule(
            operand(LogicalJoin.class,
                some(operand(LogicalFilter.class, any()))),
            "JoinProjectTransposeRule(Filter-Other)");

    public static final JoinFilterTransposeRule RIGHT_FILTER =
        new JoinFilterTransposeRule(
            operand(
                LogicalJoin.class,
                operand(RelNode.class, any()),
                operand(LogicalFilter.class, any())),
            "JoinFilterTransposeRule(Other-Filter)");

    /**
     * Creates a JoinFilterTransposeRule with default factory.
     */
    public JoinFilterTransposeRule(
        RelOptRuleOperand operand,
        String description) {
        super(operand, RelFactories.LOGICAL_BUILDER, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin joinRel = call.rel(0);
        JoinRelType joinType = joinRel.getJoinType();

        //don't pull filters above semi join in case of filter loss
        if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI || joinType == JoinRelType.LEFT_SEMI) {
            return;
        }

        // the left/right {filter,join input} pair
        // if the input is not filter, key will be null, value will be the original input
        // otherwise key will be the filter, value will be the input of filter
        Pair<LogicalFilter, RelNode> left;
        Pair<LogicalFilter, RelNode> right;

        //get the pair
        if (hasLeftFilter(call)
            && !joinType.generatesNullsOnLeft()) {
            left = new Pair<>(call.rel(1), ((LogicalFilter) call.rel(1)).getInput());
        } else {
            left = new Pair<>(null, call.rel(1));
        }
        if (hasRightFilter(call)
            && !joinType.generatesNullsOnRight()) {
            right = new Pair<>(call.rel(2), ((LogicalFilter) call.rel(2)).getInput());
        } else {
            right = new Pair<>(null, joinRel.getRight());
        }

        if (left.getKey() == null && right.getKey() == null) {
            return;
        }
        //can't deal with correlation or subQuery
        ImmutableList<Pair<LogicalFilter, RelNode>> children = ImmutableList.of(left, right);
        for (Pair<LogicalFilter, RelNode> child : children) {
            if (child.getKey() != null) {
                LogicalFilter filter = child.getKey();
                //can't deal with correlation
                if (RexUtil.containsCorrelation(filter.getCondition())) {
                    return;
                }
                //can't deal with subQuery
                if (RexUtil.hasSubQuery(filter.getCondition())) {
                    return;
                }

                // don't pull having, as it will always be a select
                RelNode filterInput = filter.getInput();
                if (filterInput instanceof HepRelVertex) {
                    filterInput = ((HepRelVertex) filterInput).getCurrentRel();
                }
                if (filterInput instanceof LogicalAggregate) {
                    return;
                }
            }
        }

        //build filter conditions
        List<RexNode> filterConditions = new ArrayList<>(2);
        //leave left filter as it was
        if (left.getKey() != null) {
            filterConditions.add(left.getKey().getCondition());
        }
        //build right filter
        final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        RexNode shiftedFilter = shiftRightFilter(
            left.getValue().getRowType().getFieldCount(),
            rexBuilder,
            joinRel,
            right.getKey());
        if (shiftedFilter != null) {
            filterConditions.add(shiftedFilter);
        }
        RexNode newFilterConditions = RexUtil.composeConjunction(rexBuilder, filterConditions, false);

        //build new join
        Join newJoinRel =
            joinRel.copy(joinRel.getTraitSet(), joinRel.getCondition(),
                left.getValue(), right.getValue(), joinRel.getJoinType(),
                joinRel.isSemiJoinDone());

        //build new filter
        LogicalFilter filter = LogicalFilter.create(newJoinRel, newFilterConditions,
            (ImmutableSet<CorrelationId>) joinRel.getVariablesSet());
        RelUtils.changeRowType(filter, joinRel.getRowType());

        call.transformTo(filter);
    }

    protected boolean hasLeftFilter(RelOptRuleCall call) {
        return call.rel(1) instanceof LogicalFilter;
    }

    protected boolean hasRightFilter(RelOptRuleCall call) {
        if (call.rels.length != 3) {
            return false;
        }
        return call.rel(2) instanceof LogicalFilter;
    }

    /**
     * shift right the filter's reference by offset
     *
     * @param offset the offset
     * @param join the original join, whose right side should be shifted
     * @param right the filter to be shifted, null means the right side of join is not a filter
     * @return a new filter, null if right is not a filter
     */
    private static RexNode shiftRightFilter(
        int offset,
        RexBuilder rexBuilder,
        LogicalJoin join,
        LogicalFilter right) {
        if (right == null) {
            return null;
        }
        int[] adjustments = new int[right.getRowType().getFieldCount()];
        Arrays.fill(adjustments, offset);

        return right.getCondition().accept(
            new RelOptUtil.RexInputConverter(
                rexBuilder,
                right.getRowType().getFieldList(),
                join.getRowType().getFieldList(),
                adjustments));
    }
}
