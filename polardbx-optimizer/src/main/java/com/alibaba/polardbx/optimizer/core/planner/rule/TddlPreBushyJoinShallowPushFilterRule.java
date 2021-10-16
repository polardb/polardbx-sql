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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
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
 * push filter above current join to all the join under current join
 *
 * @author zilin.zl 2018-12-10
 */
public class TddlPreBushyJoinShallowPushFilterRule extends FilterJoinRule {

    public static final FilterJoinRule INSTANCE = new TddlPreBushyJoinShallowPushFilterRule();

    protected TddlPreBushyJoinShallowPushFilterRule() {
        super(operand(Filter.class, operand(Join.class, RelOptRule.any())),
            "TddlPreBushyJoinShallowPushFilterRule",
            true,
            RelFactories.LOGICAL_BUILDER,
            TRUE_PREDICATE);
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

        if (filter != null && filter.getCondition() != null) {
            OptimizerUtils.DynamicDeepFinder dynamicDeepFinder =
                new OptimizerUtils.DynamicDeepFinder(Lists.newLinkedList());
            filter.getCondition().accept(dynamicDeepFinder);
            if (dynamicDeepFinder.getScalar().size() > 0) {
                return;
            }
        }

        /**
         * Simplify join type
         */
        JoinRelType joinType = join.getJoinType();
        if (!origAboveFilters.isEmpty() && joinType != JoinRelType.INNER && !(join instanceof LogicalSemiJoin)) {
            joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
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
            false,  // always forbid push deeper
            false, // always forbid push deeper
            joinFilters,
            leftFilters,     // finally leftFilters must be empty
            rightFilters)) { // finally rightFilters must be empty
            filterPushed = true;
        }

        validateJoinFilters(aboveFilters, joinFilters, join, joinType);

        if (leftFilters.isEmpty() && rightFilters.isEmpty() && joinFilters.size() == originJoinFilters.size()) {
            if (Sets.newHashSet(joinFilters).equals(Sets.newHashSet(originJoinFilters))) {
                filterPushed = false;
            }
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
         * left/right join just return
         */
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.RIGHT) {
            return;
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

        relBuilder.push(newJoinRel);

        // create a FilterRel on top of the join if needed
        relBuilder.filter(
            RexUtil.fixUp(rexBuilder, aboveFilters, RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));

        call.transformTo(relBuilder.build());
    }

    private List<RexNode> buildJoinFilter(List<RexNode> joinFilters, List<RexNode> leftFilters,
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
