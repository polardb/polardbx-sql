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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.BushyJoin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Created by yunhan.lyh on 2018/6/28.
 */

public class LogicalJoinToBushyJoinRule extends JoinToMultiJoinRule {
    public static final LogicalJoinToBushyJoinRule INSTANCE =
        new LogicalJoinToBushyJoinRule(LogicalJoin.class, RelFactories.LOGICAL_BUILDER);

    public LogicalJoinToBushyJoinRule(Class<? extends Join> clazz,
                                      RelBuilderFactory relBuilderFactory) {
        super(clazz, relBuilderFactory);
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
        final Join origJoin = call.rel(0);
        final RelNode left = call.rel(1);
        final RelNode right = call.rel(2);

        // Prescan separately the left subtree and the right with
        // ShardingRelVisitor. Fetch traversal mapping for later rules.
        if (origJoin.getJoinType() != JoinRelType.INNER) {
            return;
        }

        // combine the children MultiJoin inputs into an array of inputs
        // for the new MultiJoin
        final List<ImmutableBitSet> projFieldsList = Lists.newArrayList();
        final List<int[]> joinFieldRefCountsList = Lists.newArrayList();
        final List<RelNode> newInputs =
            combineInputs(
                origJoin,
                left,
                right,
                projFieldsList,
                joinFieldRefCountsList);

        // combine the outer join information from the left and right
        // inputs, and include the outer join information from the current
        // join, if it's a left/right outer join
        final List<Pair<JoinRelType, RexNode>> joinSpecs = Lists.newArrayList();
        combineOuterJoins(
            origJoin,
            newInputs,
            left,
            right,
            joinSpecs);

        // pull up the join filters from the children MultiJoinRels and
        // combine them with the join filter associated with this LogicalJoin to
        // form the join filter for the new MultiJoin
        List<RexNode> newJoinFilters = combineJoinFilters(origJoin, left, right);

        // add on the join field reference counts for the join condition
        // associated with this LogicalJoin
        final ImmutableMap<Integer, ImmutableIntList> newJoinFieldRefCountsMap =
            addOnJoinFieldRefCounts(newInputs,
                origJoin.getRowType().getFieldCount(),
                origJoin.getCondition(),
                joinFieldRefCountsList);

        List<RexNode> newPostJoinFilters =
            combinePostJoinFilters(origJoin, left, right);

        final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();

        RelNode bushyJoin =
            new BushyJoin(
                origJoin.getCluster(),
                newInputs,
                RexUtil.composeConjunction(rexBuilder, newJoinFilters, false),
                origJoin.getRowType(),
                origJoin.getJoinType() == JoinRelType.FULL,
                Pair.right(joinSpecs),
                Pair.left(joinSpecs),
                projFieldsList,
                newJoinFieldRefCountsMap,
                RexUtil.composeConjunction(rexBuilder, newPostJoinFilters, true));

        call.transformTo(bushyJoin);
    }
}
