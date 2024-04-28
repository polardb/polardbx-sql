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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;
import java.util.Set;

public class JoinAggToJoinAggSemiJoinRule extends RelOptRule {
    public static final JoinAggToJoinAggSemiJoinRule INSTANCE =
        new JoinAggToJoinAggSemiJoinRule(
            operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                some(operand(LogicalAggregate.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER, "JoinAggToJoinAggSemiJoinRule");

    public JoinAggToJoinAggSemiJoinRule(
        RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        if (!PlannerContext.getPlannerContext(call).getParamManager().getBoolean(
            ConnectionParams.ENABLE_JOINAGG_TO_JOINAGGSEMIJOIN)) {
            return false;
        }
        if (join.getJoinType() != JoinRelType.INNER) {
            return false;
        }
        if (join.getJoinReorderContext().isHasSemiFilter()) {
            return false;
        }

        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        final LogicalAggregate agg = call.rel(1);
        LogicalJoin output = transform(join, agg, call.builder());
        if (output != null) {
            call.transformTo(output);
        }
    }

    protected LogicalJoin transform(final LogicalJoin join, final LogicalAggregate agg,
                                    RelBuilder relBuilder) {
        final List<Integer> leftKeys = Lists.newArrayList();
        final List<Integer> rightKeys = Lists.newArrayList();
        final List<Boolean> filterNulls = Lists.newArrayList();
        RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys,
            filterNulls);

        // agg column is the same as the equal join column
        Set<Integer> leftKeySet = Sets.newHashSet(leftKeys);
        int groupByColumnNum = agg.getGroupSet().cardinality();
        for (int key : leftKeySet) {
            if (key >= groupByColumnNum) {
                return null;
            }
        }
        if (groupByColumnNum != leftKeySet.size()) {
            return null;
        }

        List<Integer> groupBy = agg.getGroupSet().toList();
        // build semi join

        List<RexNode> semiConditions = Lists.newArrayList();
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        for (int i = 0; i < leftKeys.size(); i++) {
            Integer leftKey = groupBy.get(leftKeys.get(i));
            Integer rightKey = rightKeys.get(i);
            RexNode leftRex = rexBuilder.makeInputRef(agg.getInput().getRowType().getFieldList().get(leftKey).getType(),
                leftKey);
            RexNode rightRex =
                rexBuilder.makeInputRef(join.getRight().getRowType().getFieldList().get(rightKey).getType(),
                    rightKey + agg.getInput().getRowType().getFieldCount());
            semiConditions.add(rexBuilder.makeCall(filterNulls.get(i) ?
                    SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                leftRex, rightRex));
        }

        relBuilder.push(agg.getInput());
        relBuilder.push(join.getRight());
        LogicalSemiJoin semiJoin = (LogicalSemiJoin) relBuilder.logicalSemiJoin(semiConditions,
            SqlStdOperatorTable.EQUALS,
            JoinRelType.SEMI,
            null,
            ImmutableSet.of(),
            new SqlNodeList(SqlParserPos.ZERO),
            "filter").build();
        semiJoin.getJoinReorderContext().setHasSemiFilter(true);
        // build new agg
        LogicalAggregate newAgg = agg.copy(semiJoin, agg.getGroupSet(), agg.getAggCallList());
        // build new join

        LogicalJoin newJoin = join.copy(
            join.getTraitSet(),
            join.getCondition(),
            newAgg,
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());
        newJoin.getJoinReorderContext().setHasSemiFilter(true);

        return newJoin;
    }

}
