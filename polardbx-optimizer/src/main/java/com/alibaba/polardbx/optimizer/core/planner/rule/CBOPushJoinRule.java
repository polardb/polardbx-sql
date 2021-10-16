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
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class CBOPushJoinRule extends PushJoinRule {
    public CBOPushJoinRule(RelOptRuleOperand operand, String description) {
        super(operand, "CBOPushJoinRule:" + description);
    }

    public static final CBOPushJoinRule INSTANCE = new CBOPushJoinRule(
        operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
            some(
                operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, none()),
                operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, none()))),
        "INSTANCE");

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_CBO_PUSH_JOIN);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalJoin join = (LogicalJoin) call.rels[0];
        final LogicalView leftView = (LogicalView) call.rels[1];
        final LogicalView rightView = (LogicalView) call.rels[2];

        RexNode joinCondition = join.getCondition();
        if (joinCondition.isAlwaysTrue() || joinCondition.isAlwaysFalse()) {
            return;
        }
        tryPushJoin(call, join, leftView, rightView, joinCondition);
    }

    @Override
    protected void perform(RelOptRuleCall call, List<RexNode> leftFilters,
                           List<RexNode> rightFilters, RelOptPredicateList relOptPredicateList) {
        final LogicalJoin join = (LogicalJoin) call.rels[0];
        LogicalView leftView = (LogicalView) call.rels[1];
        final LogicalView rightView = (LogicalView) call.rels[2];

        if (rightView instanceof LogicalIndexScan && !(leftView instanceof LogicalIndexScan)) {
            leftView = new LogicalIndexScan(leftView);
        }

        LogicalView newLeftView = leftView.copy(leftView.getTraitSet());
        LogicalView newRightView = rightView.copy(rightView.getTraitSet());

        LogicalJoin newLogicalJoin = join.copy(
            join.getTraitSet(),
            join.getCondition(),
            newLeftView,
            newRightView,
            join.getJoinType(),
            join.isSemiJoinDone());

        newLeftView.pushJoin(newLogicalJoin, newRightView, leftFilters, rightFilters);
        RelUtils.changeRowType(newLeftView, join.getRowType());
        call.transformTo(convert(newLeftView, join.getTraitSet()));
    }

}
