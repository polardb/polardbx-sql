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
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * @author lingce.ldm
 */
public class PushSemiJoinRule extends PushJoinRule {

    public PushSemiJoinRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushSemiJoinRule:" + description);
    }

    public static final PushSemiJoinRule INSTANCE = new PushSemiJoinRule(
        operand(LogicalSemiJoin.class, some(operand(LogicalView.class, none()), operand(LogicalView.class, none()))),
        "INSTANCE");

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalView leftView = (LogicalView)call.rels[1];
        final LogicalView rightView = (LogicalView)call.rels[2];
        if (leftView instanceof OSSTableScan || rightView instanceof OSSTableScan) {
            return false;
        }
        return PlannerContext.getPlannerContext(call).getParamManager().getBoolean(ConnectionParams.ENABLE_PUSH_JOIN)
            && !PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin join = (LogicalSemiJoin) call.rels[0];
        final LogicalView leftView = (LogicalView) call.rels[1];
        final LogicalView rightView = (LogicalView) call.rels[2];
        RexNode joinCondition = join.getCondition();
        if (join.getOperator() == null || joinCondition.isAlwaysTrue() || joinCondition.isAlwaysFalse()) {
            return;
        }

        tryPushJoin(call, join, leftView, rightView, joinCondition);
    }

    @Override
    protected void perform(RelOptRuleCall call, List<RexNode> leftFilters,
                           List<RexNode> rightFilters, RelOptPredicateList relOptPredicateList) {
        final LogicalSemiJoin logicalSemiJoin = (LogicalSemiJoin) call.rels[0];
        final LogicalView leftView = (LogicalView) call.rels[1];
        final LogicalView rightView = (LogicalView) call.rels[2];

        LogicalSemiJoin newLogicalSemiJoin = logicalSemiJoin.copy(
            logicalSemiJoin.getTraitSet(),
            logicalSemiJoin.getCondition(),
            logicalSemiJoin.getLeft(),
            logicalSemiJoin.getRight(),
            logicalSemiJoin.getJoinType(),
            logicalSemiJoin.isSemiJoinDone());

        LogicalView newLeftView = leftView.copy(leftView.getTraitSet());
        LogicalView newRightView = rightView.copy(rightView.getTraitSet());

        newLeftView.pushSemiJoin(
            newLogicalSemiJoin,
            newRightView,
            leftFilters,
            rightFilters,
            newLogicalSemiJoin.getPushDownRelNode(newRightView.getPushedRelNode(),
                call.builder(),
                call.builder().getRexBuilder(),
                leftFilters,
                rightFilters,
                false));

        RelUtils.changeRowType(newLeftView, logicalSemiJoin.getRowType());
        call.transformTo(newLeftView);
    }
}
