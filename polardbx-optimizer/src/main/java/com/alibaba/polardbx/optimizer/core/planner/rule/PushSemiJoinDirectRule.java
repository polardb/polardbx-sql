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
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * pushes semi join to left LogicalView directly.
 * differs from {@link PushSemiJoinRule}, which transforms semi join to subquery then pushes the subquery
 *
 * @author shengyu
 */
public class PushSemiJoinDirectRule extends PushSemiJoinRule {
    public PushSemiJoinDirectRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushSemiJoinDirectRule:" + description);
    }

    public static final PushSemiJoinDirectRule INSTANCE = new PushSemiJoinDirectRule(
        operand(LogicalSemiJoin.class, some(operand(LogicalView.class, none()), operand(LogicalView.class, none()))),
        "INSTANCE");

    /**
     * used to unwrap subquery in LogicalView
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalView leftView = (LogicalView)call.rels[1];
        final LogicalView rightView = (LogicalView)call.rels[2];
        if (leftView instanceof OSSTableScan || rightView instanceof OSSTableScan) {
            return false;
        }
        return PlannerContext.getPlannerContext(call).getParamManager().getBoolean(ConnectionParams.ENABLE_PUSH_JOIN)
            && PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP);
    }

    protected boolean prune(RelOptRuleCall call) {
        return false;
    }

    @Override
    protected void perform(RelOptRuleCall call, List<RexNode> leftFilters,
                           List<RexNode> rightFilters, RelOptPredicateList relOptPredicateList) {

        if (prune(call)) {
            return;
        }
        final LogicalSemiJoin logicalSemiJoin = (LogicalSemiJoin) call.rels[0];
        LogicalSemiJoin newLogicalSemiJoin = logicalSemiJoin.copy(
            logicalSemiJoin.getTraitSet(),
            logicalSemiJoin.getCondition(),
            logicalSemiJoin.getLeft(),
            logicalSemiJoin.getRight(),
            logicalSemiJoin.getJoinType(),
            logicalSemiJoin.isSemiJoinDone());

        final LogicalView leftView = (LogicalView) call.rels[1];
        final LogicalView rightView = (LogicalView) call.rels[2];
        LogicalView newLeftView = leftView.copy(leftView.getTraitSet());
        LogicalView newRightView = rightView.copy(rightView.getTraitSet());

        newLeftView.pushSemiJoinDirect(
            newLogicalSemiJoin,
            newRightView,
            leftFilters,
            rightFilters);

        RelUtils.changeRowType(newLeftView, logicalSemiJoin.getRowType());
        call.transformTo(newLeftView);
    }
}
