package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToBKAJoinRule;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalJoinToBKAJoinRule extends LogicalJoinToBKAJoinRule {

    public static final LogicalJoinToBKAJoinRule LOGICALVIEW_NOT_RIGHT = new SMPLogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_NOT_RIGHT,
            operand(RelSubset.class, any()),
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
        "LOGICALVIEW:NOT_RIGHT");

    public static final LogicalJoinToBKAJoinRule LOGICALVIEW_RIGHT = new SMPLogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_RIGHT,
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(RelSubset.class, any())), "LOGICALVIEW:RIGHT");

    SMPLogicalJoinToBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "SMP_" + desc);
    }

    @Override
    protected void createBKAJoin(
        RelOptRuleCall call,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        LogicalView inner,
        RexNode newCondition) {
        BKAJoin bkaJoin = BKAJoin.create(
            join.getTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints()); // PK set in inner table for runtime optimize.
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_BKA_JOIN);
        if (fixedCost != null) {
            bkaJoin.setFixedCost(fixedCost);
        }
        inner.setIsMGetEnabled(true);
        inner.setJoin(bkaJoin);
        call.transformTo(bkaJoin);
    }
}
