package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiNLJoinRule;
import com.alibaba.polardbx.optimizer.core.rel.SemiNLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalSemiJoinToSemiNLJoinRule extends LogicalSemiJoinToSemiNLJoinRule {

    public static final LogicalSemiJoinToSemiNLJoinRule INSTANCE =
        new SMPLogicalSemiJoinToSemiNLJoinRule("INSTANCE");

    SMPLogicalSemiJoinToSemiNLJoinRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createSemiNLJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        RelNode left,
        RelNode right,
        RexNode newCondition) {
        SemiNLJoin semiNLJoin = SemiNLJoin.create(
            semiJoin.getTraitSet().replace(outConvention), left, right, newCondition, semiJoin);
        semiNLJoin.setAntiCondition(semiJoin.getAntiCondition());
        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_NL_JOIN);
        if (fixedCost != null) {
            semiNLJoin.setFixedCost(fixedCost);
        }
        call.transformTo(semiNLJoin);
    }
}
