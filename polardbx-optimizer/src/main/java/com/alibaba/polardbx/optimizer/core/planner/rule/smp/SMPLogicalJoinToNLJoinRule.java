package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalJoinToNLJoinRule extends LogicalJoinToNLJoinRule {
    public static final LogicalJoinToNLJoinRule INSTANCE = new SMPLogicalJoinToNLJoinRule("INSTANCE");

    SMPLogicalJoinToNLJoinRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createNLJoin(RelOptRuleCall call,
                                LogicalJoin join,
                                RelNode left,
                                RelNode right,
                                RexNode newCondition) {
        NLJoin nlJoin = NLJoin.create(
            join.getTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints());
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_NL_JOIN);
        if (fixedCost != null) {
            nlJoin.setFixedCost(fixedCost);
        }
        call.transformTo(nlJoin);
    }
}
