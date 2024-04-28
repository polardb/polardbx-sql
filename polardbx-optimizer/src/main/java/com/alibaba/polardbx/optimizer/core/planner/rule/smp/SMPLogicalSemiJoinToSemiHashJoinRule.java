package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalSemiJoinToSemiHashJoinRule extends LogicalSemiJoinToSemiHashJoinRule {

    public static final LogicalSemiJoinToSemiHashJoinRule INSTANCE =
        new SMPLogicalSemiJoinToSemiHashJoinRule("INSTANCE");

    SMPLogicalSemiJoinToSemiHashJoinRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createSemiHashJoin(RelOptRuleCall call,
                                      LogicalSemiJoin semiJoin,
                                      RelNode left,
                                      RelNode right,
                                      RexNode newCondition,
                                      CBOUtil.RexNodeHolder equalConditionHolder,
                                      CBOUtil.RexNodeHolder otherConditionHolder) {
        switch (semiJoin.getJoinType()) {
        case SEMI:
        case ANTI:
        case LEFT:
        case INNER:
            SemiHashJoin semiHashJoin = SemiHashJoin.create(
                semiJoin.getTraitSet().replace(outConvention),
                left,
                right,
                newCondition,
                semiJoin,
                equalConditionHolder.getRexNode(),
                otherConditionHolder.getRexNode(),
                outDriver);
            RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_HASH_JOIN);
            if (fixedCost != null) {
                semiHashJoin.setFixedCost(fixedCost);
            }
            call.transformTo(semiHashJoin);

        default:
            // not support
        }
    }
}