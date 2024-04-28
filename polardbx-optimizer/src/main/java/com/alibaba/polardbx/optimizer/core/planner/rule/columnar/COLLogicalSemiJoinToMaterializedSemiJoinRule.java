package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToMaterializedSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

public class COLLogicalSemiJoinToMaterializedSemiJoinRule extends LogicalSemiJoinToMaterializedSemiJoinRule {

    public static final LogicalSemiJoinToMaterializedSemiJoinRule INSTANCE =
        new COLLogicalSemiJoinToMaterializedSemiJoinRule(
            operand(LogicalSemiJoin.class,
                operand(LogicalView.class, any()),
                operand(RelSubset.class, any())), "INSTANCE");

    COLLogicalSemiJoinToMaterializedSemiJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    protected void createMaterializedSemiJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        LogicalView left,
        RelNode right,
        RexNode newCondition,
        boolean distinctInput) {

        MaterializedSemiJoin materializedSemiJoin = MaterializedSemiJoin.create(
            semiJoin.getTraitSet().replace(outConvention),
            left,
            convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED)),
            newCondition,
            semiJoin,
            distinctInput);

        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_MATERIALIZED_SEMI_JOIN);
        if (fixedCost != null) {
            materializedSemiJoin.setFixedCost(fixedCost);
        }
        left.setIsMGetEnabled(true);
        left.setJoin(materializedSemiJoin);
        call.transformTo(materializedSemiJoin);
    }
}
