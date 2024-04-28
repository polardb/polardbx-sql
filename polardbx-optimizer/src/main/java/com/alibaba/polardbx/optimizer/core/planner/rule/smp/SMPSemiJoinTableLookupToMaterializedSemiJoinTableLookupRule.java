package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.JoinTableLookupTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.SemiJoinTableLookupToMaterializedSemiJoinTableLookupRule;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rex.RexNode;

public class SMPSemiJoinTableLookupToMaterializedSemiJoinTableLookupRule
    extends SemiJoinTableLookupToMaterializedSemiJoinTableLookupRule {

    public static final SemiJoinTableLookupToMaterializedSemiJoinTableLookupRule INSTANCE =
        new SMPSemiJoinTableLookupToMaterializedSemiJoinTableLookupRule(
            operand(LogicalSemiJoin.class,
                operand(LogicalTableLookup.class, null,
                    JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                    operand(LogicalIndexScan.class, none())),
                operand(RelSubset.class, any())), "INSTANCE");

    SMPSemiJoinTableLookupToMaterializedSemiJoinTableLookupRule(RelOptRuleOperand operand, String desc) {
        super(operand, "SMP_" + desc);
    }

    protected void createMaterializedSemiJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        LogicalTableLookup left,
        RelNode right,
        RexNode newCondition,
        LogicalIndexScan logicalIndexScan,
        boolean distinctInput) {
        MaterializedSemiJoin materializedSemiJoin = MaterializedSemiJoin.create(
            semiJoin.getTraitSet().replace(outConvention), left, right, newCondition, semiJoin,
            distinctInput);

        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_MATERIALIZED_SEMI_JOIN);
        if (fixedCost != null) {
            materializedSemiJoin.setFixedCost(fixedCost);
        }
        logicalIndexScan.setIsMGetEnabled(true);
        logicalIndexScan.setJoin(materializedSemiJoin);
        call.transformTo(materializedSemiJoin);
    }
}
