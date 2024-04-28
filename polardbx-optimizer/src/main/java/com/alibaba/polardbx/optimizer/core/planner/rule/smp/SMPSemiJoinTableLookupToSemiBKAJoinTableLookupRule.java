package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.JoinTableLookupTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.SemiJoinTableLookupToSemiBKAJoinTableLookupRule;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rex.RexNode;

public class SMPSemiJoinTableLookupToSemiBKAJoinTableLookupRule
    extends SemiJoinTableLookupToSemiBKAJoinTableLookupRule {

    public static final SemiJoinTableLookupToSemiBKAJoinTableLookupRule
        INSTANCE = new SMPSemiJoinTableLookupToSemiBKAJoinTableLookupRule(
        operand(LogicalSemiJoin.class,
            operand(RelSubset.class, any()),
            operand(LogicalTableLookup.class, null,
                JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                operand(LogicalIndexScan.class, none()))), "INSTANCE");

    SMPSemiJoinTableLookupToSemiBKAJoinTableLookupRule(RelOptRuleOperand operand, String desc) {
        super(operand, "SMP_" + desc);
    }

    @Override
    protected void createSemiBKAJoin(
        RelOptRuleCall call,
        LogicalSemiJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        LogicalIndexScan newLogicalIndexScan) {
        SemiBKAJoin bkaJoin = SemiBKAJoin.create(
            join.getTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints(), join);
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_SEMI_BKA_JOIN);
        if (fixedCost != null) {
            bkaJoin.setFixedCost(fixedCost);
        }
        newLogicalIndexScan.setIsMGetEnabled(true);
        newLogicalIndexScan.setJoin(bkaJoin);
        RelUtils.changeRowType(bkaJoin, join.getRowType());
        call.transformTo(bkaJoin);
    }
}
