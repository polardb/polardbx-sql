package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.AggJoinToToHashGroupJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

public class SMPAggJoinToToHashGroupJoinRule extends AggJoinToToHashGroupJoinRule {

    public static final AggJoinToToHashGroupJoinRule INSTANCE = new SMPAggJoinToToHashGroupJoinRule("INSTANCE");

    public SMPAggJoinToToHashGroupJoinRule(String desc) {
        super("SMP_" + desc);
    }

    protected void createHashGroupJoin(
        RelOptRuleCall call,
        LogicalAggregate agg,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        CBOUtil.RexNodeHolder equalConditionHolder,
        CBOUtil.RexNodeHolder otherConditionHolder) {
        HashGroupJoin newAgg = HashGroupJoin.create(
            join.getCluster().getPlanner().emptyTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            (ImmutableList<RelDataTypeField>) join.getSystemFieldList(),
            join.getHints(),
            equalConditionHolder.getRexNode(),
            otherConditionHolder.getRexNode(),
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());

        RelOptCost fixedCost = CheckJoinHint.check(newAgg, HintType.CMD_HASH_GROUP_JOIN);
        if (fixedCost != null) {
            newAgg.setFixedCost(fixedCost);
        }

        call.transformTo(newAgg);
    }
}
