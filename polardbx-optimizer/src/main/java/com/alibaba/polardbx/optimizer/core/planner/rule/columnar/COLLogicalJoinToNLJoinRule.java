package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class COLLogicalJoinToNLJoinRule extends LogicalJoinToNLJoinRule {
    public static final LogicalJoinToNLJoinRule INSTANCE = new COLLogicalJoinToNLJoinRule("INSTANCE");

    COLLogicalJoinToNLJoinRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createNLJoin(RelOptRuleCall call,
                                LogicalJoin join,
                                RelNode left,
                                RelNode right,
                                RexNode newCondition) {
        // prefer hash join rather than nl join
        if (canUseHash(join, newCondition)) {
            return;
        }
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = Lists.newArrayList();
        CBOUtil.columnarBroadcastDistribution(
            join,
            left,
            right,
            implementationList);

        // default single join
        RelNode singletonLeft = convert(left, left.getTraitSet().replace(RelDistributions.SINGLETON));
        RelNode singletonRight = convert(right, right.getTraitSet().replace(RelDistributions.SINGLETON));
        implementationList.add(Pair.of(RelDistributions.SINGLETON, Pair.of(singletonLeft, singletonRight)));

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            NLJoin nlJoin = NLJoin.create(
                join.getTraitSet().replace(outConvention).replace(implementation.getKey()),
                implementation.getValue().getKey(),
                implementation.getValue().getValue(),
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

            if (join.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(
                    convert(nlJoin, nlJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(nlJoin);
            }
        }
    }
}
