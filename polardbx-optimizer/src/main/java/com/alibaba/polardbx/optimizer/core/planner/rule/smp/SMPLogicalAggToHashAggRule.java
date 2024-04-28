package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalAggToHashAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class SMPLogicalAggToHashAggRule extends LogicalAggToHashAggRule {

    public static final LogicalAggToHashAggRule INSTANCE = new SMPLogicalAggToHashAggRule("INSTANCE");

    public SMPLogicalAggToHashAggRule(String desc) {
        super("SMP_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createHashAgg(RelOptRuleCall call, LogicalAggregate agg, RelNode newInput) {
        HashAgg hashAgg = HashAgg.create(
            agg.getTraitSet().replace(outConvention),
            newInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());

        call.transformTo(hashAgg);
    }
}
