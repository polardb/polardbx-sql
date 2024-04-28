package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

public abstract class LogicalAggToHashAggRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalAggToHashAggRule(String desc) {
        super(operand(LogicalAggregate.class, Convention.NONE, any()), "LogicalAggToHashAggRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_HASH_AGG);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        RelNode input = agg.getInput();
        RelTraitSet inputTraitSet = agg.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        RelNode newInput = convert(input, inputTraitSet);

        createHashAgg(call, agg, newInput);
    }

    protected abstract void createHashAgg(
        RelOptRuleCall call,
        LogicalAggregate agg,
        RelNode newInput);
}
