package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public abstract class LogicalViewConvertRule extends RelOptRule {

    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalViewConvertRule(String desc) {
        super(operand(LogicalView.class, Convention.NONE, any()), "DrdsLogicalViewConvertRule:" + desc);
    }

    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        createLogicalview(call, call.rel(0));
    }

    protected abstract void createLogicalview(RelOptRuleCall call, LogicalView logicalView);
}
