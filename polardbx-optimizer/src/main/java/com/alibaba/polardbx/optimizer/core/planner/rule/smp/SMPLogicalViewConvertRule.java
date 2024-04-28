package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRuleCall;

public class SMPLogicalViewConvertRule extends LogicalViewConvertRule {
    public static final LogicalViewConvertRule INSTANCE = new SMPLogicalViewConvertRule("INSTANCE");

    SMPLogicalViewConvertRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createLogicalview(RelOptRuleCall call, LogicalView logicalView) {
        LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().simplify().replace(outConvention));
        newLogicalView.optimize();
        call.transformTo(newLogicalView);
        // do nothing
    }
}
