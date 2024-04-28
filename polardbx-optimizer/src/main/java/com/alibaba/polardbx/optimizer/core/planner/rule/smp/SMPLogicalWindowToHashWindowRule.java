package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalWindowToHashWindowRule;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;

public class SMPLogicalWindowToHashWindowRule extends LogicalWindowToHashWindowRule {

    public static final LogicalWindowToHashWindowRule INSTANCE = new SMPLogicalWindowToHashWindowRule("INSTANCE");

    public SMPLogicalWindowToHashWindowRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    public void createHashWindow(
        RelOptRuleCall call,
        LogicalWindow window,
        RelNode newInput) {

        HashWindow newWindow =
            HashWindow.create(
                window.getTraitSet().replace(outConvention).replace(RelCollations.EMPTY),
                newInput,
                window.getConstants(),
                window.groups,
                window.getRowType());
        call.transformTo(newWindow);
    }
}
