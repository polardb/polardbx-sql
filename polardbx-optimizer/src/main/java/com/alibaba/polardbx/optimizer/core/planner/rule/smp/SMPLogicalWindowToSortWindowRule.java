package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalWindowToSortWindowRule;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;

public class SMPLogicalWindowToSortWindowRule extends LogicalWindowToSortWindowRule {

    public static final LogicalWindowToSortWindowRule INSTANCE = new SMPLogicalWindowToSortWindowRule("INSTANCE");

    public SMPLogicalWindowToSortWindowRule(String desc) {
        super("SMP_" + desc);
    }

    protected void createSortWindow(
        RelOptRuleCall call,
        LogicalWindow window,
        RelNode newInput,
        RelCollation relCollation) {
        if (relCollation != RelCollations.EMPTY) {
            LogicalSort sort =
                LogicalSort.create(newInput.getCluster().getPlanner().emptyTraitSet().replace(relCollation), newInput,
                    relCollation, null, null);
            newInput = convert(sort, sort.getTraitSet().replace(outConvention));
        }

        SortWindow newWindow =
            SortWindow.create(
                window.getTraitSet().replace(outConvention).replace(relCollation),
                newInput,
                window.getConstants(),
                window.groups,
                window.getRowType());
        call.transformTo(newWindow);
    }
}
