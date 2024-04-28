package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalSort;

public abstract class LogicalSortToSortRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected final boolean isTopNRule;

    protected LogicalSortToSortRule(boolean isTopNRule, String desc) {
        super(operand(LogicalSort.class, Convention.NONE, any()), "DrdsSortConvertRule:" + desc);
        this.isTopNRule = isTopNRule;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    public void onMatch(RelOptRuleCall call) {
        final LogicalSort sort = call.rel(0);
        createSort(call, sort);
    }

    protected abstract void createSort(RelOptRuleCall call, LogicalSort sort);
}

