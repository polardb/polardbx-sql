package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public abstract class LogicalWindowToSortWindowRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalWindowToSortWindowRule(String desc) {
        super(operand(LogicalWindow.class, some(operand(RelSubset.class, any()))),
            "LogicalWindowToSortWindowRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final RelNode rel = call.rel(0);
        if (!PlannerContext.getPlannerContext(rel).getParamManager().getBoolean(ConnectionParams.ENABLE_SORT_WINDOW)) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalWindow window = (LogicalWindow) call.rels[0];
        RelNode input = call.rels[1];
        ImmutableBitSet groupSets = window.groups.get(0).keys;
        List<Integer> sortFields = groupSets.toList();
        List<RelFieldCollation> orderKeys = window.groups.get(0).orderKeys.getFieldCollations();

        RelCollation relCollation = RelCollations.EMPTY;
        if (groupSets.cardinality() + orderKeys.size() > 0) {
            relCollation = CBOUtil.createRelCollation(sortFields, orderKeys);
        }
        RelNode newInput = convert(input, input.getTraitSet().replace(outConvention));

        createSortWindow(call, window, newInput, relCollation);
    }

    protected abstract void createSortWindow(
        RelOptRuleCall call,
        LogicalWindow window,
        RelNode newInput,
        RelCollation relCollation);
}
