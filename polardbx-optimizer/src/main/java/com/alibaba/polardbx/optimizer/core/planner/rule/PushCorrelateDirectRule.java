package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;

/**
 * @author shengyu
 */
public class PushCorrelateDirectRule extends PushCorrelateRule {
    public PushCorrelateDirectRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushCorrelateDirectRule:" + description);
    }

    public static final PushCorrelateDirectRule INSTANCE = new PushCorrelateDirectRule(
        operand(Correlate.class, some(operand(LogicalView.class, none()), operand(RelNode.class, any()))),
        "INSTANCE");

    /**
     * used to unwrap subquery in LogicalView
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        // return !PlannerContext.getPlannerContext(call).getParamManager().getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP);
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // TODO: implement this part
        return;
    }
}
