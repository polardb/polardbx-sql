package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class GenXplanRule extends RelOptRule {

    public static final GenXplanRule INSTANCE = new GenXplanRule();

    protected GenXplanRule() {
        super(operand(LogicalView.class, RelOptRule.none()),
            "GenXplanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getBoolean(ConnectionParams.CONN_POOL_XPROTO_XPLAN);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalView logicalView = call.rel(0);
        logicalView.getXPlan();
    }
}