package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexCall;

/**
 * @author shengyu
 */
public class CBOPushSemiJoinDirectRule extends PushSemiJoinDirectRule {

    public CBOPushSemiJoinDirectRule(RelOptRuleOperand operand, String description) {
        super(operand, "CBOPushSemiJoinDirectRule:" + description);
    }

    public static final CBOPushSemiJoinDirectRule INSTANCE = new CBOPushSemiJoinDirectRule(
        operand(LogicalSemiJoin.class, some(operand(LogicalView.class, none()), operand(LogicalView.class, none())
        )), "INSTANCE");

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_CBO_PUSH_JOIN)
            && PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP);
    }

    @Override
    protected boolean prune(RelOptRuleCall call) {
        if (PlannerContext.getPlannerContext(call).getRestrictCboPushJoin()) {
            final LogicalView leftView = (LogicalView) call.rels[1];
            final LogicalView rightView = (LogicalView) call.rels[2];
            if (leftView.getSchemaName().equalsIgnoreCase(rightView.getSchemaName())) {
                Multiset<String> tables = TreeMultiset.create();
                tables.add(leftView.getSchemaName());
                tables.addAll(leftView.getTableNames());
                tables.addAll(rightView.getTableNames());
                StringBuilder sb = new StringBuilder();
                for (String table : tables) {
                    sb.append(table).append(" ");
                }
                if (!PlannerContext.getPlannerContext(call).addTableList(sb.toString())) {
                    return true;
                }
            }
        }
        return false;
    }
}