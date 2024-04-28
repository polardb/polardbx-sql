package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

public class ConstantFoldRule extends RelOptRule {

    public ConstantFoldRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final ConstantFoldRule INSTANCE = new ConstantFoldRule(
        operand(Filter.class, RelOptRule.any()), "constant_fold");

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(call.rels[0]).getParamManager();
        return paramManager.getBoolean(ConnectionParams.ENABLE_CONSTANT_FOLD);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = (Filter) call.rels[0];
        RelNode input = filter.getInput();

        RexBuilder rb = call.builder().getRexBuilder();
        RexConstantFoldShuttle rexConstantFoldShuttle =
            new RexConstantFoldShuttle(rb, PlannerContext.getPlannerContext(filter).getExecutionContext());
        RexNode condition = filter.getCondition().accept(rexConstantFoldShuttle);
        if (condition == filter.getCondition()) {
            return;
        }
        if (filter instanceof LogicalFilter) {
            call.transformTo(LogicalFilter.create(input,
                condition,
                ImmutableSet.<CorrelationId>builder().addAll(filter.getVariablesSet()).build()));
        } else if (filter instanceof PhysicalFilter) {
            call.transformTo(PhysicalFilter.create(input,
                condition,
                ImmutableSet.<CorrelationId>builder().addAll(filter.getVariablesSet()).build()));
        }
    }

    class RexConstantFoldShuttle extends RexShuttle {
        RexBuilder rexBuilder;

        ExecutionContext ec;

        RexConstantFoldShuttle(RexBuilder rexBuilder, ExecutionContext ec) {
            this.rexBuilder = rexBuilder;
            this.ec = ec;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            if (RexUtil.isConstant(call)) {
                Object obj = RexUtils.getEvalFuncExec(call, new ExprContextProvider(ec)).eval(null);
                if (obj instanceof RowValue) {
                    return super.visitCall(call);
                }

                try {
                    switch (call.getType().getSqlTypeName()) {
                    case TIME:
                    case DATE:
                    case TIMESTAMP:
                    case DATETIME:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    case DECIMAL:
                        return rexBuilder.makeLiteral(obj.toString());
                    default:
                        return rexBuilder.makeLiteral(obj, call.getType(), true);
                    }
                } catch (AssertionError e) {
                    return super.visitCall(call);
                }
            }
            return super.visitCall(call);
        }
    }
}
