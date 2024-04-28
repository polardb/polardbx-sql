package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public abstract class LogicalSemiJoinToSemiNLJoinRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalSemiJoinToSemiNLJoinRule(String desc) {
        super(operand(LogicalSemiJoin.class, Convention.NONE, any()), "LogicalSemiJoinToSemiNLJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SEMI_NL_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin semiJoin = call.rel(0);
        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(semiJoin.getCondition(), semiJoin.getCluster().getRexBuilder());

        if (pruneSemiNLJoin(semiJoin, newCondition)) {
            return;
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(semiJoin)) {
            leftTraitSet = semiJoin.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
            rightTraitSet = semiJoin.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        } else {
            leftTraitSet = semiJoin.getLeft().getTraitSet().replace(outConvention);
            rightTraitSet = semiJoin.getRight().getTraitSet().replace(outConvention);
        }

        final RelNode left = convert(semiJoin.getLeft(), leftTraitSet);
        final RelNode right = convert(semiJoin.getRight(), rightTraitSet);

        createSemiNLJoin(
            call,
            semiJoin,
            left,
            right,
            newCondition);
    }

    protected boolean pruneSemiNLJoin(LogicalSemiJoin join, RexNode newCondition) {
        return false;
    }

    protected boolean canUseSemiHash(LogicalSemiJoin semiJoin, RexNode newCondition) {
        if (!PlannerContext.getPlannerContext(semiJoin).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SEMI_HASH_JOIN)) {
            return false;
        }
        if (!CBOUtil.checkHashJoinCondition(semiJoin,
            newCondition,
            semiJoin.getLeft().getRowType().getFieldCount(),
            new CBOUtil.RexNodeHolder(),
            new CBOUtil.RexNodeHolder())) {
            return false;
        }

        if (semiJoin.getJoinType() == JoinRelType.ANTI
            && semiJoin.getOperands() != null && !semiJoin.getOperands().isEmpty()) {
            // If this node is an Anti-Join without operands ('NOT IN')
            if (!newCondition.isA(SqlKind.EQUALS) && !newCondition.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
                // ... and contains multiple equi-conditions
                return false; // reject!
            }
        }

        // with semi nl join hint, ignore semi hash
        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_NL_JOIN);
        if (fixedCost != null && fixedCost.equals(semiJoin.getCluster().getPlanner().getCostFactory().makeTinyCost())) {
            return false;
        }
        return true;
    }

    protected abstract void createSemiNLJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        RelNode left,
        RelNode right,
        RexNode newCondition);
}
