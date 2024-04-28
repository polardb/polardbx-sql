package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public abstract class LogicalSemiJoinToSemiHashJoinRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected boolean outDriver = false;

    protected LogicalSemiJoinToSemiHashJoinRule(String desc) {
        super(operand(LogicalSemiJoin.class, Convention.NONE, any()), "LogicalSemiJoinToSemiHashJoinRule:" + desc);
    }

    protected LogicalSemiJoinToSemiHashJoinRule(boolean outDriver, String desc) {
        super(operand(LogicalSemiJoin.class, Convention.NONE, any()), "LogicalSemiJoinToSemiHashJoinRule:" + desc);
        this.outDriver = outDriver;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SEMI_HASH_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalSemiJoin join = call.rel(0);
        PlannerContext plannerContext = PlannerContext.getPlannerContext(call);
        if (outDriver) {
            JoinRelType joinType = join.getJoinType();
            if (joinType != JoinRelType.SEMI && joinType != JoinRelType.ANTI) {
                return false;
            }
            // if semi join and not enable reverse semi hash join, just return
            if (joinType == JoinRelType.SEMI && !plannerContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_REVERSE_SEMI_HASH_JOIN)) {
                return false;
            }
            // if anti join and not enable reverse anti hash join, just return
            if (joinType == JoinRelType.ANTI && !plannerContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_REVERSE_ANTI_HASH_JOIN)) {
                return false;
            }
        }
        return enable(plannerContext);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        final LogicalSemiJoin semiJoin = (LogicalSemiJoin) rel;

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(semiJoin.getCondition(), semiJoin.getCluster().getRexBuilder());

        CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();
        if (!CBOUtil.checkHashJoinCondition(semiJoin,
            newCondition,
            semiJoin.getLeft().getRowType().getFieldCount(),
            equalConditionHolder,
            otherConditionHolder)) {
            return;
        }

        if (semiJoin.getJoinType() == JoinRelType.ANTI
            && semiJoin.getOperands() != null && !semiJoin.getOperands().isEmpty()) {
            // If this node is an Anti-Join without operands ('NOT IN')
            if (!newCondition.isA(SqlKind.EQUALS) && !newCondition.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
                // ... and contains multiple equi-conditions
                return; // reject!
            }
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(semiJoin)) {
            leftTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
            rightTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        } else {
            if (outDriver) {
                return;
            }
            leftTraitSet = semiJoin.getLeft().getTraitSet().replace(outConvention);
            rightTraitSet = semiJoin.getRight().getTraitSet().replace(outConvention);
        }

        final RelNode left = convert(semiJoin.getLeft(), leftTraitSet);
        final RelNode right = convert(semiJoin.getRight(), rightTraitSet);

        createSemiHashJoin(
            call,
            semiJoin,
            left,
            right,
            newCondition,
            equalConditionHolder,
            otherConditionHolder);
    }

    protected abstract void createSemiHashJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        CBOUtil.RexNodeHolder equalConditionHolder,
        CBOUtil.RexNodeHolder otherConditionHolder);
}



