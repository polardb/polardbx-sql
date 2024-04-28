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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

public abstract class LogicalJoinToNLJoinRule extends RelOptRule {

    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalJoinToNLJoinRule(String desc) {
        super(operand(LogicalJoin.class, Convention.NONE, any()), "LogicalJoinToNLJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_NL_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

        final LogicalJoin join = call.rel(0);

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        if (pruneNLJoin(join, newCondition)) {
            return;
        }
        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
            rightTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(outConvention);
            rightTraitSet = join.getRight().getTraitSet().replace(outConvention);
        }
        final RelNode left;
        final RelNode right;
        left = convert(join.getLeft(), leftTraitSet);
        right = convert(join.getRight(), rightTraitSet);

        createNLJoin(
            call,
            join,
            left,
            right,
            newCondition);
    }

    protected boolean pruneNLJoin(Join join, RexNode newCondition) {
        return false;
    }

    public static boolean canUseHash(Join join, RexNode newCondition) {
        if (!PlannerContext.getPlannerContext(join).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_HASH_JOIN)) {
            return false;
        }
        if (!CBOUtil.checkHashJoinCondition(join,
            newCondition,
            join.getLeft().getRowType().getFieldCount(),
            new CBOUtil.RexNodeHolder(),
            new CBOUtil.RexNodeHolder())) {
            return false;
        }

        // with semi nl join hint, ignore semi hash
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_NL_JOIN);
        if (fixedCost != null && fixedCost.equals(join.getCluster().getPlanner().getCostFactory().makeTinyCost())) {
            return false;
        }
        return true;
    }

    protected abstract void createNLJoin(
        RelOptRuleCall call,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition);
}