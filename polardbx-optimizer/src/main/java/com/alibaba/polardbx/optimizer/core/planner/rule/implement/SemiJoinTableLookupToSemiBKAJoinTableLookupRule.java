package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * Avoid semi join produces too much data on the right LV
 *
 * @author hongxi.chx
 */
public abstract class SemiJoinTableLookupToSemiBKAJoinTableLookupRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected SemiJoinTableLookupToSemiBKAJoinTableLookupRule(RelOptRuleOperand operand, String desc) {
        super(operand, "SemiJoinTableLookupToSemiBKAJoinTableLookupRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    public static boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SEMI_BKA_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final RelNode rel = call.rel(0);
        if (!enable(PlannerContext.getPlannerContext(rel))) {
            return false;
        }

        if (rel instanceof SemiJoin) {
            final JoinRelType joinType = ((SemiJoin) rel).getJoinType();
            if (joinType == JoinRelType.SEMI || joinType == JoinRelType.LEFT) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin join = call.rel(0);
        RelNode left = call.rel(1);
        final LogicalTableLookup logicalTableLookup = call.rel(2);
        final LogicalIndexScan logicalIndexScan = call.rel(3);

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        RexUtils.RestrictType restrictType = RexUtils.RestrictType.RIGHT;
        // FIXME: restrict the condition for tablelookup
        // 1. must have column ref indexScan
        // 2. when equal condition ref indexScan and Primary table, executor should use only indexScan ref to build
        // Lookupkey, and let Primary table ref as other condition
        if (!RexUtils.isBatchKeysAccessCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            restrictType,
            (Pair<RelDataType, RelDataType> relDataTypePair) -> CBOUtil.bkaTypeCheck(relDataTypePair))) {
            return;
        }

        if (!RexUtils.isBatchKeysAccessConditionRefIndexScan(newCondition, join, true, logicalTableLookup)) {
            return;
        }

        if (!canBKAJoin(join)) {
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

        left = convert(left, leftTraitSet);

        LogicalIndexScan newLogicalIndexScan =
            logicalIndexScan.copy(join.getCluster().getPlanner().emptyTraitSet().replace(outConvention));

        LogicalTableLookup right = logicalTableLookup.copy(
            rightTraitSet,
            newLogicalIndexScan,
            logicalTableLookup.getJoin().getRight(),
            logicalTableLookup.getIndexTable(),
            logicalTableLookup.getPrimaryTable(),
            logicalTableLookup.getProject(),
            logicalTableLookup.getJoin(),
            logicalTableLookup.isRelPushedToPrimary(),
            logicalTableLookup.getHints());

        createSemiBKAJoin(
            call,
            join,
            left,
            right,
            newCondition,
            newLogicalIndexScan);
    }

    protected abstract void createSemiBKAJoin(
        RelOptRuleCall call,
        LogicalSemiJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        LogicalIndexScan newLogicalIndexScan);

    private boolean canBKAJoin(SemiJoin join) {
        RelNode right = join.getRight();
        if (right instanceof RelSubset) {
            right = ((RelSubset) right).getOriginal();
        }
        if (CBOUtil.checkBkaJoinForLogicalView(right)) {
            return true;
        }
        return false;
    }
}
