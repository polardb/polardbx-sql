package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;

/**
 * transform join from tableLookup to BKA join.
 * the rule is invoked in rbo phase optimizeByExpandTableLookup, right after TableLookupExpandRule
 */
public class ExpandLogicalJoinToBKAJoinRule extends RelOptRule {

    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected static PredicateImpl JOIN_RIGHT = new PredicateImpl<LogicalJoin>() {
        @Override
        public boolean test(LogicalJoin logicalJoin) {
            return logicalJoin.getJoinType() == JoinRelType.RIGHT;
        }
    };

    protected static PredicateImpl JOIN_NOT_RIGHT = new PredicateImpl<LogicalJoin>() {
        @Override
        public boolean test(LogicalJoin logicalJoin) {
            return logicalJoin.getJoinType() != JoinRelType.RIGHT;
        }
    };

    public static final ExpandLogicalJoinToBKAJoinRule
        LOGICALVIEW_NOT_RIGHT = new ExpandLogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_NOT_RIGHT,
            operand(RelNode.class, any()),
            operand(LogicalView.class, any())), "LOGICALVIEW:NOT_RIGHT_FOR_EXPAND");

    public static final ExpandLogicalJoinToBKAJoinRule
        LOGICALVIEW_RIGHT = new ExpandLogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_RIGHT,
            operand(LogicalView.class, any()),
            operand(RelNode.class, any())), "LOGICALVIEW:RIGHT_FOR_EXPAND");

    ExpandLogicalJoinToBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "ExpandLogicalJoinToBKAJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_BKA_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        final LogicalJoin join = (LogicalJoin) rel;
        final LogicalView logicalView;
        if (join.getJoinType() == JoinRelType.RIGHT) {
            logicalView = call.rel(1);
        } else {
            logicalView = call.rel(2);
        }

        RexUtils.RestrictType restrictType;
        switch (join.getJoinType()) {
        case LEFT:
        case INNER:
            restrictType = RexUtils.RestrictType.RIGHT;
            break;
        case RIGHT:
            restrictType = RexUtils.RestrictType.LEFT;
            break;
        default:
            return;
        }

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        if (!RexUtils.isBatchKeysAccessCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            restrictType,
            (Pair<RelDataType, RelDataType> relDataTypePair) -> CBOUtil.bkaTypeCheck(relDataTypePair))) {
            return;
        }

        if (!CBOUtil.canBKAJoin(join)) {
            return;
        }

        RelNode left;
        RelNode right;
        final LogicalView inner;
        if (join.getJoinType().equals(JoinRelType.RIGHT)) {
            left = inner = logicalView.copy(logicalView.getTraitSet().replace(outConvention));
            right = convert(join.getRight(), join.getRight().getTraitSet().replace(outConvention));
        } else {
            left = convert(join.getLeft(), join.getLeft().getTraitSet().replace(outConvention));
            right = inner = logicalView.copy(logicalView.getTraitSet().replace(outConvention));
        }

        BKAJoin bkaJoin = BKAJoin.create(
            join.getTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints()); // PK set in inner table for runtime optimize.
        inner.setIsMGetEnabled(true);
        inner.setJoin(bkaJoin);
        call.transformTo(bkaJoin);
    }

}
