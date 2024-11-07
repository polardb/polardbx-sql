package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.google.common.base.Predicate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;

public class COLSortJoinTransposeRule extends RelOptRule {

    public static final COLSortJoinTransposeRule INSTANCE = new COLSortJoinTransposeRule(
        operand(LogicalSort.class, null, (Predicate<Sort>) sort -> sort.withLimit(),
            operand(LogicalJoin.class, null,
                (Predicate<Join>) join -> (join instanceof LogicalJoin || join instanceof LogicalSemiJoin)
                    && (join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.RIGHT)
                    && (join.getTraitSet().simplify().getCollation().isTop()
                    && join.getTraitSet().simplify().getDistribution().isTop()), any())),
        "COLSortJoinTransposeRule"
    );

    public COLSortJoinTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        if (sort.getSortOptimizationContext().isSortPushed()) {
            return false;
        }
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SORT_JOIN_TRANSPOSE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);
        final LogicalJoin join = call.rel(1);

        // We create a new sort operator on the corresponding input
        final RelNode newLeftInput;
        final RelNode newRightInput;

        final int leftWidth = join.getLeft().getRowType().getFieldList().size();
        if (join.getJoinType() == JoinRelType.LEFT) {
            for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
                if (relFieldCollation.getFieldIndex() >= leftWidth) {
                    return;
                }
            }
            RelCollation collationToPush = sort.getCollation();
            collationToPush = RelCollationTraitDef.INSTANCE.canonize(collationToPush);

            newLeftInput =
                sort.copy(sort.getTraitSet().replace(collationToPush),
                    join.getLeft(), collationToPush, null, CBOUtil.calPushDownFetch(sort));
            newRightInput = join.getRight();

        } else {
            for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
                if (relFieldCollation.getFieldIndex() < leftWidth) {
                    return;
                }
            }
            RelCollation collationToPush = RelCollations.shift(sort.getCollation(), -leftWidth);
            collationToPush = RelCollationTraitDef.INSTANCE.canonize(collationToPush);
            newRightInput =
                sort.copy(sort.getTraitSet().replace(collationToPush),
                    join.getRight(),
                    collationToPush, null, CBOUtil.calPushDownFetch(sort));
            newLeftInput = join.getLeft();
        }

        final RelNode joinCopy =
            join.copy(join.getTraitSet(),
                join.getCondition(),
                newLeftInput,
                newRightInput,
                join.getJoinType(),
                join.isSemiJoinDone());

        LogicalSort limit = sort.copy(sort.getTraitSet(), joinCopy, sort.getCollation(), sort.offset, sort.fetch);
        limit.getSortOptimizationContext().setSortPushed(true);
        call.transformTo(limit);
    }
}
