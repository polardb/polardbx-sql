package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.CheckApplyInRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * this rule is used to convert LogicalSemiJoin + LogicalSort+ LogicalView to MaterializedSemiJoin,
 * and the logicalView must contain applyIn condition.
 */
public class SMPLogicalSortSemiJoinToMaterializedSemiJoinRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public static final SMPLogicalSortSemiJoinToMaterializedSemiJoinRule INSTANCE =
        new SMPLogicalSortSemiJoinToMaterializedSemiJoinRule(
            operand(LogicalSemiJoin.class,
                operand(LogicalSort.class, operand(LogicalView.class, any())),
                operand(RelSubset.class, any())), "INSTANCE");

    public SMPLogicalSortSemiJoinToMaterializedSemiJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "LogicalSortSemiJoinToMaterializedSemiJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_IN_TO_UNION_ALL);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(call.rel(0))) {
            return false;
        }
        if (call.rel(2) instanceof OSSTableScan) {
            return false;
        }
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin semiJoin = call.rel(0);
        final LogicalSort logicalSort = call.rel(1);
        final LogicalView logicalView = call.rel(2);
        if (logicalView instanceof OSSTableScan) {
            return;
        }
        RelNode right = call.rel(3);

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(semiJoin.getCondition(), semiJoin.getCluster().getRexBuilder());

        CheckApplyInRelVisitor checkApplyInRelVisitor = new CheckApplyInRelVisitor();
        logicalView.accept(checkApplyInRelVisitor);
        List<RexDynamicParam> applyInValues = checkApplyInRelVisitor.getApplyInValues();

        if (applyInValues.isEmpty()) {
            return;
        }

        RelTraitSet inputTraitSet = semiJoin.getCluster().getPlanner().emptyTraitSet().replace(outConvention);

        LogicalView newLogicalView = CBOUtil.sortLimitLogicalView(logicalView, logicalSort).copy(inputTraitSet);
        RelNode left = MergeSort.create(
            logicalSort.getTraitSet().replace(outConvention), newLogicalView, logicalSort.getCollation(),
            logicalSort.offset,
            logicalSort.fetch);
        right = convert(right, inputTraitSet);

        // LookupConditionBuilder.distinctLookupKeysChunk will filter out duplicated rows.
        MaterializedSemiJoin materializedSemiJoin = MaterializedSemiJoin.create(
            semiJoin.getTraitSet().replace(outConvention), left, right, newCondition, semiJoin,
            false);

        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_MATERIALIZED_SEMI_JOIN);
        if (fixedCost != null) {
            materializedSemiJoin.setFixedCost(fixedCost);
        }
        newLogicalView.setIsMGetEnabled(true);
        newLogicalView.setInToUnionAll(true);
        newLogicalView.setJoin(materializedSemiJoin);
        call.transformTo(materializedSemiJoin);
    }

}
