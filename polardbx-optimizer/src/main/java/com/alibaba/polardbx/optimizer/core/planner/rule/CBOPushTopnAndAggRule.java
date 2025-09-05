package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;

public class CBOPushTopnAndAggRule extends RelOptRule {
    public CBOPushTopnAndAggRule(RelOptRuleOperand operand, String description) {
        super(operand, "CBOPushTopnAndAggRule" + description);
    }

    public static final CBOPushTopnAndAggRule INSTANCE = new CBOPushTopnAndAggRule(
            operand(LogicalSort.class,
                    operand(LogicalAggregate.class,
                            operand(LogicalView.class, none()))), "LOGICALVIEW");

    /**
     *
     * @param call Rule call which has been determined to match all operands of
     *             this rule
     * @return
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!PlannerContext.getPlannerContext(call).getParamManager().getBoolean(ConnectionParams.ENABLE_PUSH_TOPN_AND_AGG)){
            return false;
        }

        final LogicalSort logicalSort = (LogicalSort) call.rels[0];
        final LogicalAggregate logicalAggregate = (LogicalAggregate) call.rels[1];
        final LogicalView logicalView = (LogicalView) call.rels[2];

        if (logicalView instanceof OSSTableScan){
            return false;
        }
        if (CBOUtil.containUnpushableAgg(logicalAggregate)){
            return false;
        }
        if (!(logicalSort.withOrderBy() && logicalSort.withLimit())){
            return false;
        }

        //agg可能其他rule已经执行下推
        if (logicalSort.getSortOptimizationContext().isSortPushed()){
            return false;
        }

        if (logicalView.getPushedRelNode() instanceof Sort){
            return false;
        }

        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSort logicalSort = (LogicalSort) call.rels[0];
        final LogicalAggregate logicalAggregate = (LogicalAggregate) call.rels[1];
        final LogicalView logicalView = (LogicalView) call.rels[2];

        //单表不需要当前rule处理
        if (logicalView.isSingleGroup()){
            return;
        }

        //check whether order by columns is subset of group by columns
        if (logicalAggregate.getGroupSet() == null){
            return;
        }
        for (RelFieldCollation fieldCollation : logicalSort.getCollation().getFieldCollations()){
            if (fieldCollation.getFieldIndex() >= logicalAggregate.getGroupCount()){
                return;
            }
        }

        RelNode globalAggregate;
        LogicalView aggLogicalView;
        //push agg
        if (logicalAggregate.getAggOptimizationContext().isAggPushed()){
            globalAggregate = logicalAggregate;
            aggLogicalView = logicalView;
        }else{
            globalAggregate = CBOPushAggRule.tryPushAgg(logicalAggregate, logicalView);
            if (globalAggregate == null){
                return;
            }
            if (!(globalAggregate instanceof LogicalAggregate) || !(((LogicalAggregate) globalAggregate).getInput() instanceof LogicalView)){
                return;
            }
            aggLogicalView = (LogicalView) ((LogicalAggregate) globalAggregate).getInput();
        }

        //push sort
        LogicalView aggAndSortLogicalView = CBOUtil.sortLimitLogicalView(aggLogicalView, logicalSort);
        LogicalView newLogicalView = aggAndSortLogicalView.copy(aggAndSortLogicalView.getTraitSet().replace(RelCollations.EMPTY));

        LogicalAggregate newAggregate = (LogicalAggregate) globalAggregate.copy(globalAggregate.getTraitSet(), ImmutableList.of(newLogicalView));

        LogicalSort newSort = (LogicalSort) logicalSort.copy(logicalSort.getTraitSet(), ImmutableList.of(newAggregate));
        newSort.getSortOptimizationContext().setSortPushed(true);//avoid push topn and agg again and again

        call.transformTo(newSort);

        return;

    }
}
