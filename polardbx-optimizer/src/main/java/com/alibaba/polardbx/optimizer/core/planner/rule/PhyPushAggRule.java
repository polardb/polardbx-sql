package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.TwoPhaseAggUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.ProjectRemoveRule;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PhyPushAggRule extends RelOptRule {

    public PhyPushAggRule(RelOptRuleOperand operand) {
        super(operand, "PhyPushAggRule");
    }

    public static final PhyPushAggRule INSTANCE = new PhyPushAggRule(
        operand(HashAgg.class,
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, none())));

    @Override
    public boolean matches(RelOptRuleCall call) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(call);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PUSH_AGG)) {
            return false;
        }
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.PREFER_PUSH_AGG)) {
            return false;
        }
        if (call.rels[1] instanceof OSSTableScan) {
            return false;
        }
        HashAgg hashAgg = (HashAgg) call.rels[0];
        if (hashAgg.getGroupSets().size() > 1) {
            return false;
        }
        if (hashAgg.indicator) {
            return false;
        }
        if (CBOUtil.isCheckSum(hashAgg) || CBOUtil.isGroupSets(hashAgg) || CBOUtil.isSingleValue(hashAgg)) {
            return false;
        }
        if (CBOUtil.containUnpushableAgg(hashAgg)) {
            return false;
        }

        return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (((LogicalView) call.rels[1]).isSingleGroup()) {
            onMatchSingle(call);
        } else {
            onMatchNotSingle(call);
        }
    }

    protected void onMatchSingle(RelOptRuleCall call) {
        HashAgg hashAgg = (HashAgg) call.rels[0];
        LogicalView logicalView = (LogicalView) call.rels[1];
        LogicalAggregate aggregate =
            LogicalAggregate.create(logicalView.getPushedRelNode(), hashAgg.getGroupSet(), hashAgg.getGroupSets(),
                hashAgg.getAggCallList());
        logicalView.push(aggregate);
        RelUtils.changeRowType(logicalView, aggregate.getRowType());
        call.transformTo(logicalView);
    }

    protected void onMatchNotSingle(RelOptRuleCall call) {
        HashAgg hashAgg = (HashAgg) call.rels[0];
        LogicalView logicalView = (LogicalView) call.rels[1];

        // try full matching first
        TddlRuleManager tddlRuleManager = PlannerContext.getPlannerContext(hashAgg).getExecutionContext()
            .getSchemaManager(logicalView.getSchemaName()).getTddlRuleManager();
        TableRule rt = tddlRuleManager.getTableRule(logicalView.getShardingTable());
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        final List<String> shardColumns;
        if (rt != null) {
            shardColumns = rt.getShardColumns();
        } else if (partitionInfoManager.isNewPartDbTable(logicalView.getShardingTable())) {
            shardColumns =
                partitionInfoManager.getPartitionInfo(logicalView.getShardingTable()).getPartitionColumns();
        } else {
            return;
        }

        Set<Integer> shardIndex = new HashSet<>();
        if (CBOPushAggRule.fullMatchSharding(hashAgg, logicalView, shardColumns, shardIndex)) {
            LogicalAggregate aggregate =
                LogicalAggregate.create(logicalView.getPushedRelNode(), hashAgg.getGroupSet(), hashAgg.getGroupSets(),
                    hashAgg.getAggCallList());
            logicalView.push(aggregate);
            logicalView.setOnePhaseAgg(true);
            RelUtils.changeRowType(logicalView, aggregate.getRowType());
            call.transformTo(logicalView);
            return;
        }

        if (!PlannerContext.getPlannerContext(call).getParamManager().getBoolean(ConnectionParams.ENABLE_PARTIAL_AGG)) {
            return;
        }
        // avoid push agg repeatedly
        if (logicalView.aggIsPushed()) {
            return;
        }
        TwoPhaseAggUtil.TwoPhaseAggComponent twoPhaseAggComponent =
            TwoPhaseAggUtil.splitAggWithDistinct(hashAgg, logicalView, shardIndex);
        if (twoPhaseAggComponent == null) {
            return;
        }

        LogicalAggregate partialAgg = LogicalAggregate.create(
            logicalView.getPushedRelNode(),
            twoPhaseAggComponent.getPartialAggGroupSet(),
            ImmutableList.of(twoPhaseAggComponent.getPartialAggGroupSet()),
            twoPhaseAggComponent.getPartialAggCalls());
        logicalView.push(partialAgg);
        RelUtils.changeRowType(logicalView, partialAgg.getRowType());

        HashAgg globalHashAgg = hashAgg.copy(
            hashAgg.getTraitSet(),
            logicalView,
            hashAgg.indicator,
            twoPhaseAggComponent.getGlobalAggGroupSet(),
            ImmutableList.of(twoPhaseAggComponent.getGlobalAggGroupSet()),
            twoPhaseAggComponent.getGlobalAggCalls());

        PhysicalProject project = new PhysicalProject(
            hashAgg.getCluster(),
            globalHashAgg.getTraitSet(),
            globalHashAgg,
            twoPhaseAggComponent.getProjectChildExps(),
            hashAgg.getRowType());

        if (ProjectRemoveRule.isTrivial(project)) {
            call.transformTo(globalHashAgg);
        } else {
            call.transformTo(project);
        }
    }
}
