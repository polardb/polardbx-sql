/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.TwoPhaseAggUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.canSplitDistinct;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.haveAggWithDistinct;

public class CBOPushAggRule extends RelOptRule {
    public CBOPushAggRule(RelOptRuleOperand operand, String description) {
        super(operand, "CBOPushAggRule:" + description);
    }

    public static final CBOPushAggRule LOGICALVIEW = new CBOPushAggRule(
        operand(LogicalAggregate.class, operand(LogicalView.class, none())), "LOGICALVIEW");

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalView logicalView = (LogicalView) call.rels[1];
        if (logicalView instanceof OSSTableScan) {
            if (PlannerContext.getPlannerContext(call).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_COLUMNAR_SCAN_EXEC)) {
                // columnar scan exec does not support agg push down
                return false;
            }
            if (!((OSSTableScan) logicalView).canPushAgg()) {
                return false;
            }
        }
        if (CBOUtil.containUnpushableAgg((LogicalAggregate) call.rels[0])) {
            return false;
        }
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_CBO_PUSH_AGG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate logicalAggregate = (LogicalAggregate) call.rels[0];
        final LogicalView logicalView = (LogicalView) call.rels[1];
        if (logicalView instanceof OSSTableScan) {
            if (!CBOUtil.canPushAggToOss(logicalAggregate, (OSSTableScan) logicalView)) {
                return;
            }
            if (PlannerContext.getPlannerContext(call).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_OSS_BUFFER_POOL)) {
                return;
            }
        }
        if (logicalView.isSingleGroup()) {
            if (CBOUtil.isCheckSum(logicalAggregate) && !(logicalView instanceof OSSTableScan)) {
                return;
            }
            if (CBOUtil.isCheckSumV2(logicalAggregate) && !(logicalView instanceof OSSTableScan)) {
                return;
            }
            LogicalAggregate newLogicalAggregate = logicalAggregate.copy(
                logicalView.getPushedRelNode(), logicalAggregate.getGroupSet(), logicalAggregate.getAggCallList());
            LogicalView newLogicalView = logicalView.copy(logicalAggregate.getTraitSet());
            newLogicalView.push(newLogicalAggregate);
            call.transformTo(newLogicalView);
        } else {
            RelNode node = tryPushAgg(logicalAggregate, logicalView);
            if (node != null) {
                call.transformTo(node);
            }
        }
    }

    private RelNode tryPushAgg(LogicalAggregate logicalAggregate, LogicalView logicalView) {
        assert logicalAggregate != null && logicalView != null;
        if (logicalAggregate.getAggOptimizationContext().isAggPushed()) {
            return null;
        }
        TddlRuleManager tddlRuleManager =
            PlannerContext.getPlannerContext(logicalAggregate).getExecutionContext()
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
            return null;
        }

        Set<Integer> shardIndex = new HashSet<>();
        if (fullMatchSharding(logicalAggregate, logicalView, shardColumns, shardIndex)) {
            // fullMatchSharding should not match AccessPathRule
            // so we convert LogicalView to DrdsConvention (AccessPathRule only match Convention.None!)
            LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().replace(DrdsConvention.INSTANCE));
            LogicalAggregate newLogicalAggregate = logicalAggregate.copy(
                newLogicalView.getPushedRelNode(), logicalAggregate.getGroupSet(), logicalAggregate.getAggCallList());
            newLogicalView.push(newLogicalAggregate);
            return newLogicalView;
        }

        TwoPhaseAggUtil.TwoPhaseAggComponent twoPhaseAggComponent =
            TwoPhaseAggUtil.splitAggWithDistinct(logicalAggregate, logicalView, shardIndex);
        if (twoPhaseAggComponent == null) {
            return null;
        }
        List<RexNode> childExps = twoPhaseAggComponent.getProjectChildExps();
        List<AggregateCall> globalAggCalls = twoPhaseAggComponent.getGlobalAggCalls();
        ImmutableBitSet globalAggGroupSet = twoPhaseAggComponent.getGlobalAggGroupSet();
        List<AggregateCall> partialAggCalls = twoPhaseAggComponent.getPartialAggCalls();
        ImmutableBitSet partialAggGroupSet = twoPhaseAggComponent.getPartialAggGroupSet();

        LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet());
        LogicalAggregate partialAgg = LogicalAggregate
            .create(newLogicalView.getPushedRelNode(), partialAggGroupSet, logicalAggregate.getGroupSets(),
                partialAggCalls);
        newLogicalView.push(partialAgg);
        LogicalAggregate globalAgg = logicalAggregate.copy(logicalAggregate.getTraitSet(), newLogicalView,
            logicalAggregate.indicator,
            globalAggGroupSet,
            ImmutableList.of(globalAggGroupSet), // FIXME if groupSets used ?
            globalAggCalls);

        globalAgg.getAggOptimizationContext()
            .setAggPushed(true); // TODO any better way to avoid push partial agg again and again?

        LogicalProject project = new LogicalProject(logicalAggregate.getCluster(),
            logicalAggregate.getTraitSet(),
            globalAgg,
            childExps,
            logicalAggregate.getRowType());

        if (ProjectRemoveRule.isTrivial(project)) {
            return globalAgg;
        } else {
            return project;
        }
    }

    public static boolean fullMatchSharding(Aggregate aggregate, LogicalView logicalView,
                                            List<String> shardColumns, Set<Integer> shardIndex) {
        int matchNum = 0;
        for (String shardName : shardColumns) {
            int shardRef = logicalView.getRefByColumnName(logicalView.getShardingTable(), shardName, false, true);
            if (shardRef != -1 && aggregate.getGroupSet().asList().contains(shardRef)) {
                matchNum++;
            }
            shardIndex.add(shardRef);
        }
        return matchNum != 0 && matchNum == shardColumns.size();
    }
}

