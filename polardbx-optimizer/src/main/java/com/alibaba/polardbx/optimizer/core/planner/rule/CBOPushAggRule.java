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
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
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
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCheckSumMergeFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;

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
        if (logicalView instanceof OSSTableScan && !((OSSTableScan) logicalView).canPushAgg()) {
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

        if (fullMatchSharding(logicalAggregate, logicalView, shardColumns)) {
            // fullMatchSharding should not match AccessPathRule
            // so we convert LogicalView to DrdsConvention (AccessPathRule only match Convention.None!)
            LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().replace(DrdsConvention.INSTANCE));
            LogicalAggregate newLogicalAggregate = logicalAggregate.copy(
                newLogicalView.getPushedRelNode(), logicalAggregate.getGroupSet(), logicalAggregate.getAggCallList());
            newLogicalView.push(newLogicalAggregate);
            return newLogicalView;
        }

        if (haveAggWithDistinct(logicalAggregate.getAggCallList())) {
            return null;
        }

        TwoPhaseAggComponent twoPhaseAggComponent = splitAgg(logicalAggregate, logicalView instanceof OSSTableScan);
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

    private boolean fullMatchSharding(LogicalAggregate logicalAggregate, LogicalView logicalView,
                                      List<String> shardColumns) {
        int matchNum = 0;
        for (String shardName : shardColumns) {
            int shardRef = logicalView.getRefByColumnName(logicalView.getShardingTable(), shardName, false);
            if (shardRef != -1 && logicalAggregate.getGroupSet().asList().indexOf(shardRef) != -1) {
                matchNum++;
            }
        }
        return matchNum != 0 && matchNum == shardColumns.size();
    }

    public static class TwoPhaseAggComponent {

        private List<RexNode> projectChildExps; //          Upper Project
        private List<AggregateCall> globalAggCalls; //      Global Agg
        private ImmutableBitSet globalAggGroupSet;
        private List<AggregateCall> partialAggCalls;//      Partial Agg
        private ImmutableBitSet partialAggGroupSet;

        public TwoPhaseAggComponent(List<RexNode> projectChildExps,
                                    List<AggregateCall> globalAggCalls, ImmutableBitSet globalAggGroupSet,
                                    List<AggregateCall> partialAggCalls, ImmutableBitSet partialAggGroupSet) {
            this.projectChildExps = projectChildExps;
            this.globalAggCalls = globalAggCalls;
            this.globalAggGroupSet = globalAggGroupSet;
            this.partialAggCalls = partialAggCalls;
            this.partialAggGroupSet = partialAggGroupSet;
        }

        public List<RexNode> getProjectChildExps() {
            return projectChildExps;
        }

        public List<AggregateCall> getGlobalAggCalls() {
            return globalAggCalls;
        }

        public List<AggregateCall> getPartialAggCalls() {
            return partialAggCalls;
        }

        public ImmutableBitSet getGlobalAggGroupSet() {
            return globalAggGroupSet;
        }

        public ImmutableBitSet getPartialAggGroupSet() {
            return partialAggGroupSet;
        }
    }

    public static TwoPhaseAggComponent splitAgg(Aggregate agg) {
        return splitAgg(agg, true);
    }

    public static TwoPhaseAggComponent splitAgg(Aggregate agg, boolean canSplitOrcHash) {
        TddlTypeFactoryImpl tddlTypeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        List<RexNode> childExps = new ArrayList<>();
        final int aggGroupSetCardinality = agg.getGroupSet().cardinality();
        for (int i = 0; i < aggGroupSetCardinality; i++) {
            childExps.add(new RexInputRef(i, agg.getRowType().getFieldList().get(i).getType()));
        }

        List<AggregateCall> globalAggCalls = new ArrayList<>();
        List<AggregateCall> partialAggCalls = new ArrayList<>();

        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCall aggCall = agg.getAggCallList().get(i);
            SqlAggFunction function = aggCall.getAggregation();
            switch (function.getKind()) {
            case COUNT:
                SqlSumEmptyIsZeroAggFunction sumAggFunction = new SqlSumEmptyIsZeroAggFunction();

                AggregateCall sumAggregateCall = AggregateCall.create(sumAggFunction,
                    aggCall.isDistinct(),
                    aggCall.isApproximate(),
                    ImmutableList.of(aggGroupSetCardinality + partialAggCalls.size()),
                    aggCall.filterArg,
                    aggCall.getType(),
                    aggCall.getName());

                globalAggCalls.add(sumAggregateCall);

                childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

                partialAggCalls.add(aggCall);
                break;
            case AVG:
                SqlSumAggFunction partialSumAggFunc = new SqlSumAggFunction(null);
                AggregateCall partialSumAggCall = AggregateCall.create(partialSumAggFunc,
                    aggCall.isDistinct(),
                    aggCall.isApproximate(),
                    aggCall.getArgList(),
                    aggCall.filterArg,
                    aggCall.getType(),
                    "partial_sum");

                AggregateCall globalSumAggCall =
                    partialSumAggCall.copy(ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()),
                        -1,
                        false,
                        "global_sum");

                partialAggCalls.add(partialSumAggCall);

                SqlCountAggFunction pushedCountFunc = new SqlCountAggFunction("COUNT");
                AggregateCall partialCountAggCall = AggregateCall.create(pushedCountFunc,
                    aggCall.isDistinct(),
                    aggCall.isApproximate(),
                    aggCall.getArgList(),
                    aggCall.filterArg,
                    tddlTypeFactory.createSqlType(SqlTypeName.BIGINT),
                    "partial_count");

                AggregateCall globalCountAggCall = AggregateCall.create(partialSumAggFunc,
                    partialCountAggCall.isDistinct(),
                    partialCountAggCall.isApproximate(),
                    ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()),
                    partialCountAggCall.filterArg,
                    partialCountAggCall.getType(),
                    "global_count");

                partialAggCalls.add(partialCountAggCall);

                RexInputRef partialSumRef =
                    new RexInputRef(globalSumAggCall.getArgList().get(0), partialSumAggCall.getType());
                RexInputRef partialCountRef =
                    new RexInputRef(globalCountAggCall.getArgList().get(0), partialCountAggCall.getType());

                RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
                RexCall divide = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
                    partialSumRef,
                    partialCountRef);

                RelDataType relDataType = aggCall.getType();
                if (!divide.getType().getSqlTypeName().equals(relDataType.getSqlTypeName())) {
                    RexNode castNode = rexBuilder.makeCastForConvertlet(relDataType, divide);
                    childExps.add(castNode);
                } else {
                    childExps.add(divide);
                }

                globalAggCalls.add(globalSumAggCall);
                globalAggCalls.add(globalCountAggCall);
                break;
            case MIN:
            case MAX:
            case SUM:
            case BIT_OR:
            case BIT_XOR:
            case BIT_AND:
            case __FIRST_VALUE:
                AggregateCall newAggCall =
                    aggCall.copy(ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()), -1);
                globalAggCalls.add(newAggCall);

                childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

                partialAggCalls.add(aggCall);
                break;
            case GROUP_CONCAT:
                GroupConcatAggregateCall groupConcatAggregateCall = (GroupConcatAggregateCall) aggCall;
                if (groupConcatAggregateCall.getOrderList() != null
                    && groupConcatAggregateCall.getOrderList().size() != 0) {
                    return null;
                }
                GroupConcatAggregateCall newGroupConcatAggregateCall =
                    groupConcatAggregateCall.copy(ImmutableIntList.of(aggGroupSetCardinality + partialAggCalls.size()),
                        -1, groupConcatAggregateCall.getOrderList());
                globalAggCalls.add(newGroupConcatAggregateCall);

                childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

                partialAggCalls.add(aggCall);
                break;
            case CHECK_SUM:
                if (!canSplitOrcHash) {
                    return null;
                }
                SqlCheckSumMergeFunction crcAggFunction = new SqlCheckSumMergeFunction();

                AggregateCall crcHashAggregateCall = AggregateCall.create(crcAggFunction,
                    aggCall.isDistinct(),
                    aggCall.isApproximate(),
                    ImmutableList.of(aggGroupSetCardinality + partialAggCalls.size()),
                    aggCall.filterArg,
                    aggCall.getType(),
                    aggCall.getName());

                globalAggCalls.add(crcHashAggregateCall);

                childExps.add(new RexInputRef(aggGroupSetCardinality + partialAggCalls.size(), aggCall.getType()));

                partialAggCalls.add(aggCall);
                break;
            default:
                return null;
            }
        }
        return new TwoPhaseAggComponent(childExps, globalAggCalls,
            ImmutableBitSet.range(agg.getGroupSet().cardinality()),
            partialAggCalls, agg.getGroupSet());
    }
}

