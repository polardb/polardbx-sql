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

import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.GroupConcatAggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil.isGroupSets;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.buildNewGroupSet;

/**
 * Created by lingce.ldm on 2016/11/2.
 */
public abstract class PushAggRule extends RelOptRule {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public PushAggRule(RelOptRuleOperand operand, String description) {
        super(operand, "Tddl_push_agg_rule:" + description);
    }

    private static final Predicate<LogicalAggregate> AGG_IS_NOT_PUSHED = new PredicateImpl<LogicalAggregate>() {

        @Override
        public boolean test(LogicalAggregate agg) {
            return !agg.getAggOptimizationContext().isAggPushed();
        }
    };

    public static final PushAggRule SINGLE_GROUP_VIEW = new PushAggViewRule();
    public static final PushAggRule NOT_SINGLE_GROUP_VIEW = new PushAggNotSingleGroupViewRule();

    @Override
    public boolean matches(RelOptRuleCall call) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(call);
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PUSH_AGG);
    }

    protected boolean containChecksum(LogicalAggregate agg) {
        for (AggregateCall call : agg.getAggCallList()) {
            if (call.getAggregation().getKind() == SqlKind.CHECK_SUM) {
                return true;
            }
        }
        return false;
    }

    protected boolean shouldPushAgg(LogicalView logicalView) {
        int pushAggInputRowCountThreshold = PlannerContext.getPlannerContext(logicalView).getParamManager()
            .getInt(ConnectionParams.PUSH_AGG_INPUT_ROW_COUNT_THRESHOLD);
        if (logicalView.getCluster().getMetadataQuery().getRowCount(logicalView) > pushAggInputRowCountThreshold) {
            return false;
        }
        return onlyOneAccessPath(logicalView);
    }

    public static boolean onlyOneAccessPath(LogicalView logicalView) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(logicalView);
        if (plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_INDEX_SELECTION)) {
            return AccessPathRule.hasOnlyOneAccessPath(logicalView, plannerContext.getExecutionContext());
        } else {
            return true;
        }
    }

    /**
     * AGG -> VIEW
     */
    public static class PushAggViewRule extends PushAggRule {

        @Override
        public boolean matches(RelOptRuleCall call) {
            LogicalAggregate aggregate = (LogicalAggregate) call.rels[0];
            if (aggregate.getGroupSets().size() > 1) {
                return false;
            }
            LogicalView tableScan = (LogicalView) call.rels[1];

            if (tableScan instanceof OSSTableScan) {
                return false;
            }

            if (containChecksum(aggregate)) {
                return false;
            }

            if (!shouldPushAgg(tableScan)) {
                return false;
            }

            if (isGroupSets(aggregate)) {
                return false;
            }

            return super.matches(call);
        }

        public PushAggViewRule() {
            super(
                operand(LogicalAggregate.class, operand(LogicalView.class, null, LogicalView.IS_SINGLE_GROUP, none())),
                "agg_view");
        }

        /**
         * 直接下推,上层无需保留
         */
        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalAggregate aggregate = (LogicalAggregate) call.rels[0];
            for (AggregateCall aggCall : aggregate.getAggCallList()) {
                if (aggCall.getAggregation().getKind() == SqlKind.SINGLE_VALUE) {
                    return;
                }
            }
            pushAggToView(call);
        }
    }

    protected void pushAggToView(RelOptRuleCall call) {
        LogicalAggregate aggregate = (LogicalAggregate) call.rels[0];
        LogicalView tableScan = (LogicalView) call.rels[1];
        tableScan.push(aggregate);
        RelUtils.changeRowType(tableScan, aggregate.getRowType());
        call.transformTo(tableScan);
    }

    /**
     * AGG -> VIEW (not-single-group)
     */
    private static class PushAggNotSingleGroupViewRule extends PushAggRule {

        public PushAggNotSingleGroupViewRule() {
            super(
                operand(LogicalAggregate.class,
                    null,
                    AGG_IS_NOT_PUSHED,
                    operand(LogicalView.class, null, LogicalView.NOT_SINGLE_GROUP, none())),
                "agg_gather");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            LogicalAggregate aggregate = (LogicalAggregate) call.rels[0];
            if (aggregate.getGroupSets().size() > 1) {
                return false;
            }

            LogicalView tableScan = (LogicalView) call.rels[1];

            if(tableScan instanceof OSSTableScan) {
                return false;
            }

            if (containChecksum(aggregate)) {
                return false;
            }

            if (!shouldPushAgg(tableScan)) {
                return false;
            }

            if (isGroupSets(aggregate)) {
                return false;
            }

            return super.matches(call);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalAggregate aggregate = (LogicalAggregate) call.rels[0];
            LogicalView lv = (LogicalView) call.rels[1];

            for (AggregateCall aggCall : aggregate.getAggCallList()) {
                if (aggCall.getAggregation().getKind() == SqlKind.SINGLE_VALUE) {
                    return;
                }
            }
            aggregate.getAggOptimizationContext().setAggPushed(true);
            pushDownAggregate(call, aggregate, lv);
        }
    }

    protected void pushDownAggregate(RelOptRuleCall call, LogicalAggregate aggregate, LogicalView tableScan) {
        // single_value 不下推
        if (aggregate.getAggCallList() != null) {
            for (AggregateCall aggCall : aggregate.getAggCallList()) {
                if (aggCall != null && aggCall.getAggregation() != null
                    && aggCall.getAggregation().getKind() == SqlKind.SINGLE_VALUE) {
                    return;
                }
            }
        }
        // 1. check if need push down
        // 2. init push down context
        // DSqlTableScan
        // avgIndex
        // newAggCalls
        // pushedAggCalls
        // 3. switch aggregate and push
        // 4. build new RelNode tree
        // 5. transform
        PushDownAggCtx ctx = new PushDownAggCtx(tableScan, aggregate);
        logger.debug("tableScan.aggIsPushed:" + tableScan.aggIsPushed());

        TddlRuleManager or =
            PlannerContext.getPlannerContext(aggregate).getExecutionContext()
                .getSchemaManager(tableScan.getSchemaName()).getTddlRuleManager();
        TableRule rt = or.getTableRule(tableScan.getShardingTable());

        List<String> shardColumns = or.getSharedColumns(tableScan.getShardingTable());
        //TableRule rt = ruleManager.getTableRule(tableScan.getShardingTable());

        if (shardColumns == null || shardColumns.isEmpty()) {// 单表
            tableScan.push(aggregate);
            RelUtils.changeRowType(tableScan, aggregate.getRowType());
            call.transformTo(tableScan);
            return;
        }

        boolean fullMatch = false;
        int matchNum = 0;

        for (String shardName : shardColumns) {
            boolean match = false;
            int shardRef = tableScan.getRefByColumnName(tableScan.getShardingTable(), shardName, false);
            if (shardRef == -1) {
                break;
            } else {
                for (Integer i : aggregate.getGroupSet().asList()) {
                    if (i == shardRef) {
                        match = true;
                        matchNum++;
                        break;
                    }
                }
            }

            /**
             * 聚合函数中的 distinct 也做为 group by 的一部分来处理。
             */
            // for(AggregateCall aggCall:aggregate.getAggCallList()){
            // if (aggCall.isDistinct() && nameContains(aggCall.getArgList(),
            // aggregate.getInput().getRowType(), shardName)) {
            // match = true;
            // break;
            // }
            //
            // }
            if (!match) {
                break;
            }
        }

        if (matchNum != 0 && matchNum == shardColumns.size()) {
            fullMatch = true;
        }

        if (fullMatch) {
            tableScan.push(aggregate);
            RelUtils.changeRowType(tableScan, aggregate.getRowType());
            // no Gather any more
            call.transformTo(tableScan);
            return;
        }

        RelUtils.changeRowType(aggregate.getInput(), tableScan.getRowType());

        // 已经执行过下压
        if (ctx.getTableScan().aggIsPushed()) {
            return;
        }

        if (PlannerUtils.shouldNotPushDistinctAgg(aggregate, shardColumns)) {
            return;
        }

        /**
         * push distinct
         */
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        if (PlannerUtils.haveAggWithDistinct(aggCalls)) {
            if (PlannerUtils.isAllowedAggWithDistinct(aggregate, shardColumns)) {
                List<Integer> groupSet = buildNewGroupSet(aggregate);
                ImmutableBitSet bitSet = ImmutableBitSet.of(groupSet);

                List<AggregateCall> internalAgg = new ArrayList<>();
                for (AggregateCall ac : aggregate.getAggCallList()) {
                    if (ac.getAggregation().getKind().equals(SqlKind.__FIRST_VALUE)) {
                        internalAgg.add(ac);
                    }
                }
                LogicalAggregate distinctAgg = LogicalAggregate.create(tableScan,
                    bitSet,
                    ImmutableList.of(bitSet),
                    internalAgg);

                LogicalView newTableScan = tableScan.copy(tableScan.getTraitSet());
                newTableScan.push(distinctAgg);

                RelDataType originType = tableScan.getRowType();
                RelDataType newType = distinctAgg.getRowType();

                /**
                 * 由于下层输出的列发生变化,需要重新构建上层 Agg,包括 groupSet 和 AggCall中的引用
                 */
                LogicalAggregate transformedAgg = buildNewAggWithDistinct(aggregate,
                    newTableScan,
                    originType,
                    newType);
                if (transformedAgg == null) {
                    logger.warn("Push agg with distinct error.");
                    return;
                }
                RelUtils.changeRowType(newTableScan, newType);
                call.transformTo(transformedAgg);
                return;
            } else {
                return;
            }
        }

        // main routine
        for (int i = 0; i < ctx.getAggCallCount(); ++i) {
            AggregateCall aggCall = ctx.getOneAggCall(i);
            SqlAggFunction function = aggCall.getAggregation();
            switch (function.getKind()) {
            // 将count下压，并将count修改为sum
            case COUNT:
                SqlSumEmptyIsZeroAggFunction sumAggFunction = new SqlSumEmptyIsZeroAggFunction();

                // NOTE: Aggregate节点的输出总是groupSet列在前，对应agg列在后
                AggregateCall sumAggregateCall = ctx.create(sumAggFunction, false);
                ctx.addNewAggCalls(i, sumAggregateCall);
                ctx.addPushedAggCalls(i, aggCall);
                ctx.addPushedToPartitionAggCalls(i, sumAggregateCall);
                break;
            /**
             * 将AVG分解为sum和count下压至tableScan，并将当前Agg节点中的avg替换为两个sum：
             * sum(sum()) 计算总和 SUM_ALL sum(count(*)) 计算总数 COUNT_ALL
             * 最后为当前Agg节点添加一个Project父节点，计算 SUM_ALL/COUNT_ALL
             */
            case AVG:
                TddlTypeFactoryImpl tddlTypeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

                SqlSumAggFunction pushedSumAggFunc = new SqlSumAggFunction(null);
                AggregateCall pushedSumAggCall = ctx.create(pushedSumAggFunc,
                    aggCall.getArgList(),
                    aggCall.getType(),
                    "pushed_sum");

                SqlCountAggFunction pushedCountFunc = new SqlCountAggFunction("COUNT");
                AggregateCall pushedCountAggCall = ctx.create(pushedCountFunc,
                    aggCall.getArgList(),
                    tddlTypeFactory.createSqlType(SqlTypeName.BIGINT),
                    "pushed_count");

                // 下压至TableScan，并转换为nativeSQL的聚合函数，物理表级别聚合
                int addedAggIndex = ctx.getCurAvgCount() + ctx.getAggCallCount();
                ctx.addPushedAggCalls(i, pushedSumAggCall);
                ctx.addPushedAggCalls(addedAggIndex, pushedCountAggCall);

                AggregateCall sumSumAggCall = pushedSumAggCall.copy(ImmutableIntList.of(i + ctx.getGroupSetLen()),
                    -1,
                    false,
                    "sum_pushed_sum");

                AggregateCall sumCountAggCall = pushedSumAggCall
                    .copy(ImmutableIntList.of(addedAggIndex + ctx.getGroupSetLen()), -1, false, "sum_pushed_count");

                // 保留在TableScan上层的聚合函数(全局聚合)
                ctx.addNewAggCalls(i, sumSumAggCall);
                ctx.addNewAggCalls(addedAggIndex, sumCountAggCall);

                // 下压至partition级别的聚合函数，partition级别的局部集合
                ctx.addPushedToPartitionAggCalls(i, sumSumAggCall);
                ctx.addPushedToPartitionAggCalls(addedAggIndex, sumCountAggCall);

                ctx.addAvgIndex(ctx.getCurAvgCount(), i);
                ctx.curAvgCountPlusOne();
                ctx.setNeedNewAgg(true);
                break;
            // 以下三种可以直接下压，当前Agg节点需要修改引用参数
            case MIN:
            case MAX:
            case SUM:
            case BIT_OR:
            case BIT_XOR:
            case BIT_AND:
            case __FIRST_VALUE:
                AggregateCall newAggCall = aggCall.copy(ImmutableIntList.of(ctx.getGroupSetLen() + i), -1);
                ctx.addNewAggCalls(i, newAggCall);
                ctx.addPushedAggCalls(i, aggCall);
                ctx.addPushedToPartitionAggCalls(i, aggCall);
                break;
            case GROUP_CONCAT:
                GroupConcatAggregateCall groupConcatAggregateCall = (GroupConcatAggregateCall) aggCall;
                if (groupConcatAggregateCall.getOrderList() != null
                    && groupConcatAggregateCall.getOrderList().size() != 0) {
                    return;
                }
                GroupConcatAggregateCall newGroupConcatAggregateCall =
                    groupConcatAggregateCall.copy(ImmutableIntList.of(ctx.getGroupSetLen() + i),
                        -1, groupConcatAggregateCall.getOrderList());
                ctx.addNewAggCalls(i, newGroupConcatAggregateCall);
                ctx.addPushedAggCalls(i, groupConcatAggregateCall);
                ctx.addPushedToPartitionAggCalls(i, groupConcatAggregateCall);
                break;
            default:
                throw new UnsupportedOperationException(
                    "Unsupported agg function to push down:" + function.getKind().name());
            }
        }
        build(ctx, call, aggregate);
    }

    private void build(PushDownAggCtx ctx, RelOptRuleCall call, LogicalAggregate aggregate) {
        logger.trace("Push down agg: start build");

        LogicalAggregate pushedAgg = aggregate;
        LogicalView tableScan = ctx.getTableScan();
        if (ctx.getNeedNewAgg()) {
            pushedAgg = new LogicalAggregate(ctx.getCluster(),
                aggregate.getTraitSet(),
                tableScan,
                false,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                new ArrayList<>(ctx.getPushedAggCalls().values()));
        }

        // 将Aggregate下压
        tableScan.push(pushedAgg);

        // 更新dSqlTableScan的rowType
        RelUtils.changeRowType(tableScan, pushedAgg.getRowType());
        RelNode input = tableScan;

        // 构建新的RelNode替换当前匹配的子树
        RelNode newRelNode;
        // 对于只有group by，没有聚合函数的情况，相当于project，
        // 需要修改现有Agg节点的group by字段的引用值
        ImmutableBitSet immutableBitSet = ImmutableBitSet.range(0, ctx.getGroupSetLen());
        if (ctx.getAggCallCount() == 0) {
            newRelNode = aggregate.copy(aggregate.getTraitSet(),
                input,
                false,
                immutableBitSet,
                ImmutableList.of(immutableBitSet),
                aggregate.getAggCallList());
        } else {

            newRelNode = aggregate.copy(aggregate.getTraitSet(),
                input,
                false,
                immutableBitSet,
                ImmutableList.of(immutableBitSet),
                new ArrayList<>(ctx.getNewAggCalls().values()));

            // 有AVG函数，需要构建一个Project父节点
            if (ctx.getCurAvgCount() > 0) {
                List<RexNode> childExps = new ArrayList<>();
                RelRecordType newDataType = (RelRecordType) newRelNode.getRowType();
                RelRecordType originalDataType = (RelRecordType) aggregate.getRowType();

                for (int i = 0; i < ctx.getGroupSetLen(); i++) {
                    RexInputRef rexInputRef = RexInputRef.of(i, originalDataType);
                    childExps.add(i, rexInputRef);
                }

                for (int i = ctx.getGroupSetLen(); i < ctx.getAggCallCount() + ctx.getGroupSetLen(); i++) {
                    int avgBaseIndex = i - ctx.getGroupSetLen();
                    if (ctx.getAvgIndex().contains(avgBaseIndex)) {
                        int sumIndex = i;
                        int countIndex = ctx.getGroupSetLen() + ctx.getAvgIndex().indexOf(avgBaseIndex)
                            + ctx.getAggCallCount();
                        RexInputRef sumSumRef = RexInputRef.of(sumIndex, newDataType);
                        RexInputRef sumCountRef = RexInputRef.of(countIndex, newDataType);

                        RexBuilder rexBuilder = ctx.getRexBuilder();
                        RexCall rexCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
                            sumSumRef,
                            sumCountRef);

                        RelDataType relDataType = aggregate.getAggCallList().get(avgBaseIndex).getType();
                        if (!rexCall.getType().getSqlTypeName().equals(relDataType.getSqlTypeName())) {
                            RexNode castNode = rexBuilder.makeCastForConvertlet(relDataType, rexCall);
                            childExps.add(i, castNode);
                        } else {
                            childExps.add(i, rexCall);
                        }
                    } else {
                        RexInputRef rexInputRef = RexInputRef.of(i, originalDataType);
                        childExps.add(i, rexInputRef);
                    }
                }

                newRelNode = new LogicalProject(ctx.getCluster(),
                    aggregate.getTraitSet(),
                    newRelNode,
                    childExps,
                    aggregate.getRowType());
            }
        }

        call.transformTo(newRelNode);
    }

    private LogicalAggregate buildNewAggWithDistinct(LogicalAggregate agg, RelNode tableScan, RelDataType oldType,
                                                     RelDataType newType) {
        List<Integer> newGroupSet = new ArrayList<>();
        List<RelDataTypeField> oldField = oldType.getFieldList();
        List<RelDataTypeField> newField = newType.getFieldList();
        int newRef;
        for (int i : agg.getGroupSet()) {
            newRef = findNewRef(i, oldField, newField);
            if (newRef == -2) {
                return null;
            }
            newGroupSet.add(newRef);
        }

        List<AggregateCall> newAggCalls = new ArrayList<>();
        for (AggregateCall call : agg.getAggCallList()) {
            List<Integer> newArgs = new ArrayList<>();
            for (int arg : call.getArgList()) {
                newRef = findNewRef(arg, oldField, newField);
                if (newRef == -2) {
                    return null;
                }
                newArgs.add(newRef);
            }

            List<Integer> newOrderArgs = new ArrayList<>();
            if (call instanceof GroupConcatAggregateCall) {
                for (int arg : ((GroupConcatAggregateCall) call).getOrderList()) {
                    newRef = findNewRef(arg, oldField, newField);
                    if (newRef == -2) {
                        return null;
                    }
                    newOrderArgs.add(newRef);
                }
            }

            newRef = findNewRef(call.filterArg, oldField, newField);
            if (newRef == -2) {
                return null;
            }

            if (call instanceof GroupConcatAggregateCall) {
                newAggCalls.add(((GroupConcatAggregateCall) call).copy(newArgs, newRef, newOrderArgs));
            } else {
                newAggCalls.add(call.copy(newArgs, newRef));
            }
        }
        ImmutableBitSet bitSet = ImmutableBitSet.of(newGroupSet);

        return agg.copy(agg.getTraitSet(), tableScan, agg.indicator, bitSet, ImmutableList.of(bitSet), newAggCalls);
    }

    private int findNewRef(int oldRef, List<RelDataTypeField> oldField, List<RelDataTypeField> newField) {
        if (oldRef == -1) {
            return -1;
        }
        RelDataTypeField field = oldField.get(oldRef);
        for (RelDataTypeField f : newField) {
            if (f.getType().equals(field.getType()) && f.getName().equalsIgnoreCase(field.getName())) {
                return f.getIndex();
            }
        }

        return -2;
    }
}
