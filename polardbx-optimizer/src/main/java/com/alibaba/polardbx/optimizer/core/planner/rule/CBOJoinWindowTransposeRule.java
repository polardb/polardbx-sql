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

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class CBOJoinWindowTransposeRule extends RelOptRule {
//    public static final CBOJoinWindowTransposeRule INSTANCE = new CBOJoinWindowTransposeRule(operand(LogicalJoin.class,
//            operand(LogicalWindow.class, any())), RelFactories.LOGICAL_BUILDER, "INSTANCE");

    public static final CBOJoinWindowTransposeRule INSTANCE =
        new CBOJoinWindowTransposeRule(operand(LogicalJoin.class, operand(RelNode.class, any()),
            operand(LogicalFilter.class, operand(LogicalWindow.class, any()))), RelFactories.LOGICAL_BUILDER,
            "INSTANCE");

    /**
     * Creates an CBOJoinWindowTransposeRule.
     */
    public CBOJoinWindowTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String desc) {
        super(operand, relBuilderFactory, "CBOJoinWindowTransposeRule:" + desc);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        final LogicalFilter filter = call.rel(2);
        final LogicalWindow window = call.rel(3);
        RelNode other;
        boolean windowRight = true;
        if (join.getLeft() == window) {
            windowRight = false;
            other = join.getRight();
        } else {
            other = join.getLeft();
        }

        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = window.getCluster().getRexBuilder();
        RelNode output = transform(join, filter, window, other, relBuilder, rexBuilder, windowRight);

        if (output != null) {
            call.transformTo(output);
        }
    }

    // OUTER joins are supported for group by without aggregate functions
    // FULL OUTER JOIN is not supported since it could produce wrong result
    // due to bug (CALCITE-3012)
    private boolean isJoinSupported(final Join join, final Window window) {
        return join.getJoinType() == JoinRelType.INNER && window.groups.size() == 1
            && window.groups.get(0).aggCalls.size() == 1;
    }

    protected RelNode transform(final LogicalJoin join, LogicalFilter filter, final LogicalWindow window, RelNode other,
                                final RelBuilder relBuilder,
                                final RexBuilder rexBuilder, boolean windowRight) {
        if (!isJoinSupported(join, window)) {
            return null;
        }

        // Do the columns used by the join appear in the output of the aggregate?
        final ImmutableBitSet aggregateColumns = window.groups.get(0).keys;
        final RelMetadataQuery mq = window.getCluster().getMetadataQuery();
        final ImmutableBitSet keyColumns = keyColumns(aggregateColumns,
            mq.getPulledUpPredicates(join).pulledUpPredicates);
        final ImmutableBitSet joinColumns =
            RelOptUtil.InputFinder.bits(join.getCondition());
        final boolean allColumnsInAggregate =
            keyColumns.contains(joinColumns);
        final ImmutableBitSet belowAggregateColumns =
            aggregateColumns.union(joinColumns);

        // Split join condition
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        RexNode nonEquiConj =
            RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
                join.getCondition(), leftKeys, rightKeys, filterNulls);
        // If it contains non-equi join conditions, we bail out
        if (!nonEquiConj.isAlwaysTrue()) {
            return null;
        }

        // add key of other rel
        Set<ImmutableBitSet> addKeys = Sets.newHashSet();

        // make new window and join
        LogicalJoin newJoin = null;

        if (windowRight) {
            int windowIndex = join.getRowType().getFieldCount() - 1;
            for (RexInputRef rexInputRef : RexUtil.findAllIndex(join.getCondition())) {
                if (rexInputRef.getIndex() == windowIndex) {
                    return null;
                }
            }
            newJoin =
                (LogicalJoin) join.copy(join.getTraitSet(), Lists.newArrayList(join.getLeft(), window.getInput()));

            /**
             * uniq key judge
             */
            if (leftKeys.size() == 0) {
                return null;
            }
            Boolean isUniq = mq.areColumnsUnique(other, ImmutableBitSet.of(leftKeys));
            if (isUniq == null || !isUniq) {
                return null;
            }

            Window.Group oldGroup = window.groups.get(0);
            ImmutableBitSet newGroupBy = oldGroup.keys.shift(other.getRowType().getFieldCount());
            List<Integer> indexList = newGroupBy.asList();
            ImmutableList.Builder<Window.RexWinAggCall> newAggCallsBuilder = ImmutableList.builder();
            for (int i = 0; i < oldGroup.aggCalls.size(); i++) {

                Window.RexWinAggCall targetWinCall = oldGroup.aggCalls.get(i);
                List<RexNode> newOperands =
                    RexUtil.shift(targetWinCall.getOperands(), other.getRowType().getFieldCount());
                Window.RexWinAggCall newWinAggCall =
                    new Window.RexWinAggCall((SqlAggFunction) targetWinCall.getOperator(),
                        newJoin.getRowType(),
                        newOperands,
                        targetWinCall.ordinal,
                        targetWinCall.distinct);
                newAggCallsBuilder.add(newWinAggCall);
            }
            final List<RelFieldCollation> orderKeys = new ArrayList<>();
            for (RelFieldCollation relFieldCollation : oldGroup.orderKeys.getFieldCollations()) {
                final int index = relFieldCollation.getFieldIndex();
                orderKeys.add(
                    relFieldCollation.copy(index + other.getRowType().getFieldCount()));
            }
            Window.Group newGroup = new Window.Group(ImmutableBitSet.of(indexList),
                false,
                oldGroup.lowerBound,
                oldGroup.upperBound,
                RelCollations.of(orderKeys),
                newAggCallsBuilder.build());

            final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<Map.Entry<String, RelDataType>>(newJoin
                .getRowType()
                .getFieldList());

            fieldList.add(Pair.of(window.getRowType()
                    .getFieldList()
                    .get(window.getRowType().getFieldCount() - 1)
                    .getName(),
                window.getRowType().getFieldList().get(window.getRowType().getFieldCount() - 1).getType()));
            LogicalWindow newWindow = LogicalWindow.create(window.getTraitSet(),
                newJoin,
                Lists.newArrayList(),
                rexBuilder.getTypeFactory().createStructType(fieldList),
                ImmutableList.of(newGroup));
            return LogicalFilter.create(newWindow,
                RexUtil.shift(filter.getCondition(), other.getRowType().getFieldCount()));
        } else {
//            int windowIndex = window.getRowType().getFieldCount() -1;
//            for(RexInputRef rexInputRef:RexUtil.findAllIndex(join.getCondition())){
//                if(rexInputRef.getIndex()==windowIndex){
//                    return null;
//                }
//            }
//
//            Map<Integer, Integer> shiftJoinMap = Maps.newHashMap();
//            for (int i = join.getRowType().getFieldCount() - 1; i >= window.getRowType().getFieldCount(); i--) {
//                shiftJoinMap.put(i, i - 1);
//            }
//            newJoin = join.copy(join.getTraitSet(),
//                    RexUtil.shift(join.getCondition(), shiftJoinMap),
//                    window.getInput(),
//                    join.getLeft(),
//                    join.getJoinType(),
//                    join.isSemiJoinDone());
//
//            /**
//             * uniq key judge
//             */
//            boolean isUniq = mq.areColumnsUnique(other, ImmutableBitSet.of(rightKeys));
//            if (!isUniq) {
//                return null;
//            }
//
//            Window.Group oldGroup = window.groups.get(0);
//            ImmutableBitSet newGroupBy = oldGroup.keys;
//            List<Integer> indexList = newGroupBy.asList();
//
//            Window.RexWinAggCall targetWinCall = oldGroup.aggCalls.get(0);
//
//            Window.RexWinAggCall newWinaggCall = new Window.RexWinAggCall((SqlAggFunction) targetWinCall.getOperator(),
//                    newJoin.getRowType(),
//                    targetWinCall.getOperands(),
//                    targetWinCall.ordinal,
//                    targetWinCall.distinct);
//            Window.Group newGroup = new Window.Group(ImmutableBitSet.of(indexList),
//                    false,
//                    oldGroup.lowerBound,
//                    oldGroup.upperBound,
//                    oldGroup.orderKeys,
//                    ImmutableList.of(newWinaggCall));
//
//            final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<Map.Entry<String, RelDataType>>(newJoin
//                    .getRowType()
//                    .getFieldList());
//
//            fieldList.add(Pair.of(window.getRowType()
//                            .getFieldList()
//                            .get(window.getRowType().getFieldCount() - 1)
//                            .getName(),
//                    window.getRowType().getFieldList().get(window.getRowType().getFieldCount() - 1).getType()));
//            LogicalWindow newWindow = LogicalWindow.create(window.getTraitSet(),
//                    newJoin,
//                    Lists.newArrayList(),
//                    rexBuilder.getTypeFactory().createStructType(fieldList),
//                    ImmutableList.of(newGroup));
//            Object isForceReorder = PlannerContext.getPlannerContext(join)
//                    .getExtraCmds()
//                    .get(ConnectionProperties.WINDOW_FUNC_REORDER_JOIN);
//            if (isForceReorder != null && "true".equalsIgnoreCase(isForceReorder.toString())) {
//                newWindow.setFixedCost(newWindow.getCluster().getPlanner().getCostFactory().makeTinyCost());
//            }
//            return LogicalFilter.create(newWindow,
//                    RexUtil.shift(filter.getCondition(), other.getRowType().getFieldCount()));
            return null;
        }
    }

    /**
     * Computes the closure of a set of columns according to a given list of
     * constraints. Each 'x = y' constraint causes bit y to be set if bit x is
     * set, and vice versa.
     */
    private static ImmutableBitSet keyColumns(ImmutableBitSet aggregateColumns,
                                              ImmutableList<RexNode> predicates) {
        SortedMap<Integer, BitSet> equivalence = new TreeMap<>();
        for (RexNode predicate : predicates) {
            populateEquivalences(equivalence, predicate);
        }
        ImmutableBitSet keyColumns = aggregateColumns;
        for (Integer aggregateColumn : aggregateColumns) {
            final BitSet bitSet = equivalence.get(aggregateColumn);
            if (bitSet != null) {
                keyColumns = keyColumns.union(bitSet);
            }
        }
        return keyColumns;
    }

    private static void populateEquivalences(Map<Integer, BitSet> equivalence,
                                             RexNode predicate) {
        switch (predicate.getKind()) {
        case EQUALS:
            RexCall call = (RexCall) predicate;
            final List<RexNode> operands = call.getOperands();
            if (operands.get(0) instanceof RexInputRef) {
                final RexInputRef ref0 = (RexInputRef) operands.get(0);
                if (operands.get(1) instanceof RexInputRef) {
                    final RexInputRef ref1 = (RexInputRef) operands.get(1);
                    populateEquivalence(equivalence, ref0.getIndex(), ref1.getIndex());
                    populateEquivalence(equivalence, ref1.getIndex(), ref0.getIndex());
                }
            }
        }
    }

    private static void populateEquivalence(Map<Integer, BitSet> equivalence,
                                            int i0, int i1) {
        BitSet bitSet = equivalence.get(i0);
        if (bitSet == null) {
            bitSet = new BitSet();
            equivalence.put(i0, bitSet);
        }
        bitSet.set(i1);
    }

    /**
     * Creates a {@link SqlSplittableAggFunction.Registry}
     * that is a view of a list.
     */
    private static <E> SqlSplittableAggFunction.Registry<E> registry(
        final List<E> list) {
        return e -> {
            int i = list.indexOf(e);
            if (i < 0) {
                i = list.size();
                list.add(e);
            }
            return i;
        };
    }

}
