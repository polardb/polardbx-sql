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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter;

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * support push the RF into Join' probe & build.
 */
public class RFilterJoinTransposeRule extends RelOptRule {

    public static final RFilterJoinTransposeRule HASHJOIN_INSTANCE =
        new RFilterJoinTransposeRule(HashJoin.class, "RFilterHashJoinTransposeRule");

    public static final RFilterJoinTransposeRule SEMI_HASHJOIN_INSTANCE =
        new RFilterJoinTransposeRule(SemiHashJoin.class, "RFilterSemiJoinTransposeRule");

    RFilterJoinTransposeRule(Class<? extends Join> joinClass, String desc) {
        super(operand(LogicalFilter.class,
            operand(joinClass, any())), desc);
    }

    private static List<RexCall> findRuntimeFilters(RexNode condition) {
        List<RexCall> rfs = new ArrayList<>();
        List<RexNode> conditions = RelOptUtil.conjunctions(condition);
        for (RexNode rexNode : conditions) {
            if (rexNode instanceof RexCall && (((RexCall) rexNode).getOperator() instanceof SqlRuntimeFilterFunction)) {
                rfs.add((RexCall) rexNode);
            }
        }
        return rfs;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        Join join = call.rel(1);
        return (join.getJoinType() == JoinRelType.INNER) && (findRuntimeFilters(filter.getCondition()).size() > 0);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final Join join = call.rel(1);

        final List<RexNode> pushedProbeConditions = Lists.newArrayList();
        final List<RexNode> pushedBuildConditions = Lists.newArrayList();
        final List<RexNode> remainingConditions = Lists.newArrayList();

        RelNode buildNode = join.getInner();
        RelNode probeNode = join.getOuter();
        if (join instanceof HashJoin) {
            buildNode = ((HashJoin) join).getBuildNode();
            probeNode = ((HashJoin) join).getProbeNode();
        }
        final JoinChildInfo buildChildInfo = new JoinChildInfo(join, buildNode);
        final JoinChildInfo probeChildInfo = new JoinChildInfo(join, probeNode);

        List<RexNode> filterConditions = RelOptUtil.conjunctions(filter.getCondition());

        int[] adjustments = new int[join.getRowType().getFieldCount()];
        int offset = join.getLeft().getRowType().getFieldCount();
        for (int i = 0; i < join.getLeft().getRowType().getFieldCount(); i++) {
            adjustments[i] = 0;
        }
        for (int i = 0; i < join.getRight().getRowType().getFieldCount(); i++) {
            adjustments[i + offset] = -offset;
        }

        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        boolean supportPushBuild = PlannerContext.getPlannerContext(call).getParamManager().getBoolean(
            ConnectionParams.ENABLE_RUNTIME_FILTER_INTO_BUILD_SIDE);
        Boolean[] equalKeysTypeEquals = JoinChildInfo.compareEqualKeysTypeEquals(buildChildInfo, probeChildInfo);

        RelOptUtil.RexInputConverter inputConverter = new RelOptUtil.RexInputConverter(rexBuilder,
            join.getRowType().getFieldList(), join.getLeft().getRowType().getFieldList(),
            join.getRight().getRowType().getFieldList(), adjustments);

        for (RexNode condition : filterConditions) {
            if (probeChildInfo.canPush(condition)) {
                pushedProbeConditions.add(condition.accept(inputConverter));

                if (supportPushBuild) {
                    RexCall pushedBuildCondition = buildChildInfo
                        .tryPushWhenOtherChildCanPush(condition, probeChildInfo, equalKeysTypeEquals, rexBuilder);
                    if (pushedBuildCondition != null) {
                        pushedBuildConditions.add(pushedBuildCondition);
                    }
                }
            } else if (supportPushBuild && buildChildInfo.canPush(condition)) {
                pushedBuildConditions.add(condition.accept(inputConverter));

                RexCall pushedProbeCondition = probeChildInfo
                    .tryPushWhenOtherChildCanPush(condition, buildChildInfo, equalKeysTypeEquals, rexBuilder);
                if (pushedProbeCondition != null) {
                    pushedProbeConditions.add(pushedProbeCondition);
                }
            } else {
                remainingConditions.add(condition);
            }
        }

        if (pushedProbeConditions.size() > 0 || pushedBuildConditions.size() > 0) {
            RelNode newBuildNode = buildChildInfo.child;
            if (pushedBuildConditions.size() > 0) {
                newBuildNode = LogicalFilter.create(
                    buildChildInfo.child, RexUtil.composeConjunction(rexBuilder, pushedBuildConditions, true));
            }

            RelNode newProbeNode = probeChildInfo.child;
            if (pushedProbeConditions.size() > 0) {
                newProbeNode = LogicalFilter.create(
                    probeChildInfo.child, RexUtil.composeConjunction(rexBuilder, pushedProbeConditions, true));
            }

            RelNode ret;
            if (buildChildInfo.child == join.getLeft()) {
                ret = join.copy(join.getTraitSet(), ImmutableList.of(newBuildNode, newProbeNode));
            } else {
                ret = join.copy(join.getTraitSet(), ImmutableList.of(newProbeNode, newBuildNode));
            }

            if (remainingConditions.size() > 0) {
                ret = LogicalFilter.create(
                    ret, RexUtil.composeConjunction(rexBuilder, remainingConditions, true));
            }
            call.transformTo(ret);
        }
    }

    private static class JoinChildInfo {
        public final Join join;
        public final RelNode child;
        public final int indexOffset; // Offset of field in join node to child node
        public final ImmutableBitSet bitset;
        public final ImmutableIntList equalsKeys; // index in child node

        public JoinChildInfo(Join join, RelNode child) {
            this.join = join;
            this.child = child;
            this.indexOffset = (child == join.getLeft()) ? 0 : join.getLeft().getRowType().getFieldCount();
            this.bitset = (child == join.getLeft()) ?
                ImmutableBitSet.range(0, getFieldCount(child)) :
                ImmutableBitSet.range(getFieldCount(join.getLeft()),
                    getFieldCount(join.getLeft()) + getFieldCount(join.getRight()));
            this.equalsKeys = findEqualKeys(join, child);
        }

        public static int getFieldCount(RelNode relNode) {
            return relNode.getRowType().getFieldCount();
        }

        private static ImmutableIntList findEqualKeys(Join join, RelNode child) {
            CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
            CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();
            CBOUtil.checkHashJoinCondition(join, join.getCondition(), join.getLeft().getRowType().getFieldCount(),
                equalConditionHolder, otherConditionHolder);
            RexNode equalCondition = equalConditionHolder.rexNode;
            JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), equalCondition);
            if (child == join.getLeft()) {
                return joinInfo.leftKeys;
            } else {
                return joinInfo.rightKeys;
            }
        }

        public static Boolean[] compareEqualKeysTypeEquals(JoinChildInfo child1, JoinChildInfo child2) {
            return Streams.zip(
                child1.equalsKeys.stream().map(child1::getDataTypeOfField),
                child2.equalsKeys.stream().map(child2::getDataTypeOfField),
                Object::equals)
                .toArray(Boolean[]::new);
        }

        public boolean canPush(RexNode condition) {
            if (condition instanceof RexCall && (((RexCall) condition)
                .getOperator() instanceof SqlRuntimeFilterFunction)) {
                ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
                return bitset.contains(rCols);
            } else {
                return false;
            }
        }

        public RexCall tryPushWhenOtherChildCanPush(RexNode condition, JoinChildInfo other,
                                                    Boolean[] equalKeysTypeEquals, RexBuilder rexBuilder) {
            if (condition instanceof RexCall && (((RexCall) condition)
                .getOperator() instanceof SqlRuntimeFilterFunction)) {
                List<RexNode> operands = ((RexCall) condition).getOperands();

                boolean canPushToCurrent = operands.stream()
                    .mapToInt(node -> ((RexSlot) node).getIndex())
                    .map(indexInJoinNode -> indexInJoinNode - other.indexOffset)
                    .map(other.equalsKeys::indexOf)
                    .allMatch(indexInOtherEqualKey ->
                        (indexInOtherEqualKey != -1) && equalKeysTypeEquals[indexInOtherEqualKey]);

                if (canPushToCurrent) {
                    List<RexNode> inputs = operands.stream()
                        .mapToInt(node -> ((RexSlot) node).getIndex())
                        .map(indexInJoinNode -> indexInJoinNode - other.indexOffset)
                        .map(other.equalsKeys::indexOf)
                        .map(equalsKeys::get)
                        .mapToObj(indexInThisNode -> rexBuilder.makeInputRef(child, indexInThisNode))
                        .collect(Collectors.toList());

                    return ((RexCall) condition).clone(child.getRowType(), inputs);
                }
            }
            return null;
        }

        public RelNode getOtherChild() {
            return (child == join.getLeft()) ? join.getRight() : join.getLeft();
        }

        public RelDataType getDataTypeOfField(int idx) {
            return child.getRowType().getFieldList().get(idx).getType();
        }
    }
}
