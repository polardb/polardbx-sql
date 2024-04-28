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

import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;

/**
 * support push the RFBuilder into Join' build side.
 */
public class RFBuilderJoinTransposeRule extends RelOptRule {

    public static final RFBuilderJoinTransposeRule INSTANCE =
        new RFBuilderJoinTransposeRule();

    RFBuilderJoinTransposeRule() {
        super(operand(RuntimeFilterBuilder.class,
            operand(HashJoin.class, any())), "RFBuilderJoinTransposeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(1);
        boolean insertRf = false;
        if (join instanceof HashJoin) {
            insertRf = ((HashJoin) join).isRuntimeFilterPushedDown();
        } else if (join instanceof SemiHashJoin) {
            insertRf = ((SemiHashJoin) join).isRuntimeFilterPushedDown();
        }
        return insertRf;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

        final RuntimeFilterBuilder filter = call.rel(0);
        final HashJoin hashJoin = call.rel(1);

        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());
        JoinInfo joinInfo = JoinInfo.of(hashJoin.getLeft(), hashJoin.getRight(), hashJoin.getEqualCondition());

        ImmutableBitSet.Builder currentJoinRfKeys = ImmutableBitSet.builder();
        int[] adjustments = new int[hashJoin.getRowType().getFieldCount()];
        if (hashJoin.getJoinType() != JoinRelType.RIGHT) {
            ImmutableIntList buildKeys = joinInfo.rightKeys;
            int offset = hashJoin.getLeft().getRowType().getFieldCount();
            for (int i = 0; i < buildKeys.size(); i++) {
                currentJoinRfKeys.set(buildKeys.get(i) + offset);
                adjustments[buildKeys.get(i) + offset] = -offset;
            }

        } else {
            ImmutableIntList buildKeys = joinInfo.leftKeys;
            currentJoinRfKeys.addAll(buildKeys);
            for (int i = 0; i < buildKeys.size(); i++) {
                adjustments[buildKeys.get(i)] = 0;
                currentJoinRfKeys.set(buildKeys.get(i));
            }
        }
        ImmutableBitSet buildArray = currentJoinRfKeys.build();
        List<RexNode> pushedConditions = new ArrayList<>();
        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

        final List<RexNode> remainingConditions = Lists.newArrayList();

        for (RexNode condition : conditions) {
            ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
            if (buildArray.contains(rCols)) {
                pushedConditions.add(condition.accept(
                    new RelOptUtil.RexInputConverter(
                        rexBuilder,
                        hashJoin.getRowType().getFieldList(),
                        hashJoin.getInner().getRowType().getFieldList(),
                        adjustments)));
            } else {
                remainingConditions.add(condition);
            }
        }

        if (pushedConditions.size() > 0) {
            RelNode inputOfHashJoin = hashJoin.getInner();
            RuntimeFilterUtil.updateBuildFunctionNdv(inputOfHashJoin, pushedConditions);

            RuntimeFilterBuilder newFilterBuilder = RuntimeFilterBuilder.create(
                inputOfHashJoin, RexUtil.composeConjunction(rexBuilder, pushedConditions, true));
            RelNode ret = null;
            if (hashJoin.getJoinType() != JoinRelType.RIGHT) {
                ret = hashJoin.copy(hashJoin.getTraitSet(), ImmutableList.of(hashJoin.getLeft(), newFilterBuilder));
            } else {
                ret = hashJoin.copy(hashJoin.getTraitSet(), ImmutableList.of(newFilterBuilder, hashJoin.getRight()));
            }
            if (remainingConditions.size() > 0) {
                ret = RuntimeFilterBuilder.create(
                    ret, RexUtil.composeConjunction(rexBuilder, remainingConditions, true));
            }
            call.transformTo(ret);
        }
    }
}
