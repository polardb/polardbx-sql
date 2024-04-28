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

package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public abstract class AggJoinToToHashGroupJoinRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public AggJoinToToHashGroupJoinRule(String desc) {
        super(operand(LogicalAggregate.class, operand(LogicalJoin.class, null,
                RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, none())),
            "AggJoinToToHashGroupJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(call).getParamManager();
        return paramManager.getBoolean(ConnectionParams.ENABLE_CBO_GROUP_JOIN);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = (LogicalAggregate) call.rels[0];
        LogicalJoin join = (LogicalJoin) call.rels[1];
        final JoinRelType joinType = join.getJoinType();
        switch (joinType) {
        case LEFT:
        case INNER:
        case RIGHT:
            break;
        default:
            return;
        }
        CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());
        if (!CBOUtil.checkHashJoinCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            equalConditionHolder, otherConditionHolder)) {
            return;
        }

        final RelNode left = convert(join.getLeft(), join.getLeft().getTraitSet().replace(outConvention));
        final RelNode right = convert(join.getRight(), join.getRight().getTraitSet().replace(outConvention));
        boolean canGroupJoin = canGroupJoin(agg, join, equalConditionHolder);
        if (!canGroupJoin) {
            return;
        }

        createHashGroupJoin(
            call,
            agg,
            join,
            left,
            right,
            newCondition,
            equalConditionHolder,
            otherConditionHolder);
    }

    protected abstract void createHashGroupJoin(
        RelOptRuleCall call,
        LogicalAggregate agg,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        CBOUtil.RexNodeHolder equalConditionHolder,
        CBOUtil.RexNodeHolder otherConditionHolder);

    private boolean canGroupJoin(LogicalAggregate agg, LogicalJoin join, CBOUtil.RexNodeHolder equalConditionHolder) {

        final ImmutableBitSet groupSet = agg.getGroupSet();
        final List<Pair<Integer, Integer>> simpleCondition =
            RexUtils.getSimpleCondition(join, equalConditionHolder.getRexNode());

        if (simpleCondition == null) {
            return false;
        }

        if (simpleCondition.isEmpty()) {
            return false;
        }

        final ImmutableBitSet.Builder joinLeftSetBuilder = ImmutableBitSet.builder();
        final ImmutableBitSet.Builder joinRightSetBuilder = ImmutableBitSet.builder();

        simpleCondition.stream().forEach(t -> {
            joinLeftSetBuilder.set(t.getKey());
            joinRightSetBuilder.set(t.getValue());
        });
        //init compare information
        final ImmutableBitSet joinLeftSet = joinLeftSetBuilder.build();
        final ImmutableBitSet joinRightSet = joinRightSetBuilder.build();
        final ImmutableBitSet.Builder aggCallIndexes = ImmutableBitSet.builder();
        final List<AggregateCall> aggCallList = agg.getAggCallList();
        aggCallList.stream().forEach(t -> t.getArgList().forEach(m -> aggCallIndexes.set(m.intValue())));
        ImmutableBitSet joinCompareSet = joinLeftSet;
        RelNode joinFindUniqueSide = join.getLeft();
        //1. 判断agg group set 是否等于 Join condition left
        //for right
        if (join.getJoinType() == JoinRelType.RIGHT) {
            joinCompareSet = joinRightSet;
            joinFindUniqueSide = join.getRight();
        }

        if (!joinCompareSet.equals(groupSet)) {
            return false;
        }
        //2. 如果非right join，判断 agg call list是否都为 join 右表字段，否则判断是否为左表字段
        final ImmutableBitSet immutableBitSet = aggCallIndexes.build();
        for (int i = immutableBitSet.nextSetBit(0); i >= 0; i = immutableBitSet.nextSetBit(i + 1)) {
            final boolean isRightSide = i >= join.getLeft().getRowType().getFieldCount();
            switch (join.getJoinType()) {
            case RIGHT:
                if (isRightSide) {
                    return false;
                }
                break;
            case LEFT:
            case INNER:
                if (!isRightSide) {
                    return false;
                }
                break;
            default:
                return false;
            }
        }
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        if (!checkCompare(builder, joinCompareSet, join.getLeft().getRowType().getFieldCount(), join.getJoinType())) {
            return false;
        }
        //check 过后重置index
        joinCompareSet = builder.build();
        //3. 判断是否为全局唯一键
        final RelMetadataQuery mq = agg.getCluster().getMetadataQuery();
        final Boolean areColumnsUnique = mq.areColumnsUnique(joinFindUniqueSide, joinCompareSet);
        if (areColumnsUnique != null && !areColumnsUnique) {
            return false;
        }
        return true;
    }

    private boolean checkCompare(final ImmutableBitSet.Builder joinSetBuilder, ImmutableBitSet bitSet,
                                 int joinLeftLength, JoinRelType joinRelType) {
        boolean needSideIsLeft = joinRelType != JoinRelType.RIGHT;
        for (int f = 0, i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1), f++) {
            if (i < joinLeftLength && !needSideIsLeft) {
                return false;
            } else if (i >= joinLeftLength && needSideIsLeft) {
                return false;
            }
            if (!needSideIsLeft) {
                joinSetBuilder.set(i - joinLeftLength);
            } else {
                joinSetBuilder.set(i);
            }
        }
        return true;
    }

}
