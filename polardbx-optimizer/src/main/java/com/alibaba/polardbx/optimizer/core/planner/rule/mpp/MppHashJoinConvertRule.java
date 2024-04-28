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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class MppHashJoinConvertRule extends RelOptRule {

    public static final MppHashJoinConvertRule INSTANCE = new MppHashJoinConvertRule();

    MppHashJoinConvertRule() {
        super(operand(HashJoin.class, null, CBOUtil.DRDS_CONVENTION, any()), "MppHashJoinConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final HashJoin hashJoin = call.rel(0);

        RelNode left = hashJoin.getLeft();
        RelNode right = hashJoin.getRight();

        boolean preserveInputCollation = !hashJoin.getTraitSet().simplify().getCollation().isTop();
        final RelCollation leftCollation;
        final RelCollation rightCollation;
        if (preserveInputCollation) {
            leftCollation = left.getTraitSet().getCollation();
            rightCollation = right.getTraitSet().getCollation();
        } else {
            leftCollation = RelCollations.EMPTY;
            rightCollation = RelCollations.EMPTY;
        }

        RelTraitSet emptyTraitSet = hashJoin.getCluster().getPlanner().emptyTraitSet();
        left = convert(hashJoin.getLeft(),
            emptyTraitSet.replace(leftCollation).replace(MppConvention.INSTANCE));
        right = convert(hashJoin.getRight(),
            emptyTraitSet.replace(rightCollation).replace(MppConvention.INSTANCE));

        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();

        // Hash Shuffle
        JoinInfo joinInfo = JoinInfo.of(left, right, hashJoin.getEqualCondition());

        List<Pair<List<Integer>, List<Integer>>> keyPairList = new ArrayList<>();
        keyPairList.add(Pair.of(joinInfo.leftKeys, joinInfo.rightKeys));

        if (PlannerContext.getPlannerContext(hashJoin).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SHUFFLE_BY_PARTIAL_KEY) && joinInfo.leftKeys.size() > 1) {
            for (IntPair pair : joinInfo.pairs()) {
                keyPairList.add(Pair.of(ImmutableIntList.of(pair.source), ImmutableIntList.of(pair.target)));
            }
        }

        int leftFieldCount = left.getRowType().getFieldCount();
        int rightFiledCount = right.getRowType().getFieldCount();
        Mappings.TargetMapping mapping =
            Mappings.createShiftMapping(rightFiledCount, leftFieldCount, 0, rightFiledCount);

        for (Pair<List<Integer>, List<Integer>> keyPair : keyPairList) {
            RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
                hashJoin.getCluster().getTypeFactory(), hashJoin, keyPair.left, keyPair.right);
            RelNode hashLeft = RuleUtils.ensureKeyDataTypeDistribution(left, keyDataType, keyPair.left);
            RelNode hashRight = RuleUtils.ensureKeyDataTypeDistribution(right, keyDataType, keyPair.right);

            implementationList.add(Pair.of(hashLeft.getTraitSet().getDistribution(), Pair.of(hashLeft, hashRight)));
            implementationList
                .add(Pair.of(hashRight.getTraitSet().getDistribution().apply(mapping), Pair.of(hashLeft, hashRight)));
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            // Broadcast Shuffle
            switch (hashJoin.getJoinType()) {
            case LEFT: {
                RelNode broadcastRight =
                    convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
                implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(left, broadcastRight)));
                break;
            }
            case RIGHT: {
                RelNode broadcastLeft =
                    convert(left, left.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
                implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(broadcastLeft, right)));
                break;
            }
            case INNER: {
                RelNode broadcastLeft =
                    convert(left, left.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
                implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(broadcastLeft, right)));
                RelNode broadcastRight =
                    convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
                implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(left, broadcastRight)));
                break;
            }
            }
        }

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            HashJoin newHashJoin = hashJoin.copy(
                hashJoin.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                hashJoin.getCondition(),
                implementation.right.left,
                implementation.right.right,
                hashJoin.getJoinType(),
                hashJoin.isSemiJoinDone());

            if (hashJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(convert(newHashJoin, newHashJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(newHashJoin);
            }
        }
    }
}
