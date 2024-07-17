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
import com.alibaba.polardbx.optimizer.core.rel.SemiSortMergeJoin;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class MppSemiSortMergeJoinConvertRule extends RelOptRule {

    public static final MppSemiSortMergeJoinConvertRule INSTANCE = new MppSemiSortMergeJoinConvertRule();

    MppSemiSortMergeJoinConvertRule() {
        super(operand(SemiSortMergeJoin.class, null, CBOUtil.DRDS_CONVENTION, any()),
            "MppSemiSortMergeJoinConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final SemiSortMergeJoin semiSortMergeJoin = call.rel(0);
        RelNode left = semiSortMergeJoin.getLeft();
        RelNode right = semiSortMergeJoin.getRight();

        // use input collation
        left = convert(left, left.getTraitSet().replace(MppConvention.INSTANCE));
        right = convert(right, right.getTraitSet().replace(MppConvention.INSTANCE));

        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();
        List<Pair<List<Integer>, List<Integer>>> keyPairList = new ArrayList<>();
        keyPairList.add(Pair.of(semiSortMergeJoin.getLeftColumns(), semiSortMergeJoin.getRightColumns()));

        if (PlannerContext.getPlannerContext(semiSortMergeJoin).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SHUFFLE_BY_PARTIAL_KEY)
            && semiSortMergeJoin.getJoinInfo().leftKeys.size() > 1) {
            for (IntPair pair : semiSortMergeJoin.getJoinInfo().pairs()) {
                keyPairList.add(Pair.of(ImmutableIntList.of(pair.source), ImmutableIntList.of(pair.target)));
            }
        }

        // Hash Shuffle
        for (Pair<List<Integer>, List<Integer>> keyPair : keyPairList) {
            RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
                semiSortMergeJoin.getCluster().getTypeFactory(), semiSortMergeJoin, keyPair.left, keyPair.right);
            RelNode hashLeft = RuleUtils.ensureKeyDataTypeDistribution(left, keyDataType, keyPair.left);
            RelNode hashRight = RuleUtils.ensureKeyDataTypeDistribution(right, keyDataType, keyPair.right);
            implementationList.add(Pair.of(hashLeft.getTraitSet().getDistribution(), Pair.of(hashLeft, hashRight)));
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            // Broadcast Shuffle
            RelNode broadcastRight =
                convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
            implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(left, broadcastRight)));
        }

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            SemiSortMergeJoin newSemiSortMergeJoin = semiSortMergeJoin.copy(
                semiSortMergeJoin.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                semiSortMergeJoin.getCondition(),
                implementation.right.left,
                implementation.right.right,
                semiSortMergeJoin.getJoinType(),
                semiSortMergeJoin.isSemiJoinDone());

            if (semiSortMergeJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE)
                == RelDistributions.SINGLETON) {
                call.transformTo(convert(newSemiSortMergeJoin,
                    newSemiSortMergeJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(newSemiSortMergeJoin);
            }
        }
    }
}



