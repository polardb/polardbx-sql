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
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.SortMergeJoin;
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
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class MppSortMergeJoinConvertRule extends RelOptRule {

    public static final MppSortMergeJoinConvertRule INSTANCE = new MppSortMergeJoinConvertRule();

    MppSortMergeJoinConvertRule() {
        super(operand(SortMergeJoin.class, null, CBOUtil.DRDS_CONVENTION, any()), "MppSortMergeJoinConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final SortMergeJoin sortMergeJoin = call.rel(0);
        RelNode left = sortMergeJoin.getLeft();
        RelNode right = sortMergeJoin.getRight();

        // use input collation
        left = convert(left, left.getTraitSet().replace(MppConvention.INSTANCE));
        right = convert(right, right.getTraitSet().replace(MppConvention.INSTANCE));
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();
        List<Pair<List<Integer>, List<Integer>>> keyPairList = new ArrayList<>();
        keyPairList.add(Pair.of(sortMergeJoin.getLeftColumns(), sortMergeJoin.getRightColumns()));

        if (PlannerContext.getPlannerContext(sortMergeJoin).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SHUFFLE_BY_PARTIAL_KEY)
            && sortMergeJoin.getJoinInfo().leftKeys.size() > 1) {
            for (IntPair pair : sortMergeJoin.getJoinInfo().pairs()) {
                keyPairList.add(Pair.of(ImmutableIntList.of(pair.source), ImmutableIntList.of(pair.target)));
            }
        }

        // Hash Shuffle
        for (Pair<List<Integer>, List<Integer>> keyPair : keyPairList) {
            RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
                sortMergeJoin.getCluster().getTypeFactory(), sortMergeJoin, keyPair.left,
                keyPair.right);
            RelNode hashLeft = RuleUtils.ensureKeyDataTypeDistribution(left, keyDataType, keyPair.left);
            RelNode hashRight = RuleUtils.ensureKeyDataTypeDistribution(right, keyDataType,
                keyPair.right);
            implementationList.add(Pair.of(hashLeft.getTraitSet().getDistribution(), Pair.of(hashLeft, hashRight)));
            int leftFieldCount = left.getRowType().getFieldCount();
            int rightFiledCount = right.getRowType().getFieldCount();
            Mappings.TargetMapping mapping =
                Mappings.createShiftMapping(rightFiledCount, leftFieldCount, 0, rightFiledCount);
            implementationList
                .add(Pair.of(hashRight.getTraitSet().getDistribution().apply(mapping), Pair.of(hashLeft, hashRight)));
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            // Broadcast Shuffle
            RelNode broadCostLeft = convert(left, left.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
            implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(broadCostLeft, right)));
            RelNode broadcastRight =
                convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
            implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(left, broadcastRight)));
        }

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            SortMergeJoin newSortMergeJoin = sortMergeJoin.copy(
                // remove newSortMergeJoin Distribution
                sortMergeJoin.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                sortMergeJoin.getCondition(),
                implementation.right.left,
                implementation.right.right,
                sortMergeJoin.getJoinType(),
                sortMergeJoin.isSemiJoinDone());

            if (sortMergeJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(convert(newSortMergeJoin,
                    newSortMergeJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(newSortMergeJoin);
            }
        }
    }
}

