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
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoying
 */
public class MppHashGroupJoinConvertRule extends RelOptRule {

    public static final MppHashGroupJoinConvertRule INSTANCE = new MppHashGroupJoinConvertRule();

    MppHashGroupJoinConvertRule() {
        super(operand(HashGroupJoin.class, null, CBOUtil.DRDS_CONVENTION, any()), "MppHashGroupJoinConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final HashGroupJoin hashGroupJoin = call.rel(0);
        RelTraitSet emptyTraitSet = hashGroupJoin.getCluster().getPlanner().emptyTraitSet();
        RelNode left = convert(hashGroupJoin.getLeft(), emptyTraitSet.replace(MppConvention.INSTANCE));
        RelNode right = convert(hashGroupJoin.getRight(), emptyTraitSet.replace(MppConvention.INSTANCE));

        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();

        JoinInfo joinInfo = JoinInfo.of(left, right, hashGroupJoin.getEqualCondition());
        RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
            hashGroupJoin.getCluster().getTypeFactory(), hashGroupJoin, joinInfo.leftKeys, joinInfo.rightKeys);
        // Hash Shuffle
        RelNode hashLeft = RuleUtils.ensureKeyDataTypeDistribution(left, keyDataType, joinInfo.leftKeys);
        RelNode hashRight = RuleUtils.ensureKeyDataTypeDistribution(right, keyDataType, joinInfo.rightKeys);
        implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(hashLeft, hashRight)));

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            // Broadcast Shuffle
            RelNode broadCostLeft = convert(left, left.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
            implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(broadCostLeft, right)));
            RelNode broadcastRight = convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
            implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(left, broadcastRight)));
        }

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            HashGroupJoin newHashGroupJoin = hashGroupJoin.copy(
                hashGroupJoin.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                hashGroupJoin.getCondition(),
                implementation.right.left,
                implementation.right.right,
                hashGroupJoin.getJoinType(),
                hashGroupJoin.isSemiJoinDone());
            call.transformTo(newHashGroupJoin);
        }
    }
}