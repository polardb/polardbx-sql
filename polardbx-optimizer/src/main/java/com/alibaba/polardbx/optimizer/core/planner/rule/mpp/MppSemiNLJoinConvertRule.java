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
import com.alibaba.polardbx.optimizer.core.rel.SemiNLJoin;
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
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class MppSemiNLJoinConvertRule extends RelOptRule {

    public static final MppSemiNLJoinConvertRule INSTANCE = new MppSemiNLJoinConvertRule();

    MppSemiNLJoinConvertRule() {
        super(operand(SemiNLJoin.class, null, CBOUtil.DRDS_CONVENTION, any()), "MppSemiNLJoinConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final SemiNLJoin semiNLJoin = call.rel(0);
        RelNode left = semiNLJoin.getLeft();
        RelNode right = semiNLJoin.getRight();

        boolean preserveInputCollation = !semiNLJoin.getTraitSet().simplify().getCollation().isTop();
        final RelCollation leftCollation;
        final RelCollation rightCollation;
        if (preserveInputCollation) {
            leftCollation = semiNLJoin.getLeft().getTraitSet().getCollation();
            rightCollation = semiNLJoin.getRight().getTraitSet().getCollation();
        } else {
            leftCollation = RelCollations.EMPTY;
            rightCollation = RelCollations.EMPTY;
        }

        RelTraitSet emptyTraitSet = semiNLJoin.getCluster().getPlanner().emptyTraitSet();
        left = convert(left, emptyTraitSet.replace(leftCollation).replace(MppConvention.INSTANCE));
        right = convert(right, emptyTraitSet.replace(rightCollation).replace(MppConvention.INSTANCE));

        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();

        // Singleton
        RelNode singletonLeft = convert(left, left.getTraitSet().replace(RelDistributions.SINGLETON));
        RelNode singletonRight = convert(right, right.getTraitSet().replace(RelDistributions.SINGLETON));
        implementationList.add(Pair.of(RelDistributions.SINGLETON, Pair.of(singletonLeft, singletonRight)));
        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            // Broadcast Shuffle
            RelNode broadcastRight =
                convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
            implementationList.add(Pair.of(RelDistributions.ANY, Pair.of(left, broadcastRight)));
        }

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            SemiNLJoin newSemiNLJoin = semiNLJoin.copy(
                semiNLJoin.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                semiNLJoin.getCondition(),
                implementation.right.left,
                implementation.right.right,
                semiNLJoin.getJoinType(),
                semiNLJoin.isSemiJoinDone());

            if (semiNLJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(convert(newSemiNLJoin,
                    newSemiNLJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(newSemiNLJoin);
            }
        }
    }
}
