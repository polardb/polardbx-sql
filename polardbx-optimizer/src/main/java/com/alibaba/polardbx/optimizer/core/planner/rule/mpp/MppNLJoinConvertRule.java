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
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
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

/**
 * @author dylan
 */
public class MppNLJoinConvertRule extends RelOptRule {

    public static final MppNLJoinConvertRule INSTANCE = new MppNLJoinConvertRule();

    MppNLJoinConvertRule() {
        super(operand(NLJoin.class, null, CBOUtil.DRDS_CONVENTION, any()), "MppNLJoinConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        NLJoin nlJoin = call.rel(0);
        RelNode left = nlJoin.getLeft();
        RelNode right = nlJoin.getRight();

        boolean preserveInputCollation = !nlJoin.getTraitSet().simplify().getCollation().isTop();
        final RelCollation leftCollation;
        final RelCollation rightCollation;
        if (preserveInputCollation) {
            leftCollation = nlJoin.getLeft().getTraitSet().getCollation();
            rightCollation = nlJoin.getRight().getTraitSet().getCollation();
        } else {
            leftCollation = RelCollations.EMPTY;
            rightCollation = RelCollations.EMPTY;
        }

        RelTraitSet emptyTraitSet = nlJoin.getCluster().getPlanner().emptyTraitSet();
        left = convert(left, emptyTraitSet.replace(leftCollation).replace(MppConvention.INSTANCE));
        right = convert(right, emptyTraitSet.replace(rightCollation).replace(MppConvention.INSTANCE));

        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();

        RelNode singletonLeft = convert(left, left.getTraitSet().replace(RelDistributions.SINGLETON));
        RelNode singletonRight = convert(right, right.getTraitSet().replace(RelDistributions.SINGLETON));
        implementationList.add(Pair.of(RelDistributions.SINGLETON, Pair.of(singletonLeft, singletonRight)));

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            // Broadcast Shuffle
            switch (nlJoin.getJoinType()) {
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
            NLJoin newNLJoin = nlJoin.copy(
                nlJoin.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                nlJoin.getCondition(),
                implementation.right.left,
                implementation.right.right,
                nlJoin.getJoinType(),
                nlJoin.isSemiJoinDone());

            if (nlJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(convert(newNLJoin, newNLJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(newNLJoin);
            }
        }

    }
}

