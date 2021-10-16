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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.Map;

public class SemiJoinSemiJoinTransposeRule extends RelOptRule {

    public static final SemiJoinSemiJoinTransposeRule INSTANCE =
        new SemiJoinSemiJoinTransposeRule(RelFactories.LOGICAL_BUILDER);

    private static Map<Pair<JoinRelType, JoinRelType>, Pair<JoinRelType, JoinRelType>> lAsscomTable;

    static {
        lAsscomTable = new HashMap<>();
        lAsscomTable.put(Pair.of(JoinRelType.SEMI, JoinRelType.SEMI), Pair.of(JoinRelType.SEMI, JoinRelType.SEMI));
        lAsscomTable.put(Pair.of(JoinRelType.ANTI, JoinRelType.ANTI), Pair.of(JoinRelType.ANTI, JoinRelType.ANTI));
        lAsscomTable.put(Pair.of(JoinRelType.ANTI, JoinRelType.SEMI), Pair.of(JoinRelType.SEMI, JoinRelType.ANTI));
        lAsscomTable.put(Pair.of(JoinRelType.SEMI, JoinRelType.ANTI), Pair.of(JoinRelType.ANTI, JoinRelType.SEMI));
    }

    public SemiJoinSemiJoinTransposeRule(RelBuilderFactory relBuilderFactory) {
        super(
            operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
                operand(RelSubset.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            relBuilderFactory, "SemiJoinSemiJoinTransposeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin topJoin = call.rel(0);
        final LogicalSemiJoin bottomJoin = call.rel(1);
        final RelNode relC = call.rel(2);
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        // becomes
        //
        //        newTopJoin
        //        /        \
        //   newBottomJoin  B
        //    /    \
        //   A      C

        Pair<JoinRelType, JoinRelType> newJoinTypePair =
            lAsscomTable.get(Pair.of(bottomJoin.getJoinType(), topJoin.getJoinType()));
        if (newJoinTypePair == null) {
            return;
        }

        final Join newBottomJoin =
            topJoin.copy(topJoin.getTraitSet(), topJoin.getCondition(), relA,
                relC, newJoinTypePair.getKey(), topJoin.isSemiJoinDone());

        final Join newTopJoin =
            bottomJoin.copy(bottomJoin.getTraitSet(), bottomJoin.getCondition(), newBottomJoin,
                relB, newJoinTypePair.getValue(), bottomJoin.isSemiJoinDone());

        final RelBuilder relBuilder = call.builder();
        relBuilder.push(newTopJoin);
        call.transformTo(relBuilder.build());
    }
}
