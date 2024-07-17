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

package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.SemiNLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

public class COLLogicalSemiJoinToSemiNLJoinRule extends LogicalSemiJoinToSemiNLJoinRule {

    public static final LogicalSemiJoinToSemiNLJoinRule INSTANCE =
        new COLLogicalSemiJoinToSemiNLJoinRule("INSTANCE");

    COLLogicalSemiJoinToSemiNLJoinRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected boolean pruneSemiNLJoin(LogicalSemiJoin join, RexNode newCondition) {
        // prefer hash join rather than nl join
        return canUseSemiHash(join, newCondition);
    }

    @Override
    protected void createSemiNLJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        RelNode left,
        RelNode right,
        RexNode newCondition) {
        if (canUseSemiHash(semiJoin, newCondition)) {
            return;
        }
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();
        CBOUtil.columnarBroadcastDistribution(semiJoin, left, right, implementationList);

        // default single semiJoin
        RelNode singletonLeft = convert(left, left.getTraitSet().replace(RelDistributions.SINGLETON));
        RelNode singletonRight = convert(right, right.getTraitSet().replace(RelDistributions.SINGLETON));
        implementationList.add(Pair.of(RelDistributions.SINGLETON, Pair.of(singletonLeft, singletonRight)));

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            SemiNLJoin semiNLJoin = SemiNLJoin.create(
                semiJoin.getTraitSet().replace(outConvention).replace(implementation.getKey()),
                implementation.getValue().getKey(),
                implementation.getValue().getValue(),
                newCondition,
                semiJoin);
            RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_NL_JOIN);
            if (fixedCost != null) {
                semiNLJoin.setFixedCost(fixedCost);
            }
            if (semiJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(
                    convert(semiNLJoin, semiNLJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(semiNLJoin);
            }
        }
    }
}
