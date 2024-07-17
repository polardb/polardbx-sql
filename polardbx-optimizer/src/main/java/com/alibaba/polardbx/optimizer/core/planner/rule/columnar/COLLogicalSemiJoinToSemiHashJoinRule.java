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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class COLLogicalSemiJoinToSemiHashJoinRule extends LogicalSemiJoinToSemiHashJoinRule {

    public static final LogicalSemiJoinToSemiHashJoinRule INSTANCE =
        new COLLogicalSemiJoinToSemiHashJoinRule("INSTANCE");

    public static final LogicalSemiJoinToSemiHashJoinRule OUTER_INSTANCE =
        new COLLogicalSemiJoinToSemiHashJoinRule(true, "OUTER_INSTANCE");

    COLLogicalSemiJoinToSemiHashJoinRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    COLLogicalSemiJoinToSemiHashJoinRule(boolean outDriver, String desc) {
        super(outDriver, "COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createSemiHashJoin(RelOptRuleCall call,
                                      LogicalSemiJoin semiJoin,
                                      RelNode left,
                                      RelNode right,
                                      RexNode newCondition,
                                      CBOUtil.RexNodeHolder equalConditionHolder,
                                      CBOUtil.RexNodeHolder otherConditionHolder) {
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();

        JoinInfo joinInfo = JoinInfo.of(left, right, equalConditionHolder.getRexNode());
        List<Pair<List<Integer>, List<Integer>>> keyPairList = new ArrayList<>();

        for (IntPair pair : joinInfo.pairs()) {
            keyPairList.add(Pair.of(ImmutableIntList.of(pair.source), ImmutableIntList.of(pair.target)));
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_JOIN)) {
            CBOUtil.columnarHashDistribution(
                keyPairList,
                semiJoin,
                left,
                right,
                null,
                implementationList);
        }
        // implementationList may be empty, in this case use none partition wise join
        if (CollectionUtils.isEmpty(implementationList)) {
            RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
                semiJoin.getCluster().getTypeFactory(), semiJoin, joinInfo.leftKeys, joinInfo.rightKeys);
            RelNode hashLeft = RuleUtils.ensureKeyDataTypeDistribution(left, keyDataType, joinInfo.leftKeys);
            RelNode hashRight = RuleUtils.ensureKeyDataTypeDistribution(right, keyDataType, joinInfo.rightKeys);
            implementationList.add(Pair.of(hashLeft.getTraitSet().getDistribution(), Pair.of(hashLeft, hashRight)));
        }

        CBOUtil.columnarBroadcastDistribution(semiJoin, left, right, implementationList);

        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            SemiHashJoin semiHashJoin = SemiHashJoin.create(
                semiJoin.getTraitSet().replace(outConvention).replace(implementation.getKey()),
                implementation.getValue().getKey(),
                implementation.getValue().getValue(),
                newCondition,
                semiJoin,
                equalConditionHolder.getRexNode(),
                otherConditionHolder.getRexNode(),
                outDriver);

            RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_HASH_JOIN);
            if (fixedCost != null) {
                semiHashJoin.setFixedCost(fixedCost);
            }
            if (semiJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(
                    convert(semiHashJoin, semiHashJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(semiHashJoin);
            }
        }
    }
}
