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

package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalJoinToHashJoinRule extends LogicalJoinToHashJoinRule {

    public static final LogicalJoinToHashJoinRule INSTANCE = new SMPLogicalJoinToHashJoinRule("INSTANCE");
    public static final LogicalJoinToHashJoinRule OUTER_INSTANCE =
        new SMPLogicalJoinToHashJoinRule(true, "OUTER_INSTANCE");

    public SMPLogicalJoinToHashJoinRule(String desc) {
        super("SMP_" + desc);
    }

    SMPLogicalJoinToHashJoinRule(boolean outDriver, String desc) {
        super(outDriver, "SMP_" + desc);
    }

    @Override
    protected void createHashJoin(RelOptRuleCall call,
                                  LogicalJoin join,
                                  RelNode left,
                                  RelNode right,
                                  RexNode newCondition,
                                  CBOUtil.RexNodeHolder equalConditionHolder,
                                  CBOUtil.RexNodeHolder otherConditionHolder) {

        HashJoin hashJoin = HashJoin.create(
            join.getTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints(),
            equalConditionHolder.getRexNode(),
            otherConditionHolder.getRexNode(),
            outDriver);
        HintType cmdHashJoin = HintType.CMD_HASH_JOIN;
        if (outDriver) {
            cmdHashJoin = HintType.CMD_HASH_OUTER_JOIN;
        }
        RelOptCost fixedCost = CheckJoinHint.check(join, cmdHashJoin);
        if (fixedCost != null) {
            hashJoin.setFixedCost(fixedCost);
        }

        call.transformTo(hashJoin);
    }
}
