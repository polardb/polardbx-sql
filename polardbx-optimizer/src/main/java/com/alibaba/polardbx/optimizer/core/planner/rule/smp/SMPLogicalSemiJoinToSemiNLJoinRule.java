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

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiNLJoinRule;
import com.alibaba.polardbx.optimizer.core.rel.SemiNLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalSemiJoinToSemiNLJoinRule extends LogicalSemiJoinToSemiNLJoinRule {

    public static final LogicalSemiJoinToSemiNLJoinRule INSTANCE =
        new SMPLogicalSemiJoinToSemiNLJoinRule("INSTANCE");

    SMPLogicalSemiJoinToSemiNLJoinRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createSemiNLJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        RelNode left,
        RelNode right,
        RexNode newCondition) {
        SemiNLJoin semiNLJoin = SemiNLJoin.create(
            semiJoin.getTraitSet().replace(outConvention), left, right, newCondition, semiJoin);
        semiNLJoin.setAntiCondition(semiJoin.getAntiCondition());
        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_NL_JOIN);
        if (fixedCost != null) {
            semiNLJoin.setFixedCost(fixedCost);
        }
        call.transformTo(semiNLJoin);
    }
}
