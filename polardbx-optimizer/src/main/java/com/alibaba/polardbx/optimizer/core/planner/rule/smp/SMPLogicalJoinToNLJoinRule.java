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

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalJoinToNLJoinRule extends LogicalJoinToNLJoinRule {
    public static final LogicalJoinToNLJoinRule INSTANCE = new SMPLogicalJoinToNLJoinRule("INSTANCE");

    SMPLogicalJoinToNLJoinRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createNLJoin(RelOptRuleCall call,
                                LogicalJoin join,
                                RelNode left,
                                RelNode right,
                                RexNode newCondition) {
        NLJoin nlJoin = NLJoin.create(
            join.getTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints());
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_NL_JOIN);
        if (fixedCost != null) {
            nlJoin.setFixedCost(fixedCost);
        }
        call.transformTo(nlJoin);
    }
}
