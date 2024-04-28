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

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToMaterializedSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

public class SMPLogicalSemiJoinToMaterializedSemiJoinRule extends LogicalSemiJoinToMaterializedSemiJoinRule {

    public static final LogicalSemiJoinToMaterializedSemiJoinRule INSTANCE =
        new SMPLogicalSemiJoinToMaterializedSemiJoinRule(
            operand(LogicalSemiJoin.class,
                operand(LogicalView.class, any()),
                operand(RelSubset.class, any())), "INSTANCE");

    SMPLogicalSemiJoinToMaterializedSemiJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "SMP_" + desc);
    }

    protected void createMaterializedSemiJoin(
        RelOptRuleCall call,
        LogicalSemiJoin semiJoin,
        LogicalView left,
        RelNode right,
        RexNode newCondition,
        boolean distinctInput) {
        MaterializedSemiJoin materializedSemiJoin = MaterializedSemiJoin.create(
            semiJoin.getTraitSet().replace(outConvention), left, right, newCondition, semiJoin,
            distinctInput);

        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_MATERIALIZED_SEMI_JOIN);
        if (fixedCost != null) {
            materializedSemiJoin.setFixedCost(fixedCost);
        }
        left.setIsMGetEnabled(true);
        left.setJoin(materializedSemiJoin);
        call.transformTo(materializedSemiJoin);
    }
}
