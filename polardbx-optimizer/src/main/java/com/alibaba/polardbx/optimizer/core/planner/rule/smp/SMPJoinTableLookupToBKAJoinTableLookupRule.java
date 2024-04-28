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

import com.alibaba.polardbx.optimizer.core.planner.rule.JoinTableLookupTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.JoinTableLookupToBKAJoinTableLookupRule;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rex.RexNode;

public class SMPJoinTableLookupToBKAJoinTableLookupRule extends JoinTableLookupToBKAJoinTableLookupRule {

    public static final SMPJoinTableLookupToBKAJoinTableLookupRule
        TABLELOOKUP_NOT_RIGHT = new SMPJoinTableLookupToBKAJoinTableLookupRule(
        operand(LogicalJoin.class, null, JOIN_NOT_RIGHT,
            operand(RelSubset.class, any()),
            operand(LogicalTableLookup.class, null,
                JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                operand(LogicalIndexScan.class, none()))), "TABLELOOKUP:NOT_RIGHT");

    public static final SMPJoinTableLookupToBKAJoinTableLookupRule
        TABLELOOKUP_RIGHT = new SMPJoinTableLookupToBKAJoinTableLookupRule(
        operand(LogicalJoin.class, null, JOIN_RIGHT,
            operand(LogicalTableLookup.class, null,
                JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                operand(LogicalIndexScan.class, none())),
            operand(RelSubset.class, any())), "TABLELOOKUP:RIGHT");

    SMPJoinTableLookupToBKAJoinTableLookupRule(RelOptRuleOperand operand, String desc) {
        super(operand, "SMP_" + desc);
    }

    @Override
    protected void createBKAJoin(
        RelOptRuleCall call,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        LogicalIndexScan newLogicalIndexScan) {
        BKAJoin bkaJoin = BKAJoin.create(
            join.getTraitSet().replace(outConvention),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints()); // PK set in inner table for runtime optimize.
        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_BKA_JOIN);
        if (fixedCost != null) {
            bkaJoin.setFixedCost(fixedCost);
        }
        newLogicalIndexScan.setIsMGetEnabled(true);
        newLogicalIndexScan.setJoin(bkaJoin);
        call.transformTo(bkaJoin);
    }
}
