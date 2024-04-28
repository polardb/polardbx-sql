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

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiBKAJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

public class COLLogicalSemiJoinToSemiBKAJoinRule extends LogicalSemiJoinToSemiBKAJoinRule {

    public static final LogicalSemiJoinToSemiBKAJoinRule INSTANCE = new COLLogicalSemiJoinToSemiBKAJoinRule(
        operand(LogicalSemiJoin.class,
            operand(RelSubset.class, any()),
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())), "INSTANCE");

    COLLogicalSemiJoinToSemiBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createSemiBKAJoin(
        RelOptRuleCall call,
        LogicalSemiJoin join,
        RelNode left,
        LogicalView right,
        RexNode newCondition) {
        SemiBKAJoin bkaJoin = SemiBKAJoin.create(
            join.getTraitSet().replace(outConvention).replace(RelDistributions.ANY),
            left,
            right,
            newCondition,
            join.getVariablesSet(),
            join.getJoinType(),
            join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()),
            join.getHints(), join);

        RelOptCost fixedCost = CheckJoinHint.check(join, HintType.CMD_SEMI_BKA_JOIN);
        if (fixedCost != null) {
            bkaJoin.setFixedCost(fixedCost);
        }
        right.setIsMGetEnabled(true);
        right.setJoin(bkaJoin);
        RelUtils.changeRowType(bkaJoin, join.getRowType());
        if (join.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
            call.transformTo(convert(bkaJoin, bkaJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
        } else {
            call.transformTo(bkaJoin);
        }

    }
}
