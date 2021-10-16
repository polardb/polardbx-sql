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

import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

public class LogicalJoinToNLJoinRule extends ConverterRule {

    public static final LogicalJoinToNLJoinRule INSTANCE = new LogicalJoinToNLJoinRule("INSTANCE");

    LogicalJoinToNLJoinRule(String desc) {
        super(LogicalJoin.class, Convention.NONE, DrdsConvention.INSTANCE, "LogicalJoinToNLJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_NL_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalJoin join = (LogicalJoin) rel;

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getRight().getTraitSet().replace(DrdsConvention.INSTANCE);
        }
        final RelNode left;
        final RelNode right;
        left = convert(join.getLeft(), leftTraitSet);
        right = convert(join.getRight(), rightTraitSet);

        NLJoin nlJoin = NLJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
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
        return nlJoin;
    }
}
