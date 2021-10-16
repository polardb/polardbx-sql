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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public class LogicalSemiJoinToSemiHashJoinRule extends ConverterRule {

    public static final LogicalSemiJoinToSemiHashJoinRule INSTANCE = new LogicalSemiJoinToSemiHashJoinRule("INSTANCE");

    LogicalSemiJoinToSemiHashJoinRule(String desc) {
        super(LogicalSemiJoin.class, Convention.NONE, DrdsConvention.INSTANCE,
            "LogicalSemiJoinToSemiHashJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SEMI_HASH_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalSemiJoin semiJoin = (LogicalSemiJoin) rel;

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(semiJoin.getCondition(), semiJoin.getCluster().getRexBuilder());

        CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();
        if (!CBOUtil.checkHashJoinCondition(semiJoin,
            newCondition,
            semiJoin.getLeft().getRowType().getFieldCount(),
            equalConditionHolder,
            otherConditionHolder)) {
            return null;
        }

        if (semiJoin.getJoinType() == JoinRelType.ANTI
            && semiJoin.getOperands() != null && !semiJoin.getOperands().isEmpty()) {
            // If this node is an Anti-Join without operands ('NOT IN')
            if (!newCondition.isA(SqlKind.EQUALS)) {
                // ... and contains multiple equi-conditions
                return null; // reject!
            }
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(semiJoin)) {
            leftTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
        } else {
            leftTraitSet = semiJoin.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = semiJoin.getRight().getTraitSet().replace(DrdsConvention.INSTANCE);
        }

        final RelNode left = convert(semiJoin.getLeft(), leftTraitSet);
        final RelNode right = convert(semiJoin.getRight(), rightTraitSet);

        switch (semiJoin.getJoinType()) {
        case SEMI:
        case ANTI:
        case LEFT:
        case INNER:
            SemiHashJoin semiHashJoin = SemiHashJoin.create(
                semiJoin.getTraitSet().replace(DrdsConvention.INSTANCE),
                left,
                right,
                newCondition,
                semiJoin,
                equalConditionHolder.getRexNode(),
                otherConditionHolder.getRexNode());
            RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_HASH_JOIN);
            if (fixedCost != null) {
                semiHashJoin.setFixedCost(fixedCost);
            }
            return semiHashJoin;
        default:
            // not support
            return null;
        }
    }
}
