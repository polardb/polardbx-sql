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

package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

public abstract class LogicalJoinToHashJoinRule extends RelOptRule {
    protected boolean outDriver = false;

    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected LogicalJoinToHashJoinRule(String desc) {
        super(operand(LogicalJoin.class, Convention.NONE, any()), "LogicalJoinToHashJoinRule:" + desc);
    }

    protected LogicalJoinToHashJoinRule(boolean outDriver, String desc) {
        super(operand(LogicalJoin.class, Convention.NONE, any()), "LogicalJoinToHashJoinRule:" + desc);
        this.outDriver = outDriver;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_HASH_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalJoin join = call.rel(0);
        if (outDriver) {
            if (join.getJoinType() != JoinRelType.LEFT && join.getJoinType() != JoinRelType.RIGHT) {
                return false;
            }
        }
        return enable(PlannerContext.getPlannerContext(call));
    }

    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);

        final LogicalJoin join = (LogicalJoin) rel;

        CBOUtil.RexNodeHolder equalConditionHolder = new CBOUtil.RexNodeHolder();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        if (!CBOUtil.checkHashJoinCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            equalConditionHolder, otherConditionHolder)) {
            return;
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
            rightTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        } else {
            if (outDriver) {
                return;
            }
            leftTraitSet = join.getLeft().getTraitSet().replace(outConvention);
            rightTraitSet = join.getRight().getTraitSet().replace(outConvention);
        }
        RelNode left = convert(join.getLeft(), leftTraitSet);
        RelNode right = convert(join.getRight(), rightTraitSet);

        createHashJoin(
            call,
            join,
            left,
            right,
            newCondition,
            equalConditionHolder,
            otherConditionHolder);
    }

    protected abstract void createHashJoin(
        RelOptRuleCall call,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        CBOUtil.RexNodeHolder equalConditionHolder,
        CBOUtil.RexNodeHolder otherConditionHolder);
}


