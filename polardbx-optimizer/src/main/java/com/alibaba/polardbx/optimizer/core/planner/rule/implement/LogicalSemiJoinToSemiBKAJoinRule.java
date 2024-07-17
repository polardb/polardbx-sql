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
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rex.RexNode;

/**
 * Avoid semi join produces too much data on the right LV
 *
 * @author hongxi.chx
 */
public abstract class LogicalSemiJoinToSemiBKAJoinRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected LogicalSemiJoinToSemiBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "LogicalSemiJoinToSemiBKAJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    public static boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SEMI_BKA_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final RelNode rel = call.rel(0);
        if (!enable(PlannerContext.getPlannerContext(rel))) {
            return false;
        }

        if (call.rel(2) instanceof OSSTableScan) {
            return false;
        }
        if (rel instanceof SemiJoin) {
            final JoinRelType joinType = ((SemiJoin) rel).getJoinType();
            if (joinType == JoinRelType.SEMI || joinType == JoinRelType.LEFT) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin join = call.rel(0);
        RelNode left = call.rel(1);
        final LogicalView logicalView = call.rel(2);
        if (logicalView instanceof OSSTableScan) {
            return;
        }
        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        RexUtils.RestrictType restrictType = RexUtils.RestrictType.RIGHT;
        if (!RexUtils.isBatchKeysAccessCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            restrictType,
            CBOUtil::bkaTypeCheck)) {
            return;
        }

        if (!canBKAJoin(join)) {
            return;
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
            rightTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(outConvention);
            rightTraitSet = join.getRight().getTraitSet().replace(outConvention);
        }

        left = convert(left, leftTraitSet);
        LogicalView right = logicalView.copy(rightTraitSet);

        createSemiBKAJoin(
            call,
            join,
            left,
            right,
            newCondition);
    }

    protected abstract void createSemiBKAJoin(
        RelOptRuleCall call,
        LogicalSemiJoin join,
        RelNode left,
        LogicalView right,
        RexNode newCondition);

    private boolean canBKAJoin(SemiJoin join) {
        RelNode right = join.getRight();
        if (right instanceof RelSubset) {
            right = ((RelSubset) right).getOriginal();
        }
        if (CBOUtil.checkBkaJoinForLogicalView(right)) {
            return true;
        }
        return false;
    }
}
