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
import com.alibaba.polardbx.common.utils.Pair;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;

public abstract class LogicalJoinToBKAJoinRule extends RelOptRule {

    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected static PredicateImpl JOIN_RIGHT = new PredicateImpl<LogicalJoin>() {
        @Override
        public boolean test(LogicalJoin logicalJoin) {
            return logicalJoin.getJoinType() == JoinRelType.RIGHT;
        }
    };

    protected static PredicateImpl JOIN_NOT_RIGHT = new PredicateImpl<LogicalJoin>() {
        @Override
        public boolean test(LogicalJoin logicalJoin) {
            return logicalJoin.getJoinType() != JoinRelType.RIGHT;
        }
    };

    protected LogicalJoinToBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "LogicalJoinToBKAJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_BKA_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalView logicalView;
        if (((LogicalJoin) call.rel(0)).getJoinType() == JoinRelType.RIGHT) {
            logicalView = call.rel(1);
        } else {
            logicalView = call.rel(2);
        }

        if (logicalView instanceof OSSTableScan) {
            return false;
        }
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        final LogicalJoin join = (LogicalJoin) rel;
        final LogicalView logicalView;
        if (join.getJoinType() == JoinRelType.RIGHT) {
            logicalView = call.rel(1);
        } else {
            logicalView = call.rel(2);
        }

        RexUtils.RestrictType restrictType;
        switch (join.getJoinType()) {
        case LEFT:
        case INNER:
            restrictType = RexUtils.RestrictType.RIGHT;
            break;
        case RIGHT:
            restrictType = RexUtils.RestrictType.LEFT;
            break;
        default:
            return;
        }

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        if (!RexUtils.isBatchKeysAccessCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            restrictType,
            (Pair<RelDataType, RelDataType> relDataTypePair) -> CBOUtil.bkaTypeCheck(relDataTypePair))) {
            return;
        }

        if (!CBOUtil.canBKAJoin(join)) {
            return;
        }

        RelNode left;
        RelNode right;

        final LogicalView inner;

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
            rightTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(outConvention);
            rightTraitSet = join.getRight().getTraitSet().replace(outConvention);
        }

        if (join.getJoinType().equals(JoinRelType.RIGHT)) {
            left = inner = logicalView.copy(leftTraitSet);
            right = convert(join.getRight(), rightTraitSet);
        } else {
            left = convert(join.getLeft(), leftTraitSet);
            right = inner = logicalView.copy(rightTraitSet);
        }

        createBKAJoin(
            call,
            join,
            left,
            right,
            inner,
            newCondition);
    }

    protected abstract void createBKAJoin(
        RelOptRuleCall call,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        LogicalView inner,
        RexNode newCondition);

}
