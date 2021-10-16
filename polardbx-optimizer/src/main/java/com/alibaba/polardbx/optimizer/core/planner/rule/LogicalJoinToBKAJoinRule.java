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

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;

public class LogicalJoinToBKAJoinRule extends RelOptRule {

    public static PredicateImpl JOIN_RIGHT = new PredicateImpl<LogicalJoin>() {
        @Override
        public boolean test(LogicalJoin logicalJoin) {
            return logicalJoin.getJoinType() == JoinRelType.RIGHT;
        }
    };

    public static PredicateImpl JOIN_NOT_RIGHT = new PredicateImpl<LogicalJoin>() {
        @Override
        public boolean test(LogicalJoin logicalJoin) {
            return logicalJoin.getJoinType() != JoinRelType.RIGHT;
        }
    };

    public static final LogicalJoinToBKAJoinRule LOGICALVIEW_NOT_RIGHT = new LogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_NOT_RIGHT,
            operand(RelSubset.class, any()),
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())), "LOGICALVIEW:NOT_RIGHT");

    public static final LogicalJoinToBKAJoinRule LOGICALVIEW_RIGHT = new LogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_RIGHT,
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()),
            operand(RelSubset.class, any())), "LOGICALVIEW:RIGHT");

    public static final LogicalJoinToBKAJoinRule LOGICALVIEW_NOT_RIGHT_FOR_EXPAND = new LogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_NOT_RIGHT,
            operand(RelNode.class, any()),
            operand(LogicalView.class, any())), "LOGICALVIEW:NOT_RIGHT_FOR_EXPAND");

    public static final LogicalJoinToBKAJoinRule LOGICALVIEW_RIGHT_FOR_EXPAND = new LogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_RIGHT,
            operand(LogicalView.class, any()),
            operand(RelNode.class, any())), "LOGICALVIEW:RIGHT_FOR_EXPAND");

    public static final LogicalJoinToBKAJoinRule TABLELOOKUP_NOT_RIGHT = new LogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_NOT_RIGHT,
            operand(RelSubset.class, any()),
            operand(LogicalTableLookup.class, null,
                JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                operand(LogicalIndexScan.class, none()))), "TABLELOOKUP:NOT_RIGHT");

    public static final LogicalJoinToBKAJoinRule TABLELOOKUP_RIGHT = new LogicalJoinToBKAJoinRule(
        operand(LogicalJoin.class, null, JOIN_RIGHT,
            operand(LogicalTableLookup.class, null,
                JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                operand(LogicalIndexScan.class, none())),
            operand(RelSubset.class, any())), "TABLELOOKUP:RIGHT");

    LogicalJoinToBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "LogicalJoinToBKAJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_BKA_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.getRule() == LogicalJoinToBKAJoinRule.LOGICALVIEW_NOT_RIGHT
            || call.getRule() == LogicalJoinToBKAJoinRule.LOGICALVIEW_RIGHT
            || call.getRule() == LogicalJoinToBKAJoinRule.LOGICALVIEW_NOT_RIGHT_FOR_EXPAND) {
            onMatchLogicalView(call);
        } else if (call.getRule() == LogicalJoinToBKAJoinRule.TABLELOOKUP_NOT_RIGHT
            || call.getRule() == LogicalJoinToBKAJoinRule.TABLELOOKUP_RIGHT
            || call.getRule() == LogicalJoinToBKAJoinRule.LOGICALVIEW_RIGHT_FOR_EXPAND) {
            onMatchTableLookup(call);
        }
    }

    private void onMatchTableLookup(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        final LogicalJoin join = (LogicalJoin) rel;
        final LogicalTableLookup logicalTableLookup;
        final LogicalIndexScan logicalIndexScan;
        if (join.getJoinType() == JoinRelType.RIGHT) {
            logicalTableLookup = call.rel(1);
            logicalIndexScan = call.rel(2);
        } else {
            logicalTableLookup = call.rel(2);
            logicalIndexScan = call.rel(3);
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

        // FIXME: restrict the condition for tablelookup
        // 1. must have column ref indexScan
        // 2. when equal condition ref indexScan and Primary table, executor should use only indexScan ref to build
        // Lookupkey, and let Primary table ref as other condition
        if (!RexUtils.isBatchKeysAccessCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            restrictType,
            (Pair<RelDataType, RelDataType> relDataTypePair) -> CBOUtil.bkaTypeCheck(relDataTypePair))) {
            return;
        }

        if (!RexUtils.isBatchKeysAccessConditionRefIndexScan(newCondition, join,
            join.getJoinType() != JoinRelType.RIGHT, logicalTableLookup)) {
            return;
        }

        if (!CBOUtil.canBKAJoin(join)) {
            return;
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getRight().getTraitSet().replace(DrdsConvention.INSTANCE);
        }

        RelNode left;
        RelNode right;
        final LogicalIndexScan newLogicalIndexScan =
            logicalIndexScan.copy(rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE));

        if (join.getJoinType().equals(JoinRelType.RIGHT)) {
            left = logicalTableLookup.copy(
                leftTraitSet,
                newLogicalIndexScan,
                logicalTableLookup.getJoin().getRight(),
                logicalTableLookup.getIndexTable(),
                logicalTableLookup.getPrimaryTable(),
                logicalTableLookup.getProject(),
                logicalTableLookup.getJoin(),
                logicalTableLookup.isRelPushedToPrimary(),
                logicalTableLookup.getHints());
            right = convert(join.getRight(), rightTraitSet);
        } else {
            left = convert(join.getLeft(), leftTraitSet);
            right = logicalTableLookup.copy(
                rightTraitSet,
                newLogicalIndexScan,
                logicalTableLookup.getJoin().getRight(),
                logicalTableLookup.getIndexTable(),
                logicalTableLookup.getPrimaryTable(),
                logicalTableLookup.getProject(),
                logicalTableLookup.getJoin(),
                logicalTableLookup.isRelPushedToPrimary(),
                logicalTableLookup.getHints());
        }

        BKAJoin bkaJoin = BKAJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
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

    private void onMatchLogicalView(RelOptRuleCall call) {
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

        if (call.getRule() == LogicalJoinToBKAJoinRule.LOGICALVIEW_NOT_RIGHT_FOR_EXPAND
            || call.getRule() == LogicalJoinToBKAJoinRule.LOGICALVIEW_RIGHT_FOR_EXPAND) {
            if (join.getJoinType().equals(JoinRelType.RIGHT)) {
                left = inner = logicalView.copy(logicalView.getTraitSet().replace(DrdsConvention.INSTANCE));
                right = convert(join.getRight(), join.getRight().getTraitSet().replace(DrdsConvention.INSTANCE));
            } else {
                left = convert(join.getLeft(), join.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE));
                right = inner = logicalView.copy(logicalView.getTraitSet().replace(DrdsConvention.INSTANCE));
            }
        } else {
            final RelTraitSet leftTraitSet;
            final RelTraitSet rightTraitSet;
            if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
                leftTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
                rightTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
            } else {
                leftTraitSet = join.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE);
                rightTraitSet = join.getRight().getTraitSet().replace(DrdsConvention.INSTANCE);
            }

            if (join.getJoinType().equals(JoinRelType.RIGHT)) {
                left = inner = logicalView.copy(leftTraitSet);
                right = convert(join.getRight(), rightTraitSet);
            } else {
                left = convert(join.getLeft(), leftTraitSet);
                right = inner = logicalView.copy(rightTraitSet);
            }
        }

        BKAJoin bkaJoin = BKAJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
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
        inner.setIsMGetEnabled(true);
        inner.setJoin(bkaJoin);
        call.transformTo(bkaJoin);
    }

}
