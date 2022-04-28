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
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
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
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * Avoid semi join produces too much data on the right LV
 *
 * @author hongxi.chx
 */
public class LogicalSemiJoinToSemiBKAJoinRule extends RelOptRule {

    public static final LogicalSemiJoinToSemiBKAJoinRule INSTANCE = new LogicalSemiJoinToSemiBKAJoinRule(
        operand(LogicalSemiJoin.class,
            operand(RelSubset.class, any()),
            operand(LogicalView.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())), "INSTANCE");

    public static final LogicalSemiJoinToSemiBKAJoinRule TABLELOOKUP = new LogicalSemiJoinToSemiBKAJoinRule(
        operand(LogicalSemiJoin.class,
            operand(RelSubset.class, any()),
            operand(LogicalTableLookup.class, null,
                JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                operand(LogicalIndexScan.class, none()))), "TABLELOOKUP");

    LogicalSemiJoinToSemiBKAJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "LogicalSemiJoinToSemiBKAJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
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
        if (call.getRule() == LogicalSemiJoinToSemiBKAJoinRule.INSTANCE) {
            onMatchLogicalView(call);
        } else if (call.getRule() == LogicalSemiJoinToSemiBKAJoinRule.TABLELOOKUP) {
            onMatchTableLookup(call);
        }
    }

    private void onMatchTableLookup(RelOptRuleCall call) {
        final LogicalSemiJoin join = call.rel(0);
        RelNode left = call.rel(1);
        final LogicalTableLookup logicalTableLookup = call.rel(2);
        final LogicalIndexScan logicalIndexScan = call.rel(3);

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(join.getCondition(), join.getCluster().getRexBuilder());

        RexUtils.RestrictType restrictType = RexUtils.RestrictType.RIGHT;
        // FIXME: restrict the condition for tablelookup
        // 1. must have column ref indexScan
        // 2. when equal condition ref indexScan and Primary table, executor should use only indexScan ref to build
        // Lookupkey, and let Primary table ref as other condition
        if (!RexUtils.isBatchKeysAccessCondition(join, newCondition, join.getLeft().getRowType().getFieldCount(),
            restrictType,
            (Pair<RelDataType, RelDataType> relDataTypePair) -> CBOUtil.bkaTypeCheck(relDataTypePair))) {
            return;
        }

        if (!RexUtils.isBatchKeysAccessConditionRefIndexScan(newCondition, join, true, logicalTableLookup)) {
            return;
        }

        if (!canBKAJoin(join)) {
            return;
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getRight().getTraitSet().replace(DrdsConvention.INSTANCE);
        }

        left = convert(left, leftTraitSet);

        LogicalIndexScan newLogicalIndexScan =
            logicalIndexScan.copy(join.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE));

        LogicalTableLookup right = logicalTableLookup.copy(
            rightTraitSet,
            newLogicalIndexScan,
            logicalTableLookup.getJoin().getRight(),
            logicalTableLookup.getIndexTable(),
            logicalTableLookup.getPrimaryTable(),
            logicalTableLookup.getProject(),
            logicalTableLookup.getJoin(),
            logicalTableLookup.isRelPushedToPrimary(),
            logicalTableLookup.getHints());

        SemiBKAJoin bkaJoin = SemiBKAJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
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
        newLogicalIndexScan.setIsMGetEnabled(true);
        newLogicalIndexScan.setJoin(bkaJoin);
        RelUtils.changeRowType(bkaJoin, join.getRowType());

        call.transformTo(bkaJoin);
    }

    private void onMatchLogicalView(RelOptRuleCall call) {
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
            (Pair<RelDataType, RelDataType> relDataTypePair) -> CBOUtil.bkaTypeCheck(relDataTypePair))) {
            return;
        }

        if (!canBKAJoin(join)) {
            return;
        }

        final RelTraitSet leftTraitSet;
        final RelTraitSet rightTraitSet;
        if (RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(join)) {
            leftTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(DrdsConvention.INSTANCE);
            rightTraitSet = join.getRight().getTraitSet().replace(DrdsConvention.INSTANCE);
        }

        left = convert(left, leftTraitSet);
        LogicalView right = logicalView.copy(rightTraitSet);

        SemiBKAJoin bkaJoin = SemiBKAJoin.create(
            join.getTraitSet().replace(DrdsConvention.INSTANCE),
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

        call.transformTo(bkaJoin);
    }

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
