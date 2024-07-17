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
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
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
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.PredicateImpl;

public abstract class JoinTableLookupToBKAJoinTableLookupRule extends RelOptRule {

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

    public JoinTableLookupToBKAJoinTableLookupRule(RelOptRuleOperand operand, String desc) {
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
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        final LogicalJoin join = (LogicalJoin) rel;
        final LogicalTableLookup logicalTableLookup;
        final LogicalIndexScan logicalIndexScan;
        //    Join
        //   /    \
        //  rel   LookUp
        //          \
        //          IndexScan
        //
        //-------------------
        //
        //   BKA Join
        //   /    \
        //  rel   LookUp
        //          \
        //          IndexScan(join=rel)
        //
        // lookup will be transformed to another BKA join in optimizeByExpandTableLookup after CBO

        if (join.getJoinType() == JoinRelType.RIGHT) {
            logicalTableLookup = call.rel(1);
            logicalIndexScan = call.rel(2);
        } else {
            logicalTableLookup = call.rel(2);
            logicalIndexScan = call.rel(3);
        }

        //     Right Join
        //     /      \
        //   LookUp   rel
        //    /
        // IndexScan
        //
        // -----------------
        //
        //  BKA Right Join
        //     /      \
        //   LookUp   rel
        //    /
        // IndexScan(join=rel)
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
            leftTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
            rightTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        } else {
            leftTraitSet = join.getLeft().getTraitSet().replace(outConvention);
            rightTraitSet = join.getRight().getTraitSet().replace(outConvention);
        }

        RelNode left;
        RelNode right;
        final LogicalIndexScan newLogicalIndexScan =
            logicalIndexScan.copy(rel.getCluster().getPlanner().emptyTraitSet().replace(outConvention));

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

        createBKAJoin(
            call,
            join,
            left,
            right,
            newCondition,
            newLogicalIndexScan);
    }

    protected abstract void createBKAJoin(
        RelOptRuleCall call,
        LogicalJoin join,
        RelNode left,
        RelNode right,
        RexNode newCondition,
        LogicalIndexScan newLogicalIndexScan);
}
