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
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

public abstract class SemiJoinTableLookupToMaterializedSemiJoinTableLookupRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected SemiJoinTableLookupToMaterializedSemiJoinTableLookupRule(RelOptRuleOperand operand, String desc) {
        super(operand, "SemiJoinTableLookupToMaterializedSemiJoinTableLookupRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MATERIALIZED_SEMI_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(call.rel(0))) {
            return false;
        }
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin semiJoin = call.rel(0);
        final LogicalTableLookup logicalTableLookup = call.rel(1);
        final LogicalIndexScan logicalIndexScan = call.rel(2);
        RelNode right = call.rel(3);

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(semiJoin.getCondition(), semiJoin.getCluster().getRexBuilder());

        // FIXME: restrict the condition for tablelookup
        // 1. must have column ref indexScan
        // 2. when equal condition ref indexScan and Primary table, executor should use only indexScan ref to build
        // Lookupkey, and let Primary table ref as other condition
        if (!RexUtils
            .isBatchKeysAccessCondition(semiJoin, newCondition,
                semiJoin.getLeft().getRowType().getFieldCount(),
                RexUtils.RestrictType.LEFT,
                LogicalSemiJoinToMaterializedSemiJoinRule::typeCheck, true)) {
            return;
        }

        if (!RexUtils.isBatchKeysAccessConditionRefIndexScan(newCondition, semiJoin,
            false, logicalTableLookup)) {
            return;
        }

        if (!LogicalSemiJoinToMaterializedSemiJoinRule.canMaterializedSemiJoin(semiJoin)) {
            return;
        }

        RelTraitSet inputTraitSet = semiJoin.getCluster().getPlanner().emptyTraitSet().replace(outConvention);

        final LogicalIndexScan newLogicalIndexScan =
            logicalIndexScan.copy(inputTraitSet);

        LogicalTableLookup left = logicalTableLookup.copy(
            inputTraitSet,
            newLogicalIndexScan,
            logicalTableLookup.getJoin().getRight(),
            logicalTableLookup.getIndexTable(),
            logicalTableLookup.getPrimaryTable(),
            logicalTableLookup.getProject(),
            logicalTableLookup.getJoin(),
            logicalTableLookup.isRelPushedToPrimary(),
            logicalTableLookup.getHints());
        right = convert(right, right.getTraitSet().replace(outConvention));

        ImmutableBitSet rightBitSet = ImmutableBitSet.range(0, right.getRowType().getFieldCount());
        Boolean rightInputUnique = semiJoin.getCluster().getMetadataQuery().areColumnsUnique(right, rightBitSet);
        boolean distinctInput = rightInputUnique == null ? true : !rightInputUnique;

        createMaterializedSemiJoin(
            call,
            semiJoin,
            left,
            right,
            newCondition,
            newLogicalIndexScan,
            distinctInput);
    }

    protected abstract void createMaterializedSemiJoin(
        RelOptRuleCall call,
        LogicalSemiJoin join,
        LogicalTableLookup left,
        RelNode right,
        RexNode newCondition,
        LogicalIndexScan logicalIndexScan,
        boolean distinctInput);
}
