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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
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
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

public class LogicalSemiJoinToMaterializedSemiJoinRule extends RelOptRule {

    public static final LogicalSemiJoinToMaterializedSemiJoinRule INSTANCE =
        new LogicalSemiJoinToMaterializedSemiJoinRule(
            operand(LogicalSemiJoin.class,
                operand(LogicalView.class, any()),
                operand(RelSubset.class, any())), "LogicalSemiJoinToMaterializedSemiJoinRule:INSTANCE");

    public static final LogicalSemiJoinToMaterializedSemiJoinRule TABLELOOKUP =
        new LogicalSemiJoinToMaterializedSemiJoinRule(
            operand(LogicalSemiJoin.class,
                operand(LogicalTableLookup.class, null,
                    JoinTableLookupTransposeRule.INNER_TABLE_LOOKUP_RIGHT_IS_LOGICALVIEW,
                    operand(LogicalIndexScan.class, none())),
                operand(RelSubset.class, any())), "LogicalSemiJoinToMaterializedSemiJoinRule:TABLELOOKUP");

    LogicalSemiJoinToMaterializedSemiJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
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
        if (call.getRule() == LogicalSemiJoinToMaterializedSemiJoinRule.INSTANCE) {
            onMatchLogicalView(call);
        } else if (call.getRule() == LogicalSemiJoinToMaterializedSemiJoinRule.TABLELOOKUP) {
            onMatchTableLookup(call);
        }
    }

    public void onMatchTableLookup(RelOptRuleCall call) {
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
                (Pair<RelDataType, RelDataType> relDataTypePair) -> typeCheck(relDataTypePair), true)) {
            return;
        }

        if (!RexUtils.isBatchKeysAccessConditionRefIndexScan(newCondition, semiJoin,
            false, logicalTableLookup)) {
            return;
        }

        if (!canMaterializedSemiJoin(semiJoin)) {
            return;
        }

        RelTraitSet inputTraitSet = semiJoin.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);

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
        right = convert(right, right.getTraitSet().replace(DrdsConvention.INSTANCE));

        ImmutableBitSet rightBitSet = ImmutableBitSet.range(0, right.getRowType().getFieldCount());
        Boolean rightInputUnique = semiJoin.getCluster().getMetadataQuery().areColumnsUnique(right, rightBitSet);
        boolean distinctInput = rightInputUnique == null ? true : !rightInputUnique;

        MaterializedSemiJoin materializedSemiJoin = MaterializedSemiJoin.create(
            semiJoin.getTraitSet().replace(DrdsConvention.INSTANCE), left, right, newCondition, semiJoin,
            distinctInput);

        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_MATERIALIZED_SEMI_JOIN);
        if (fixedCost != null) {
            materializedSemiJoin.setFixedCost(fixedCost);
        }
        newLogicalIndexScan.setIsMGetEnabled(true);
        newLogicalIndexScan.setJoin(materializedSemiJoin);
        call.transformTo(materializedSemiJoin);
    }

    public void onMatchLogicalView(RelOptRuleCall call) {
        final LogicalSemiJoin semiJoin = call.rel(0);
        final LogicalView logicalView = call.rel(1);
        if (logicalView instanceof OSSTableScan) {
            return;
        }
        RelNode right = call.rel(2);

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(semiJoin.getCondition(), semiJoin.getCluster().getRexBuilder());

        if (!RexUtils
            .isBatchKeysAccessCondition(semiJoin, newCondition,
                semiJoin.getLeft().getRowType().getFieldCount(),
                RexUtils.RestrictType.LEFT,
                (Pair<RelDataType, RelDataType> relDataTypePair) -> typeCheck(relDataTypePair), true)) {
            return;
        }
        if (!canMaterializedSemiJoin(semiJoin)) {
            return;
        }

        RelTraitSet inputTraitSet = semiJoin.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);

        LogicalView left = logicalView.copy(inputTraitSet);
        right = convert(right, inputTraitSet);

        ImmutableBitSet rightBitSet = ImmutableBitSet.range(0, right.getRowType().getFieldCount());
        Boolean rightInputUnique = semiJoin.getCluster().getMetadataQuery().areColumnsUnique(right, rightBitSet);
        boolean distinctInput = rightInputUnique == null ? true : !rightInputUnique;

        MaterializedSemiJoin materializedSemiJoin = MaterializedSemiJoin.create(
            semiJoin.getTraitSet().replace(DrdsConvention.INSTANCE), left, right, newCondition, semiJoin,
            distinctInput);

        RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_MATERIALIZED_SEMI_JOIN);
        if (fixedCost != null) {
            materializedSemiJoin.setFixedCost(fixedCost);
        }
        left.setIsMGetEnabled(true);
        left.setJoin(materializedSemiJoin);
        call.transformTo(materializedSemiJoin);
    }

    public static boolean typeCheck(Pair<RelDataType, RelDataType> relDataTypePair) {
        RelDataType relDataType1 = relDataTypePair.getKey();
        RelDataType relDataType2 = relDataTypePair.getValue();
        DataType dt1 = DataTypeUtil.calciteToDrdsType(relDataType1);
        DataType dt2 = DataTypeUtil.calciteToDrdsType(relDataType2);

        if (dt1 == null || dt2 == null) {
            return false;
        }

        if (DataTypeUtil.isFloatSqlType(dt2)) {
            // float number can not be materialized
            return false;
        } else if (DataTypeUtil.isNumberSqlType(dt1)) {
            if (!DataTypeUtil.isStringSqlType(dt2) && !DataTypeUtil.isNumberSqlType(dt2)) {
                // if dt2 neither of number or string may mis-match with dt1
                return false;
            } else {
                return true;
            }
        } else if (DataTypeUtil.equalsSemantically(dt1, DataTypes.BooleanType)) {
            // BooleanType may come from tinyint(1) which will mis-match
            if (!DataTypeUtil.isStringSqlType(dt2)
                && !DataTypeUtil.isNumberSqlType(dt2)
                && !DataTypeUtil.equalsSemantically(dt2, DataTypes.BooleanType)) {
                return false;
            } else {
                return true;
            }
        } else if (DataTypeUtil.equalsSemantically(dt1, dt2)) {
            return true;
        } else {
            return false;
        }
    }

    private boolean canMaterializedSemiJoin(LogicalSemiJoin join) {
        if (join.getJoinType() != JoinRelType.SEMI && join.getJoinType() != JoinRelType.ANTI) {
            return false;
        }

        if (join.getJoinType() == JoinRelType.ANTI && join.getOperands().isEmpty()) {
            return false;
        }

        RelNode left = join.getLeft();
        if (left instanceof RelSubset) {
            left = ((RelSubset) left).getOriginal();
        }

        if (CBOUtil.checkBkaJoinForLogicalView(left)) {
            return true;
        }

        return false;
    }
}
