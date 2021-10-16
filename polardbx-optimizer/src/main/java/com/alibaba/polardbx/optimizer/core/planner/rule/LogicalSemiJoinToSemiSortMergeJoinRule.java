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
import com.alibaba.polardbx.optimizer.core.rel.SemiSortMergeJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

public class LogicalSemiJoinToSemiSortMergeJoinRule extends ConverterRule {

    public static final LogicalSemiJoinToSemiSortMergeJoinRule INSTANCE =
        new LogicalSemiJoinToSemiSortMergeJoinRule("INSTANCE");

    public LogicalSemiJoinToSemiSortMergeJoinRule(String desc) {
        super(LogicalSemiJoin.class, Convention.NONE, DrdsConvention.INSTANCE,
            "LogicalSemiJoinToSemiSortMergeJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(call.rel(0))) {
            return false;
        }
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SEMI_SORT_MERGE_JOIN);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalSemiJoin logicalSemiJoin = (LogicalSemiJoin) rel;

        final RelNode left = logicalSemiJoin.getLeft();
        final RelNode right = logicalSemiJoin.getRight();

        List<Integer> leftColumns = new ArrayList<>();
        List<Integer> rightColumns = new ArrayList<>();
        CBOUtil.RexNodeHolder otherConditionHolder = new CBOUtil.RexNodeHolder();

        int leftFieldCount = logicalSemiJoin.getLeft().getRowType().getFieldCount();

        RexNode newCondition =
            JoinConditionSimplifyRule
                .simplifyCondition(logicalSemiJoin.getCondition(), logicalSemiJoin.getCluster().getRexBuilder());

        if (!CBOUtil
            .checkSortMergeCondition(logicalSemiJoin, newCondition, leftFieldCount, leftColumns,
                rightColumns, otherConditionHolder)) {
            return null;
        }

        if (logicalSemiJoin.getJoinType() == JoinRelType.ANTI
            && logicalSemiJoin.getOperands() != null && !logicalSemiJoin.getOperands().isEmpty()) {
            // If this node is an Anti-Join without operands ('NOT IN')
            if (!logicalSemiJoin.getCondition().isA(SqlKind.EQUALS)) {
                // ... and contains multiple equi-conditions
                return null; // reject!
            }
        }

        if (logicalSemiJoin.getJoinType() == JoinRelType.ANTI
            && logicalSemiJoin.getOperands() != null && !logicalSemiJoin.getOperands().isEmpty()) {
            // If this node is an Anti-Join without operands ('NOT IN')
            if (!newCondition.isA(SqlKind.EQUALS)) {
                // ... and contains multiple equi-conditions
                return null; // reject!
            }
        }

        RelCollation leftRelCollation = CBOUtil.createRelCollation(leftColumns);
        RelCollation rightRelCollation = CBOUtil.createRelCollation(rightColumns);

        RelNode newLeft;
        RelNode newRight;

        RelTraitSet inputTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);

        newLeft = convert(left, inputTraitSet);
        newRight = convert(right, inputTraitSet);

        LogicalSort leftSort =
            LogicalSort.create(rel.getCluster().getPlanner().emptyTraitSet().replace(leftRelCollation), newLeft,
                leftRelCollation, null, null);
        newLeft = convert(leftSort, leftSort.getTraitSet().replace(DrdsConvention.INSTANCE));

        LogicalSort rightSort =
            LogicalSort.create(rel.getCluster().getPlanner().emptyTraitSet().replace(rightRelCollation), newRight,
                rightRelCollation, null, null);
        newRight = convert(rightSort, rightSort.getTraitSet().replace(DrdsConvention.INSTANCE));

        SemiSortMergeJoin semiSortMergeJoin = SemiSortMergeJoin.create(
            logicalSemiJoin.getTraitSet().replace(DrdsConvention.INSTANCE),
            newLeft,
            newRight,
            leftRelCollation,
            newCondition,
            logicalSemiJoin,
            leftColumns,
            rightColumns,
            otherConditionHolder.getRexNode());
        RelOptCost fixedCost = CheckJoinHint.check(logicalSemiJoin, HintType.CMD_SEMI_SORT_MERGE_JOIN);
        if (fixedCost != null) {
            semiSortMergeJoin.setFixedCost(fixedCost);
        }
        return semiSortMergeJoin;
    }
}
