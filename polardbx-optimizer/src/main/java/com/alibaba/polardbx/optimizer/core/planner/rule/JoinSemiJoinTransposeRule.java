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

import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that matches a
 * {@link LogicalJoin}, one of whose inputs have
 * {@link LogicalSemiJoin}, and
 * try to pull the LogicalSemiJoin up.
 * <p>
 * The rule is used to generate a simpler sql for DN.
 *
 * <p>LogicalSemiJoin will not be pulled up in the following cases:
 * 1. left semi join
 * 2. null generating join
 *
 * @author shengyu
 */
public class JoinSemiJoinTransposeRule extends RelOptRule {

    public static final JoinSemiJoinTransposeRule LEFT_SEMI =
        new JoinSemiJoinTransposeRule(
            operand(LogicalJoin.class,
                operand(LogicalSemiJoin.class, any()),
                operand(RelNode.class, any())),
            RelFactories.LOGICAL_BUILDER, "JoinSemiJoinTransposeRule:left") {

            @Override
            public boolean matches(RelOptRuleCall call) {
                final LogicalSemiJoin semiJoin = call.rel(1);
                //can't handle left semi join
                return semiJoin.getJoinType() == JoinRelType.SEMI || semiJoin.getJoinType() == JoinRelType.ANTI;
            }

            //        Join
            //       /    \
            //   SemiJoin  Y
            //    /    \
            //   X      Z
            //
            // becomes
            //
            //     newSemiJoin
            //      /      \
            //  newJoin     Z
            //    /  \
            //   X    Y
            //
            // Join condition don't need to change, SemiJoin condition should change
            // Note that join can't generate null on left
            @Override
            public void onMatch(RelOptRuleCall call) {
                LogicalJoin join = call.rel(0);
                LogicalSemiJoin semiJoin = call.rel(1);
                if (join.getJoinType().generatesNullsOnLeft()) {
                    return;
                }
                //get new semiJoin condition
                RelNode x = semiJoin.getLeft();
                RelNode z = semiJoin.getRight();
                RelNode y = join.getRight();
                int nFieldsX = x.getRowType().getFieldList().size();
                int nFieldsZ = z.getRowType().getFieldList().size();
                int nFieldsY = y.getRowType().getFieldList().size();
                int nTotalFields = nFieldsX + nFieldsZ + nFieldsY;
                int[] adjustments = new int[nTotalFields];
                for (int i = nFieldsX; i < nFieldsX + nFieldsZ; i++) {
                    adjustments[i] = nFieldsY;
                }
                transform(call, join, semiJoin, x, y, z, adjustments);
            }
        };

    public static final JoinSemiJoinTransposeRule RIGHT_SEMI =
        new JoinSemiJoinTransposeRule(
            operand(LogicalJoin.class,
                operand(RelNode.class, any()),
                operand(LogicalSemiJoin.class, any())),
            RelFactories.LOGICAL_BUILDER, "JoinSemiJoinTransposeRule:right") {
            @Override
            public boolean matches(RelOptRuleCall call) {
                final LogicalSemiJoin semiJoin = call.rel(2);
                //can't handle left semi join
                return semiJoin.getJoinType() == JoinRelType.SEMI || semiJoin.getJoinType() == JoinRelType.ANTI;
            }

            /**
             *        Join
             *       /    \
             *      X   SemiJoin
             *           /   \
             *          Y     Z
             *
             * becomes
             *
             *     newSemiJoin
             *      /      \
             *  newJoin     Z
             *    /  \
             *   X    Y
             *
             * Join condition don't need to change, SemiJoin condition should change
             * Note that join can't generate null on right
             *
             */
            @Override
            public void onMatch(RelOptRuleCall call) {
                LogicalJoin join = call.rel(0);
                LogicalSemiJoin semiJoin = call.rel(2);
                if (join.getJoinType().generatesNullsOnRight()) {
                    return;
                }
                RelNode x = join.getLeft();
                RelNode y = semiJoin.getLeft();
                RelNode z = semiJoin.getRight();
                int nFieldsX = x.getRowType().getFieldList().size();

                int nTotalFields = nFieldsX + y.getRowType().getFieldList().size()
                    + z.getRowType().getFieldList().size();
                int[] adjustments = new int[nTotalFields];
                for (int i = nFieldsX; i < nTotalFields; i++) {
                    adjustments[i] = -nFieldsX;
                }
                transform(call, join, semiJoin, x, y, z, adjustments);
            }
        };

    /**
     * build a tree like
     * newSemiJoin
     * /      \
     * newJoin Z
     * /  \
     * X    Y
     *
     * @param join the old join
     * @param semiJoin the old semiJoin
     * @param x left of new join
     * @param y right of new join
     * @param z right of new semiJoin
     * @param adjustments adjustment for semiJoin condition
     */
    private static void transform(RelOptRuleCall call, LogicalJoin join, LogicalSemiJoin semiJoin, RelNode x, RelNode y,
                                  RelNode z, int[] adjustments) {
        List<RelDataTypeField> fields = new ArrayList<>(x.getRowType().getFieldList());
        fields.addAll(y.getRowType().getFieldList());
        fields.addAll(z.getRowType().getFieldList());

        RelOptUtil.RexInputConverter rexInputConverter = new RelOptUtil.RexInputConverter(
            semiJoin.getCluster().getRexBuilder(),
            fields,
            adjustments);
        RexNode newSemiJoinCondition = semiJoin.getCondition().accept(rexInputConverter);

        LogicalJoin newJoin = join.copy(
            join.getTraitSet(),
            join.getCondition(),
            x,
            y,
            join.getJoinType(),
            join.isSemiJoinDone()
        );

        LogicalSemiJoin newSemiJoin = semiJoin.copy(
            semiJoin.getTraitSet(),
            newSemiJoinCondition,
            newJoin,
            z,
            semiJoin.getJoinType(),
            semiJoin.isSemiJoinDone()
        );
        RelUtils.changeRowType(newSemiJoin, newJoin.getRowType());
        call.transformTo(newSemiJoin);
    }

    public JoinSemiJoinTransposeRule(
        RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
    }
}
