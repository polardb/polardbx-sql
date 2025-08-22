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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.SemiJoin}
 * down in a tree past a {@link org.apache.calcite.rel.core.Join}
 * in order to trigger other rules that will convert {@code SemiJoin}s.
 *
 * <ul>
 * <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(SemiJoin(X, Z), Y)
 * <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
 *
 * <p>Whether this
 * first or second conversion is applied depends on which operands actually
 * participate in the semi-join.</p>
 */
public class CBOLogicalSemiJoinLogicalJoinTransposeRule extends AbstractCBOLogicalSemiJoinLogicalJoinTransposeRule {
    public static final CBOLogicalSemiJoinLogicalJoinTransposeRule INSTANCE =
        new CBOLogicalSemiJoinLogicalJoinTransposeRule(
            operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                some(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER, "CBOLogicalSemiJoinLogicalJoinTransposeRule", false);

    public static final CBOLogicalSemiJoinLogicalJoinTransposeRule PROJECT_INSTANCE =
        new CBOLogicalSemiJoinLogicalJoinTransposeRule(
            operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                    operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER,
            "CBOLogicalSemiJoinLogicalJoinTransposeRule:Project", false);

    public static final CBOLogicalSemiJoinLogicalJoinTransposeRule LASSCOM =
        new CBOLogicalSemiJoinLogicalJoinTransposeRule(
            operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                some(operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER,
            "CBOLogicalSemiJoinLogicalJoinTransposeRule:LASSCOM", true);

    public static final CBOLogicalSemiJoinLogicalJoinTransposeRule PROJECT_LASSCOM =
        new CBOLogicalSemiJoinLogicalJoinTransposeRule(
            operand(LogicalSemiJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                    operand(LogicalJoin.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any()))),
            RelFactories.LOGICAL_BUILDER,
            "CBOLogicalSemiJoinLogicalJoinTransposeRule:PROJECT_LASSCOM", true);

    private final boolean lAsscom;

    public CBOLogicalSemiJoinLogicalJoinTransposeRule(
        RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description, boolean lAsscom) {
        super(operand, relBuilderFactory, description);
        this.lAsscom = lAsscom;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalSemiJoin topJoin = call.rel(0);
        final LogicalJoin bottomJoin;
        if (call.getRule() == INSTANCE || call.getRule() == LASSCOM) {
            bottomJoin = call.rel(1);
        } else if (call.getRule() == PROJECT_INSTANCE || call.getRule() == PROJECT_LASSCOM) {
            bottomJoin = call.rel(2);
        } else {
            return false;
        }

        if (bottomJoin.getJoinReorderContext().isHasCommuteZigZag()) {
            return false;
        }
        if (topJoin.getJoinReorderContext().isHasSemiFilter()) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.getRule() == INSTANCE || call.getRule() == LASSCOM) {
            onMatchInstance(call);
        } else if (call.getRule() == PROJECT_INSTANCE || call.getRule() == PROJECT_LASSCOM) {
            onMatchProjectInstance(call);
        }
    }

    public class InputReferenceFinder extends RexVisitorImpl<Void> {
        private final boolean[] refs;

        public InputReferenceFinder(boolean[] refs) {
            super(true);
            this.refs = refs;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            refs[inputRef.getIndex()] = true;
            return null;
        }
    }

    @Override
    protected LogicalJoin transform(final LogicalSemiJoin topJoin, final LogicalJoin bottomJoin,
                                    RelBuilder relBuilder) {
        if (topJoin.getJoinType() != JoinRelType.SEMI && topJoin.getJoinType() != JoinRelType.ANTI) {
            return null;
        }

        // X is the left child of the join below the semi-join
        // Y is the right child of the join below the semi-join
        // Z is the right child of the semi-join
        int nFieldsX = bottomJoin.getLeft().getRowType().getFieldList().size();
        int nFieldsY = bottomJoin.getRight().getRowType().getFieldList().size();
        int nFieldsZ = topJoin.getRight().getRowType().getFieldList().size();
        int nTotalFields = nFieldsX + nFieldsY + nFieldsZ;
        List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();

        // create a list of fields for the full join result; note that
        // we can't simply use the fields from the semi-join because the
        // row-type of a semi-join only includes the left hand side fields
        List<RelDataTypeField> joinFields =
            topJoin.getRowType().getFieldList();
        for (int i = 0; i < (nFieldsX + nFieldsY); i++) {
            fields.add(joinFields.get(i));
        }
        joinFields = topJoin.getRight().getRowType().getFieldList();
        for (int i = 0; i < nFieldsZ; i++) {
            fields.add(joinFields.get(i));
        }

        boolean[] joinCondRefs = new boolean[nTotalFields];
        InputReferenceFinder inputReferenceCounter =
            new InputReferenceFinder(joinCondRefs);
        topJoin.getCondition().accept(inputReferenceCounter);

        // determine which operands below the semi-join are the actual
        // Rels that participate in the semi-join
        int nKeysFromX = 0;
        int nKeysFromY = 0;

        for (int i = 0; i < nFieldsX; i++) {
            if (joinCondRefs[i]) {
                nKeysFromX++;
            }
        }
        for (int i = nFieldsX; i < nFieldsX + nFieldsY; i++) {
            if (joinCondRefs[i]) {
                nKeysFromY++;
            }
        }

        // the keys must all originate from either the left or right;
        // otherwise, a semi-join wouldn't have been created
        // assert (nKeysFromX == 0) || (nKeysFromX == leftKeys.size());
        /** it can be a semi-join key from X AND Y */
        if ((nKeysFromX != 0) && (nKeysFromX != nKeysFromX + nKeysFromY)) {
            return null;
        }

        // need to convert the semi-join condition and possibly the keys
        RexNode newSemiJoinFilter;
        List<RexNode> newOperands = null;
        List<RexNode> newInnerOperands = null;
        List<Integer> newLeftKeys;
        int[] adjustments = new int[nTotalFields];
        if (nKeysFromX > 0) {
            // (X, Y, Z) --> (X, Z, Y)
            // semiJoin(X, Z)
            // pass 0 as Y's adjustment because there shouldn't be any
            // references to Y in the semi-join filter
            setJoinAdjustments(
                adjustments,
                nFieldsX,
                nFieldsY,
                nFieldsZ,
                0,
                -nFieldsY);

            RelOptUtil.RexInputConverter rexInputConverter = new RelOptUtil.RexInputConverter(
                topJoin.getCluster().getRexBuilder(),
                fields,
                adjustments);

            newSemiJoinFilter = topJoin.getCondition().accept(rexInputConverter);
            if (topJoin.getOperands() != null) {
                newOperands = new ArrayList<>();
                for (RexNode rexNode : topJoin.getOperands()) {
                    newOperands.add(rexNode.accept(rexInputConverter));
                }
            }
        } else if (lAsscom) {
            return null; /** if only match lAsscom, bail out */
        } else {
            // (X, Y, Z) --> (X, Y, Z)
            // semiJoin(Y, Z)
            setJoinAdjustments(
                adjustments,
                nFieldsX,
                nFieldsY,
                nFieldsZ,
                -nFieldsX,
                -nFieldsX);
            RelOptUtil.RexInputConverter rexInputConverter = new RelOptUtil.RexInputConverter(
                topJoin.getCluster().getRexBuilder(),
                fields,
                adjustments);
            newSemiJoinFilter = topJoin.getCondition().accept(rexInputConverter);
            if (topJoin.getOperands() != null) {
                newOperands = new ArrayList<>();
                for (RexNode rexNode : topJoin.getOperands()) {
                    newOperands.add(rexNode.accept(rexInputConverter));
                }
            }
        }

        // create the new join
        RelNode leftSemiJoinOp;
        if (nKeysFromX > 0) {
            leftSemiJoinOp = bottomJoin.getLeft();
        } else {
            leftSemiJoinOp = bottomJoin.getRight();
        }
        LogicalSemiJoin newSemiJoin = topJoin.copy(
            topJoin.getTraitSet(),
            newSemiJoinFilter,
            leftSemiJoinOp,
            topJoin.getRight(),
            topJoin.getJoinType(),
            topJoin.isSemiJoinDone(),
            newOperands);
        newSemiJoin.getJoinReorderContext().setHasTopPushThrough(true);

        RelNode leftJoinRel;
        RelNode rightJoinRel;
        JoinRelType joinType = bottomJoin.getJoinType();
        if (nKeysFromX > 0) {
            leftJoinRel = newSemiJoin;
            rightJoinRel = bottomJoin.getRight();
            if (joinType == JoinRelType.RIGHT) {
                joinType = JoinRelType.INNER;
            }
        } else {
            leftJoinRel = bottomJoin.getLeft();
            rightJoinRel = newSemiJoin;
            if (joinType == JoinRelType.LEFT) {
                joinType = JoinRelType.INNER;
            }
        }

        LogicalJoin newJoinRel =
            bottomJoin.copy(
                bottomJoin.getTraitSet(),
                bottomJoin.getCondition(),
                leftJoinRel,
                rightJoinRel,
                joinType,
                bottomJoin.isSemiJoinDone());

        return newJoinRel;
    }

    /**
     * Sets an array to reflect how much each index corresponding to a field
     * needs to be adjusted. The array corresponds to fields in a 3-way join
     * between (X, Y, and Z). X remains unchanged, but Y and Z need to be
     * adjusted by some fixed amount as determined by the input.
     *
     * @param adjustments array to be filled out
     * @param nFieldsX number of fields in X
     * @param nFieldsY number of fields in Y
     * @param nFieldsZ number of fields in Z
     * @param adjustY the amount to adjust Y by
     * @param adjustZ the amount to adjust Z by
     */
    private void setJoinAdjustments(
        int[] adjustments,
        int nFieldsX,
        int nFieldsY,
        int nFieldsZ,
        int adjustY,
        int adjustZ) {
        for (int i = 0; i < nFieldsX; i++) {
            adjustments[i] = 0;
        }
        for (int i = nFieldsX; i < (nFieldsX + nFieldsY); i++) {
            adjustments[i] = adjustY;
        }
        for (int i = nFieldsX + nFieldsY;
             i < (nFieldsX + nFieldsY + nFieldsZ);
             i++) {
            adjustments[i] = adjustZ;
        }
    }
}
