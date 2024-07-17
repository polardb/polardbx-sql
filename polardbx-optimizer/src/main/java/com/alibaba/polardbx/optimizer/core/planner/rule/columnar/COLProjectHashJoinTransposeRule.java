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

package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a Project past a HashJoin in columnar mode
 * by splitting the projection into a projection on top of each child of
 * the join.
 */
public class COLProjectHashJoinTransposeRule extends RelOptRule {
    public static final ProjectJoinTransposeRule INSTANCE =
        new ProjectJoinTransposeRule(
            PushProjector.ExprCondition.TRUE,
            RelFactories.LOGICAL_BUILDER);

    /**
     * Condition for expressions that should be preserved in the projection.
     */
    private final PushProjector.ExprCondition preserveExprCondition;

    /**
     * Creates a COLProjectHashJoinTransposeRule with an explicit inputRefThreshold.
     *
     * @param inputRefThreshold the maximum number of input reference in all expressions for project executor.
     */
    public COLProjectHashJoinTransposeRule(int inputRefThreshold) {
        super(
            operand(Project.class,
                operand(Join.class, any())),
            RelFactories.LOGICAL_BUILDER, null);

        this.preserveExprCondition = new PushProjector.InputRefExprCondition(inputRefThreshold);
    }

    public void onMatch(RelOptRuleCall call) {
        Project origProj = call.rel(0);
        final Join join = call.rel(1);

        if (!(join instanceof HashJoin)) {
            return;
        }

        HashJoin hashJoin = (HashJoin) join;

        if (Util.projectHasSubQuery(origProj)) {
            return;
        }

        // locate all fields referenced in the projection and join condition;
        // determine which inputs are referenced in the projection and
        // join condition; if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        PushProjector pushProject = null;
        pushProject =
            new PushProjector(origProj, hashJoin.getCondition(), hashJoin, preserveExprCondition, call.builder());

        if (pushProject.locateAllRefs()) {
            return;
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        RelNode leftProjRel =
            pushProject.createProjectRefsAndExprs(
                hashJoin.getLeft(),
                true,
                false);
        RelNode rightProjRel =
            pushProject.createProjectRefsAndExprs(
                hashJoin.getRight(),
                true,
                true);

        // convert the join condition to reference the projected columns
        RexNode newJoinFilter = null;
        int[] adjustments = pushProject.getAdjustments();
        if (hashJoin.getCondition() != null) {
            List<RelDataTypeField> projJoinFieldList = new ArrayList<>();
            projJoinFieldList.addAll(hashJoin.getSystemFieldList());

            projJoinFieldList.addAll(leftProjRel.getRowType().getFieldList());

            projJoinFieldList.addAll(rightProjRel.getRowType().getFieldList());

            newJoinFilter = pushProject.convertRefsAndExprs(
                hashJoin.getCondition(),
                projJoinFieldList,
                adjustments);
        }

        // create a new join with the projected children
        HashJoin newJoinRel = hashJoin.copy(
            join.getTraitSet(),
            newJoinFilter,
            leftProjRel,
            rightProjRel,
            join.getJoinType(),
            join.isSemiJoinDone());

        // put the original project on top of the join, converting it to
        // reference the modified projection list
        RelNode topProject =
            pushProject.createNewProject(newJoinRel, adjustments);

        call.transformTo(topProject);
    }
}
