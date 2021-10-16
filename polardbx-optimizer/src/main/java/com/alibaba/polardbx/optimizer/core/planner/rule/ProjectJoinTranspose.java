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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 *
 *  project
 *     |
 *   join
 *   /  \
 *  A    B
 *
 * -----------------------------
 * becomes
 *
 *        newTopProject
 *              |
 *           newJoin
 *           /     \
 *  leftProjRel   rightProjRel
 *       |             |
 *       A             B
 *
 * </pre>
 */
public class ProjectJoinTranspose {
    private boolean skipped;
    private RelOptRuleCall call;
    private final LogicalJoin join;
    private final Project project;
    private Project leftProjRel;
    private Project rightProjRel;
    private Project newTopProject;
    private LogicalJoin newJoin;

    public ProjectJoinTranspose(RelOptRuleCall call, Project project, LogicalJoin join) {
        this.call = call;
        this.join = join;
        this.project = project;
    }

    boolean isSkipped() {
        return skipped;
    }

    public Project getLeftProjRel() {
        return leftProjRel;
    }

    public Project getRightProjRel() {
        return rightProjRel;
    }

    public Project getNewTopProject() {
        return newTopProject;
    }

    public LogicalJoin getNewJoin() {
        return newJoin;
    }

    public ProjectJoinTranspose invoke() {
        // locate all fields referenced in the projection and join condition;
        // determine which inputs are referenced in the projection and
        // join condition; if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        PushProjector pushProject =
            new PushProjector(
                project,
                join.getCondition(),
                join,
                PushProjector.ExprCondition.FALSE,
                call.builder());
        if (pushProject.locateAllRefs()) {
            skipped = true;
            return this;
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        leftProjRel = pushProject.createProjectRefsAndExprs(
            join.getLeft(),
            true,
            false);
        rightProjRel =
            pushProject.createProjectRefsAndExprs(
                join.getRight(),
                true,
                true);

        // convert the join condition to reference the projected columns
        RexNode newJoinFilter = null;
        int[] adjustments = pushProject.getAdjustments();
        if (join.getCondition() != null) {
            List<RelDataTypeField> projJoinFieldList = new ArrayList<>();
            projJoinFieldList.addAll(join.getSystemFieldList());
            projJoinFieldList.addAll(leftProjRel.getRowType().getFieldList());
            projJoinFieldList.addAll(rightProjRel.getRowType().getFieldList());
            newJoinFilter = pushProject.convertRefsAndExprs(join.getCondition(), projJoinFieldList, adjustments);
        }

        // create a new join with the projected children
        this.newJoin =
            join.copy(
                join.getTraitSet(),
                newJoinFilter,
                leftProjRel,
                rightProjRel,
                join.getJoinType(),
                join.isSemiJoinDone());

        // put the original project on top of the join, converting it to
        // reference the modified projection list
        newTopProject = (Project) pushProject.createNewProject(this.newJoin, adjustments);
        skipped = false;
        return this;
    }
}
