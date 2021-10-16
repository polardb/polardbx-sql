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

import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.optimizer.core.planner.rule.JoinTableLookupTransposeRule.isRelPushedToPrimary;

/**
 * @author chenmo.cm
 */
public class ProjectTableLookupTransposeRule extends RelOptRule {

    public static final ProjectTableLookupTransposeRule INSTANCE = new ProjectTableLookupTransposeRule();

    public ProjectTableLookupTransposeRule() {
        super(operand(Project.class, operand(LogicalTableLookup.class, any())), "ProjectTableLookupTransposeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalTableLookup tableLookup = call.rel(1);
        // avoid project transpose tablelookup(which has been sort transpose, we need to preserve collation and
        // distribution)
        return tableLookup.getTraitSet().simplify().getTrait(RelCollationTraitDef.INSTANCE).isTop();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project topProject = call.rel(0);
        final LogicalTableLookup tableLookup = call.rel(1);

        if (topProject != null && Util.projectHasSubQuery(topProject)) {
            return;
        }

        final Project bottomProject = tableLookup.getProject();

        final List<RexNode> newProjects = PlannerUtils.mergeProject(topProject, bottomProject, call);
        if (null == newProjects) {
            return;
        }

        final LogicalJoin join = tableLookup.getJoin();
        final RelNode primary = join.getRight();
        Project newTopProj = topProject.copy(topProject.getTraitSet(), join, newProjects, topProject.getRowType());

        final ProjectJoinTranspose projectJoinTranspose = new ProjectJoinTranspose(call, newTopProj, join).invoke();
        if (projectJoinTranspose.isSkipped()) {
            // replace the two projects with a combined projection
            final LogicalTableLookup newTableLookup = tableLookup.copy(
                join.getJoinType(),
                join.getCondition(),
                newProjects,
                topProject.getRowType());
            call.transformTo(newTableLookup);
        } else {
            // push project through join
            newTopProj = projectJoinTranspose.getNewTopProject();
            final Join newJoin = projectJoinTranspose.getNewJoin();
            final Project leftProjRel = projectJoinTranspose.getLeftProjRel();
            final Project rightProjRel = projectJoinTranspose.getRightProjRel();

            RelNode newPrimary = Optional.ofNullable(rightProjRel)
                .map(proj -> proj.copy(proj.getTraitSet(), ImmutableList.of(primary)))
                .orElse(primary);
            final RexNode newJoinCondition = newJoin.getCondition();

            boolean isRelPushedToPrimary2 =
                isRelPushedToPrimary(tableLookup, leftProjRel, newPrimary, newJoinCondition);

            final LogicalTableLookup newTableLookup = tableLookup.copy(
                join.getJoinType(),
                newJoinCondition,
                newTopProj.getProjects(),
                newTopProj.getRowType(),
                leftProjRel,
                newPrimary,
                isRelPushedToPrimary2);
            call.transformTo(newTableLookup);
        }

    }

}
