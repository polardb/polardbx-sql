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

import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;

public class ProjectSortTransitiveRule extends RelOptRule {
    public static final ProjectSortTransitiveRule INSTANCE =
        new ProjectSortTransitiveRule(operand(LogicalProject.class, operand(Sort.class, any())),
            "ProjectSortTransitiveRule");

    public static final ProjectSortTransitiveRule TABLELOOKUP =
        new ProjectSortTransitiveRule(
            operand(LogicalProject.class, operand(LogicalSort.class, operand(LogicalTableLookup.class, any()))),
            "ProjectSortTransitiveRule:TABLELOOKUP");

    public static final ProjectSortTransitiveRule LOGICALVIEW =
        new ProjectSortTransitiveRule(
            operand(LogicalProject.class, operand(LogicalSort.class, operand(LogicalView.class, none()))),
            "ProjectSortTransitiveRule:LOGICALVIEW");

    private ProjectSortTransitiveRule(RelOptRuleOperand op, String desc) {
        super(op, RelFactories.LOGICAL_BUILDER, desc);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Sort sort = call.rel(1);
        return sort instanceof LogicalSort || sort instanceof MemSort || sort instanceof Limit || sort instanceof TopN;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final Sort sort = call.rel(1);

        if (OptimizerUtils.hasSubquery(project)) {
            return;
        }

        final Mappings.TargetMapping map =
            RelOptUtil.permutationIgnoreCast(
                project.getProjects(), project.getInput().getRowType());

        Mapping inverseMapping = map.inverse();

        List<RexNode> topProjectChildExps = new ArrayList<>();
        // identity project
        for (int i = 0; i < project.getChildExps().size(); i++) {
            topProjectChildExps.add(new RexInputRef(i, project.getChildExps().get(i).getType()));
        }

        List<RexNode> bottomProjectChildExps = new ArrayList<>();
        bottomProjectChildExps.addAll(project.getChildExps());
        List<RelDataTypeField> bottomProjectRelDataTypeFieldList = new ArrayList<>(project.getRowType().getFieldList());

        List<RelFieldCollation> newRelFieldCollationList = new ArrayList<>();

        for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
            if (inverseMapping.getTargetOpt(fc.getFieldIndex()) < 0) {
                newRelFieldCollationList.add(fc.copy(bottomProjectChildExps.size()));
                bottomProjectRelDataTypeFieldList.add(sort.getRowType().getFieldList().get(fc.getFieldIndex()));
                bottomProjectChildExps.add(new RexInputRef(fc.getFieldIndex(),
                    sort.getRowType().getFieldList().get(fc.getFieldIndex()).getType()));
            } else {
                newRelFieldCollationList.add(fc.copy(inverseMapping.getTargetOpt(fc.getFieldIndex())));
            }
        }

        RelCollation newRelCollation = RelCollations.of(newRelFieldCollationList);

        RelDataType bottomProjectRowType = new RelRecordType(bottomProjectRelDataTypeFieldList);

        // can not purge any column, bail out.
        if (bottomProjectRowType.getFieldCount() >= sort.getInput().getRowType().getFieldCount()) {
            return;
        }

        LogicalProject bottomProject = project.copy(
            project.getTraitSet().replace(RelCollations.EMPTY).replace(RelDistributions.ANY), sort.getInput(),
            bottomProjectChildExps,
            bottomProjectRowType,
            bottomProjectRowType);

        if (ProjectRemoveRule.isTrivial(bottomProject)) {
            return; // KEY! trivial bottomProject means rule used again or this transformation isn't helpful for column pruning
        }

        final Sort newSort =
            sort.copy(
                sort.getTraitSet().replace(RelCollationTraitDef.INSTANCE.canonize(newRelCollation)),
                bottomProject,
                newRelCollation,
                sort.offset,
                sort.fetch);

        LogicalProject topProject =
            project.copy(project.getTraitSet(),
                newSort, topProjectChildExps, project.getRowType());
        if (ProjectRemoveRule.isTrivial(topProject)) {
            call.transformTo(newSort);
        } else {
            call.transformTo(topProject);
        }
    }
}
