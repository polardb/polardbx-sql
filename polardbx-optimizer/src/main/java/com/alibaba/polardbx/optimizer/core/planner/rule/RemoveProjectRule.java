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

import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.optimizer.core.planner.rule.MergeUnionValuesRule.reWriteProject;
import static com.alibaba.polardbx.optimizer.core.planner.rule.MergeUnionValuesRule.reWriteProjects;

/**
 * @author minggong.zm 2018/1/29
 */
public class RemoveProjectRule extends RelOptRule {

    public RemoveProjectRule(RelOptRuleOperand operand, String description) {
        super(operand, "Tddl_insert_values_rule:" + description);
    }

    public static final RemoveProjectRule INSTANCE = new RemoveProjectRule(operand(LogicalInsert.class,
        operand(LogicalProject.class, none())), "remove_project");

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalInsert modify = (LogicalInsert) call.rels[0];
        LogicalProject project = (LogicalProject) call.rels[1];
        if (!modify.isInsert() && !modify.isReplace()) {
            return;
        }
        // if the project node contains un-pushable function, don't transform the insert rel node.
        List<RexNode> exps = project.getChildExps();
        for (RexNode node : exps) {
            if (RexUtils.containsUnPushableFunction(node, false)) {
                return;
            }
        }

        LogicalDynamicValues dynamicValues = null;
        RelNode projectChild = ((HepRelVertex) project.getInput()).getCurrentRel();
        if (projectChild instanceof LogicalValues) {
            LogicalValues oldValues = (LogicalValues) projectChild;
            List<ImmutableList<RexNode>> tuples = reWriteProjects(project.getChildExps(), oldValues.getTuples());

            dynamicValues = LogicalDynamicValues.createDrdsValues(oldValues.getCluster(),
                oldValues.getTraitSet(),
                modify.getInsertRowType(), // If inserted value is an
                // expression, project.getRowType
                ImmutableList.copyOf(tuples));
        } else if (projectChild instanceof LogicalDynamicValues) {
            LogicalDynamicValues child = (LogicalDynamicValues) projectChild;
            if (child.getTuples().size() == 1) {
                List<ImmutableList<RexNode>> tuples = reWriteProjects(project.getProjects(), child.getTuples());
                dynamicValues = LogicalDynamicValues.createDrdsValues(project.getCluster(),
                    project.getTraitSet(),
                    modify.getInsertRowType(),
                    ImmutableList.copyOf(tuples));
            }
        } else if (projectChild instanceof LogicalProject) {
            LogicalProject midProject = (LogicalProject) projectChild;
            List<ImmutableList<RexNode>> tuples = new ArrayList<>();
            List<RexNode> newProjects = reWriteProject(project.getChildExps(), midProject.getChildExps());
            ImmutableList<RexNode> rets = ImmutableList.copyOf(newProjects);
            tuples.add(rets);
            dynamicValues = LogicalDynamicValues.createDrdsValues(midProject.getCluster(),
                midProject.getTraitSet(),
                modify.getInsertRowType(),
                ImmutableList.copyOf(tuples));
        } else if (projectChild instanceof LogicalUnion) {
            LogicalUnion union = (LogicalUnion) projectChild;
            List<RelNode> unionInputs = union.getInputs();
            List<List<RexNode>> oldTuples = new ArrayList<>(unionInputs.size());
            for (int i = 0; i < unionInputs.size(); i++) {
                RelNode unionInput = ((HepRelVertex) unionInputs.get(i)).getCurrentRel();
                if (unionInput instanceof LogicalProject) {
                    final boolean valuesChild = Optional.ofNullable(((LogicalProject) unionInput).getInput()).filter(
                        r -> ((HepRelVertex) r).getCurrentRel() instanceof LogicalValues).isPresent();
                    if (OptimizerUtils.hasSubquery(unionInput) || !valuesChild) {
                        // Cannot replace LogicalProject with LogicalDynamicValues, if scalar subquery or child relnode exists
                        break;
                    }
                    LogicalProject midProject = (LogicalProject) unionInput;
                    oldTuples.add(midProject.getChildExps());
                } else {
                    break;
                }
            }
            if (oldTuples.size() == unionInputs.size()) {
                List<ImmutableList<RexNode>> tuples = reWriteProjects(project.getChildExps(), oldTuples);
                dynamicValues = LogicalDynamicValues.createDrdsValues(union.getCluster(),
                    union.getTraitSet(),
                    modify.getInsertRowType(),
                    ImmutableList.copyOf(tuples));
            }
        }

        if (dynamicValues != null) {
            List<RelNode> inputs = new ArrayList<>();
            inputs.add(dynamicValues);
            LogicalInsert newModify = (LogicalInsert) modify.copy(modify.getTraitSet(), inputs);

            call.transformTo(newModify);
        }
    }
}
