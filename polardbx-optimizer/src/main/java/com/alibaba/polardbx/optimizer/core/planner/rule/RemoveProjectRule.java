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

import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
            if (RexUtils.containsUnPushableFunction(node)) {
                return;
            }
        }

        LogicalDynamicValues dynamicValues = null;
        RelNode projectChild = ((HepRelVertex) project.getInput()).getCurrentRel();
        if (projectChild instanceof LogicalValues) {
            LogicalValues oldValues = (LogicalValues) projectChild;
            ImmutableList<ImmutableList<RexNode>> tuples = getTuples(project.getChildExps(), oldValues.getTuples());
            dynamicValues = LogicalDynamicValues.createDrdsValues(oldValues.getCluster(),
                oldValues.getTraitSet(),
                modify.getInsertRowType(), // If inserted value is an
                // expression, project.getRowType
                tuples);
        } else if (projectChild instanceof LogicalDynamicValues) {
            LogicalDynamicValues child = (LogicalDynamicValues) projectChild;
            if (child.getTuples().size() == 1) {
                ImmutableList<ImmutableList<RexNode>> tuples = getSingleTuple(project.getChildExps(),
                    child.getTuples().get(0));
                dynamicValues = LogicalDynamicValues.createDrdsValues(project.getCluster(),
                    project.getTraitSet(),
                    modify.getInsertRowType(),
                    tuples);
            }
        } else if (projectChild instanceof LogicalProject) {
            LogicalProject midProject = (LogicalProject) projectChild;
            ImmutableList<ImmutableList<RexNode>> tuples = getSingleTuple(project.getChildExps(),
                midProject.getChildExps());
            dynamicValues = LogicalDynamicValues.createDrdsValues(midProject.getCluster(),
                midProject.getTraitSet(),
                modify.getInsertRowType(),
                tuples);
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
                ImmutableList<ImmutableList<RexNode>> tuples = getTuples(project.getChildExps(), oldTuples);
                dynamicValues = LogicalDynamicValues.createDrdsValues(union.getCluster(),
                    union.getTraitSet(),
                    modify.getInsertRowType(),
                    tuples);
            }
        }

        if (dynamicValues != null) {
            List<RelNode> inputs = new ArrayList<>();
            inputs.add(dynamicValues);
            LogicalInsert newModify = (LogicalInsert) modify.copy(modify.getTraitSet(), inputs);

            call.transformTo(newModify);
        }
    }

    /**
     * When a default value is used, other values will be RexInputRef instead of
     * RexLiteral. We must transform RexInputRef to RexLiteral. If it's a
     * RexDynamicParam, keep it as it was.
     *
     * @param exps LogicalProject > exps
     * @param lookupList LogicalProject > LogicalProject > exps
     */
    public static ImmutableList<ImmutableList<RexNode>> getSingleTuple(List<RexNode> exps, List<RexNode> lookupList) {
        List<RexNode> tuple = new ArrayList<>(exps.size());
        for (RexNode exp : exps) {
            if (exp instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) exp;
                int index = inputRef.getIndex();
                RexNode newExp = lookupList.get(index);
                tuple.add(newExp);
            } else {
                tuple.add(exp);
            }
        }
        return ImmutableList.of(ImmutableList.copyOf(tuple));
    }

    /**
     * When a default value is used, other values will be RexInputRef instead of
     * RexLiteral. We must transform RexInputRef to RexLiteral. If it's a
     * RexDynamicParam, keep it as it was.
     *
     * @param exps LogicalProject > exps
     * @param lookupList LogicalProject > LogicalValues > tuples or LogicalUnion
     * > LogicalProject > exps
     */
    public static ImmutableList<ImmutableList<RexNode>> getTuples(List<RexNode> exps,
                                                                  List<? extends List<? extends RexNode>> lookupList) {
        List<ImmutableList<RexNode>> newTuples = new ArrayList<>(lookupList.size());
        for (int i = 0; i < lookupList.size(); i++) {
            List<? extends RexNode> oldTuple = lookupList.get(i);
            List<RexNode> newTuple = new ArrayList<>(exps.size());
            for (RexNode exp : exps) {
                if (exp instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) exp;
                    int index = inputRef.getIndex();
                    RexNode newExp = oldTuple.get(index);
                    newTuple.add(newExp);
                } else {
                    newTuple.add(exp);
                }
            }
            newTuples.add(ImmutableList.copyOf(newTuple));
        }
        return ImmutableList.copyOf(newTuples);
    }
}
