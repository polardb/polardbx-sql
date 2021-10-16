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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
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

/**
 * @author minggong.zm 2018/1/29
 */
public class RemoveUnionProjectRule extends RelOptRule {

    public RemoveUnionProjectRule(RelOptRuleOperand operand, String description) {
        super(operand, "Tddl_insert_values_rule:" + description);
    }

    public static final RemoveUnionProjectRule INSTANCE = new RemoveUnionProjectRule(operand(LogicalInsert.class,
        operand(LogicalUnion.class,
            operand(LogicalProject.class,
                operand(LogicalValues.class, none())))),
        // operand(LogicalValues.class,
        // none())),
        "remove_union_project");

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalInsert modify = (LogicalInsert) call.rels[0];
        LogicalUnion union = (LogicalUnion) call.rels[1];
        if (!modify.isInsert() && !modify.isReplace()) {
            return;
        }

        List<RelNode> unionInputs = union.getInputs();
        if (unionInputs.isEmpty()) {
            return;
        }

        LogicalProject oneProject = null;
        List<ImmutableList<RexNode>> exps = new ArrayList<>();
        for (int i = 0; i < unionInputs.size(); i++) {
            LogicalProject project = (LogicalProject) ((HepRelVertex) unionInputs.get(i)).getCurrentRel();
            oneProject = project;
            exps.add((ImmutableList<RexNode>) project.getChildExps());
        }

        // Row type should be modify.insertRowType, not oneProject.getRowType().
        // Row type of LogicalProject isn't necessarily the same with INSERT
        // columns.
        LogicalDynamicValues dynamicValues = LogicalDynamicValues.createDrdsValues(oneProject.getCluster(),
            oneProject.getTraitSet(),
            modify.getInsertRowType(),
            ImmutableList.copyOf(exps));

        List<RelNode> inputs = new ArrayList<>();
        inputs.add(dynamicValues);
        LogicalInsert newModify = (LogicalInsert) modify.copy(modify.getTraitSet(), inputs);

        call.transformTo(newModify);
    }
}
