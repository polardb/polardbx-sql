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

import com.alibaba.polardbx.optimizer.core.function.SqlSequenceFunction;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class MergeUnionValuesRule extends RelOptRule {

    public static final UnionValuesRule UNION_VALUES_INSTANCE = new UnionValuesRule();
    public static final RemoveProjectValueRule REMOVE_PROJECT_INSTANCE = new RemoveProjectValueRule();

    protected MergeUnionValuesRule(RelOptRuleOperand operand, String description) {
        super(operand, RelFactories.LOGICAL_BUILDER, description);
    }

    public static class UnionValuesRule extends MergeUnionValuesRule {

        public UnionValuesRule() {
            super(
                operand(LogicalUnion.class, any()),
                "UnionValuesRule");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalUnion union = call.rel(0);
            if (union.all) {
                LogicalDynamicValues dynamicValues = null;
                List<RelNode> unionInputs = union.getInputs();
                List<ImmutableList<RexNode>> oldTuples = new ArrayList<>(unionInputs.size());
                for (int i = 0; i < unionInputs.size(); i++) {
                    RelNode unionInput = ((HepRelVertex) unionInputs.get(i)).getCurrentRel();
                    if (unionInput instanceof LogicalProject) {
                        LogicalProject project = (LogicalProject) unionInput;
                        final boolean valuesChild =
                            Optional.ofNullable(((LogicalProject) unionInput).getInput()).filter(
                                r -> ((HepRelVertex) r).getCurrentRel() instanceof LogicalValues).isPresent();

                        //in order to support select test_seq.nextval as abc from dual where count = 18;
                        boolean existSequece = project.getProjects().stream()
                            .anyMatch(t -> (t instanceof RexCall) && (((RexCall) t).op instanceof SqlSequenceFunction));

                        if (OptimizerUtils.hasSubquery(unionInput) || !valuesChild || existSequece) {
                            // Cannot replace LogicalProject with LogicalDynamicValues,
                            // if scalar subquery or child relnode exists
                            break;
                        }
                        LogicalValues values =
                            (LogicalValues) ((HepRelVertex) ((LogicalProject) unionInput).getInput()).getCurrentRel();
                        List<ImmutableList<RexNode>> tuples =
                            reWriteProjects(project.getChildExps(), values.getTuples());
                        oldTuples.addAll(tuples);
                    } else {
                        break;
                    }
                }
                if (oldTuples.size() == unionInputs.size()) {
                    dynamicValues = LogicalDynamicValues.createDrdsValues(union.getCluster(),
                        union.getTraitSet(),
                        union.getRowType(),
                        ImmutableList.copyOf(oldTuples));
                    call.transformTo(dynamicValues);
                }
            }
        }
    }

    public static class RemoveProjectValueRule extends MergeUnionValuesRule {

        public RemoveProjectValueRule() {
            super(operand(LogicalProject.class,
                    operand(LogicalDynamicValues.class, any())),
                "RemoveProjectValueRule");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final LogicalProject project = call.rel(0);
            // if the project node contains un-pushable function, don't transform in order to the insert rel node.
            List<RexNode> exps = project.getChildExps();
            for (RexNode node : exps) {
                if (RexUtil.containsUnPushableFunction(node, false)) {
                    return false;
                }
            }

            return !OptimizerUtils.hasSubquery(project);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            LogicalProject project = (LogicalProject) call.rels[0];
            LogicalDynamicValues dynamicValues = (LogicalDynamicValues) call.rels[1];

            List<ImmutableList<RexNode>> tuples = reWriteProjects(project.getProjects(), dynamicValues.getTuples());
            LogicalDynamicValues newValues = LogicalDynamicValues.createDrdsValues(project.getCluster(),
                project.getTraitSet(),
                project.getRowType(),
                ImmutableList.copyOf(tuples));

            call.transformTo(newValues);
        }
    }

    public static List<RexNode> reWriteProject(List<RexNode> projects, List<? extends RexNode> lookupList) {
        final List<RexNode> list = new ArrayList<>();
        new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return lookupList.get(ref.getIndex());
            }
        }.visitList(projects, list);
        return list;
    }

    public static List<ImmutableList<RexNode>> reWriteProjects(
        List<RexNode> projects, List<? extends List<? extends RexNode>> lookupList) {
        List<ImmutableList<RexNode>> tuples = new ArrayList<>();
        for (List<? extends RexNode> rexNodes : lookupList) {
            List<RexNode> newProjects = reWriteProject(projects, rexNodes);

            ImmutableList<RexNode> rets = ImmutableList.copyOf(newProjects);
            tuples.add(rets);
        }
        return tuples;
    }
}
