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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;

import java.util.List;

public class RFBuilderProjectTransposeRule extends RelOptRule {

    public static final RFBuilderProjectTransposeRule INSTANCE =
        new RFBuilderProjectTransposeRule();

    RFBuilderProjectTransposeRule() {
        super(operand(RuntimeFilterBuilder.class,
            operand(LogicalProject.class, any())), "RFBuilderProjectTransposeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

        final RuntimeFilterBuilder filter = call.rel(0);
        final LogicalProject project = call.rel(1);

        if (RexOver.containsOver(project.getProjects(), null)) {
            // In general a filter cannot be pushed below a windowing calculation.
            // Applying the filter before the aggregation function changes
            // the results of the windowing invocation.
            //
            // When the filter is on the PARTITION BY expression of the OVER clause
            // it can be pushed down. For now we don't support this.
            return;
        }

        RexNode newCondition = RelOptUtil.pushPastProject(filter.getCondition(), project);
        List<RexNode> conditions = RelOptUtil.conjunctions(newCondition);
        boolean supportPushDown = true;
        for (RexNode condition : conditions) {
            if (condition instanceof RexCall) {
                supportPushDown = ((RexCall) condition).getOperands().stream().anyMatch(t -> t instanceof RexInputRef);
            } else {
                supportPushDown = false;
            }
            if (!supportPushDown) {
                return;
            }
        }

        RelNode inputOfProject = project.getInput();

        RuntimeFilterUtil.updateBuildFunctionNdv(inputOfProject, conditions);

        RuntimeFilterBuilder newFilterBuilder = filter.copy(
            filter.getTraitSet(), ImmutableList.of(inputOfProject), newCondition);
        RelNode newProject = project.copy(project.getTraitSet(), ImmutableList.of(newFilterBuilder));
        call.transformTo(newProject);
    }
}
