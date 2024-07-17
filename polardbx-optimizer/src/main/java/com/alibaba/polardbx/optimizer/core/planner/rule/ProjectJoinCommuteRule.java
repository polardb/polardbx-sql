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

import com.alibaba.polardbx.optimizer.core.planner.OneStepTransformer;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Util;

/**
 * @author dylan
 */
public class ProjectJoinCommuteRule extends RelOptRule {

    /**
     * Instance of the rule that only swaps inner joins.
     */
    public static final ProjectJoinCommuteRule INSTANCE = new ProjectJoinCommuteRule(false);

    /**
     * Instance of the rule that swaps outer joins as well as inner joins.
     */
    public static final ProjectJoinCommuteRule SWAP_OUTER = new ProjectJoinCommuteRule(true);

    private final boolean swapOuter;

    public ProjectJoinCommuteRule(Class<? extends Join> clazz,
                                  RelBuilderFactory relBuilderFactory, boolean swapOuter) {
        super(operand(LogicalProject.class, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION,
                operand(clazz, null, RelOptUtil.NO_COLLATION_AND_DISTRIBUTION, any())),
            relBuilderFactory, null);
        this.swapOuter = swapOuter;
    }

    private ProjectJoinCommuteRule(boolean swapOuter) {
        this(LogicalJoin.class, RelFactories.LOGICAL_BUILDER, swapOuter);
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
        LogicalProject logicalProject = call.rel(0);
        LogicalJoin join = call.rel(1);
        if (join.getJoinReorderContext().isHasCommute() || join.getJoinReorderContext().isHasExchange()) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        LogicalProject logicalProject = call.rel(0);
        LogicalJoin join = call.rel(1);

        if (Util.projectHasSubQuery(logicalProject)) {
            return;
        }

        if (!join.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return;
        }

        final RelNode swapped = JoinCommuteRule.swap(join, this.swapOuter);
        if (swapped == null) {
            return;
        }

        // The result is either a Project or, if the project is trivial, a
        // raw Join.
        final Join newJoin =
            swapped instanceof Join
                ? (Join) swapped
                : (Join) swapped.getInput(0);

        if (newJoin instanceof LogicalJoin) {
            ((LogicalJoin) newJoin).getJoinReorderContext().setHasCommute(true);
        }

        LogicalProject newLogicalProject = logicalProject.copy(logicalProject.getTraitSet(),
            swapped, logicalProject.getProjects(), logicalProject.getRowType());

        // if swapped is project, merge it
        if (swapped instanceof LogicalProject) {
            RelNode output = OneStepTransformer.transform(newLogicalProject, ProjectMergeRule.INSTANCE);
            output = OneStepTransformer.transform(output, ProjectRemoveRule.INSTANCE);
            call.transformTo(output);
        } else {
            call.transformTo(newLogicalProject);
        }
    }
}
