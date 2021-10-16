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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * @author dylan
 */
public abstract class AbstractInnerJoinRightAssociateRule extends RelOptRule {

    public AbstractInnerJoinRightAssociateRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                                               String description) {
        super(operand, relBuilderFactory, description);
    }

    protected RelNode transform(final LogicalJoin topJoin, final LogicalJoin bottomJoin, RelBuilder relBuilder) {
        return null;
    }

    protected void onMatchInstance(RelOptRuleCall call) {
        final LogicalJoin topJoin = call.rel(0);
        final LogicalJoin bottomJoin = call.rel(1);
        RelNode output = transform(topJoin, bottomJoin, call.builder());
        if (output != null) {
            call.transformTo(output);
        }
    }

    protected void onMatchProjectInstance(RelOptRuleCall call) {
        final LogicalJoin inputTopJoin = call.rel(0);
        final LogicalProject logicalProject = call.rel(1);
        final LogicalJoin bottomJoin = call.rel(2);
        final RelNode relC = call.rel(3);

        LogicalJoin beforeProjectPullUpJoin = inputTopJoin.copy(
            inputTopJoin.getTraitSet(),
            inputTopJoin.getCondition(),
            logicalProject,
            relC,
            inputTopJoin.getJoinType(),
            inputTopJoin.isSemiJoinDone());

        RelNode afterProjectPullUpJoin =
            OneStepTransformer.transform(beforeProjectPullUpJoin, JoinProjectTransposeRule.LEFT_PROJECT);
        if (afterProjectPullUpJoin == beforeProjectPullUpJoin) {
            return;
        }

        assert afterProjectPullUpJoin instanceof LogicalProject;

        LogicalProject newLogicalProject = (LogicalProject) afterProjectPullUpJoin;

        LogicalJoin topJoin = (LogicalJoin) newLogicalProject.getInput();

        RelNode transformResult = transform(topJoin, bottomJoin, call.builder());

        if (transformResult != null) {
            newLogicalProject.replaceInput(0, transformResult);
            RelNode output = OneStepTransformer.transform(newLogicalProject, ProjectMergeRule.INSTANCE);
            output = OneStepTransformer.transform(output, ProjectRemoveRule.INSTANCE);
            call.transformTo(output);
        }
    }
}
