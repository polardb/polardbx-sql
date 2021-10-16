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
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

public abstract class AbstractAggregsteJoinTransposeRule extends RelOptRule {
    public AbstractAggregsteJoinTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                                              String description) {
        super(operand, relBuilderFactory, description);
    }

    protected RelNode transform(final LogicalAggregate aggregate, final LogicalJoin join, final RelBuilder relBuilder,
                                final RexBuilder rexBuilder) {
        return null;
    }

    protected void onMatchInstance(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final LogicalJoin join = call.rel(1);
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        RelNode output = transform(aggregate, join, relBuilder, rexBuilder);
        if (output != null) {
            call.transformTo(output);
        }
    }

    protected void onMatchProjectInstance(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

        RelNode afterMergeProjectNode = OneStepTransformer.transform(aggregate, AggregateProjectMergeRule.INSTANCE);
        if (afterMergeProjectNode == aggregate) {
            return;
        }

        if (afterMergeProjectNode instanceof LogicalAggregate && afterMergeProjectNode
            .getInput(0) instanceof LogicalJoin) {
            RelNode transformReuslt =
                transform((LogicalAggregate) afterMergeProjectNode, (LogicalJoin) afterMergeProjectNode.getInput(0),
                    relBuilder, rexBuilder);
            if (transformReuslt != null) {
                call.transformTo(transformReuslt);
            }
        }
    }
}
