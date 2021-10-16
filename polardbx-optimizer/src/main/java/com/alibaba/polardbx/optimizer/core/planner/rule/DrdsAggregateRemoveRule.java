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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that removes
 * a {@link org.apache.calcite.rel.core.Aggregate}
 * if it computes no aggregate functions
 * (that is, it is implementing {@code SELECT DISTINCT})
 * and the underlying relational expression is already distinct.
 */
public class DrdsAggregateRemoveRule extends RelOptRule {

    public static final DrdsAggregateRemoveRule INSTANCE = new DrdsAggregateRemoveRule(LogicalAggregate.class);

    /**
     * Creates an AggregateRemoveRule.
     */
    public DrdsAggregateRemoveRule(Class<? extends Aggregate> aggregateClass) {
        // REVIEW jvs 14-Mar-2006: We have to explicitly mention the child here
        // to make sure the rule re-fires after the child changes (e.g. via
        // ProjectRemoveRule), since that may change our information
        // about whether the child is distinct.  If we clean up the inference of
        // distinct to make it correct up-front, we can get rid of the reference
        // to the child here.
        super(
            operand(aggregateClass,
                operand(RelNode.class, any())),
            "DrdsAggregateRemoveRule");
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final RelNode input = call.rel(1);
        if (!aggregate.getAggCallList().isEmpty() || aggregate.getAggOptimizationContext().isAggPushed()
            || aggregate.indicator) {
            return;
        }
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!SqlFunctions.isTrue(mq.areColumnsUnique(input, aggregate.getGroupSet()))) {
            return;
        }

        // Distinct is "GROUP BY c1, c2" (where c1, c2 are a set of columns on
        // which the input is unique, i.e. contain a key) and has no aggregate
        // functions. It can be removed.
        // If aggregate was projecting a subset of columns, add a project for the
        // same effect.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(input);
        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        final List<RelDataTypeField> childFields = input.getRowType().getFieldList();
        for (int i : aggregate.getGroupSet()) {
            projects.add(RexInputRef.of2(i, childFields));
        }
        relBuilder.project(Pair.left(projects), Pair.right(projects));
        call.transformTo(relBuilder.build());
    }
}

// End DrdsAggregateRemoveRule.java