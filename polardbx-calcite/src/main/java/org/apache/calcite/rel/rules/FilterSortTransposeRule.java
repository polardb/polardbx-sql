/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that pushes
 * a {@link LogicalFilter}
 * past a {@link LogicalSort}.
 */
public class FilterSortTransposeRule extends RelOptRule {
    /**
     * The default instance of
     * {@link FilterSortTransposeRule}.
     */
    public static final FilterSortTransposeRule INSTANCE =
        new FilterSortTransposeRule(Filter.class, Sort.class,
            RelFactories.LOGICAL_BUILDER);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FilterSortTransposeRule.
     *
     * <p>If {@code filterFactory} is null, creates the same kind of filter as
     * matched in the rule. Similarly {@code projectFactory}.</p>
     */
    public FilterSortTransposeRule(
        Class<? extends Filter> filterClass,
        Class<? extends Sort> sortClass,
        RelBuilderFactory relBuilderFactory) {
        this(
            operand(filterClass,
                operand(sortClass, any())), relBuilderFactory);
    }

    protected FilterSortTransposeRule(
        RelOptRuleOperand operand,
        RelBuilderFactory relBuilderFactory) {
        super(operand, relBuilderFactory, null);
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final Sort sort = call.rel(1);

        if (sort.getInputs().size() > 1) {
            return;
        }

        if (sort.withLimit()) {
            return;
        }

        RelNode newFilter = filter.copy(filter.getTraitSet(), sort.getInput(), filter.getCondition());
        RelNode newSort = sort.copy(sort.getTraitSet(), newFilter);

        call.transformTo(newSort);
    }
}

// End FilterSortTransposeRule.java
