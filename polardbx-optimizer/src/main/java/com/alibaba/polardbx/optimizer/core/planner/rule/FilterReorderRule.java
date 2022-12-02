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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

public class FilterReorderRule extends RelOptRule {

    public FilterReorderRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final FilterReorderRule INSTANCE = new FilterReorderRule(
        operand(Filter.class, RelOptRule.any()), "filter_reorder");

    @Override
    public boolean matches(RelOptRuleCall call) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(call.rels[0]).getParamManager();
        return paramManager.getBoolean(ConnectionParams.ENABLE_FILTER_REORDER);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = (Filter) call.rels[0];
        RelNode input = filter.getInput();
        List<RexNode> conditions = RelOptUtil.conjunctions(filter.getCondition());

        RexBuilder rb = call.builder().getRexBuilder();
        List<RexNode> results = RelOptUtil.reorderFilters(conditions, filter.getInput());
        if (results == null) {
            return;
        }
        if (filter instanceof LogicalFilter) {
            call.transformTo(LogicalFilter.create(input,
                RexUtil.flatten(rb, rb.makeCall(AND, results)),
                ImmutableSet.<CorrelationId>builder().addAll(filter.getVariablesSet()).build()));
        } else if (filter instanceof PhysicalFilter) {
            call.transformTo(PhysicalFilter.create(input,
                RexUtil.flatten(rb, rb.makeCall(AND, results)),
                ImmutableSet.<CorrelationId>builder().addAll(filter.getVariablesSet()).build()));
        }
    }

}
