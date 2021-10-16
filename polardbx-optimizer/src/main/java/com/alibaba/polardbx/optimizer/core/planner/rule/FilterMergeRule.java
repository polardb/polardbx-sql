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

import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexUtil;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

/**
 * Created by fangwu
 */
public class FilterMergeRule extends RelOptRule {

    public FilterMergeRule(RelOptRuleOperand operand, String description) {
        super(operand, "Tddl_push_down_rule:" + description);
    }

    public static final FilterMergeRule INSTANCE = new FilterMergeRule(
        operand(Filter.class, operand(Filter.class, any())), "filter_merge");

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter1 = (Filter) call.rels[0];
        Filter filter2 = (Filter) call.rels[1];

        ImmutableSet.Builder variablesSet = ImmutableSet.builder();
        if (filter1.getVariablesSet() != null) {
            variablesSet.addAll(filter1.getVariablesSet());
        }
        if (filter2.getVariablesSet() != null) {
            variablesSet.addAll(filter2.getVariablesSet());
        }

        RexBuilder rb = call.builder().getRexBuilder();
        // to preserve filter order, filter2 first, filter1 later
        if (filter1 instanceof LogicalFilter) {
            call.transformTo(LogicalFilter.create(filter2.getInput(),
                RexUtil.flatten(rb, rb.makeCall(AND, filter2.getCondition(), filter1.getCondition())),
                variablesSet.build()));
        } else if (filter1 instanceof PhysicalFilter) {
            call.transformTo(PhysicalFilter.create(filter2.getInput(),
                RexUtil.flatten(rb, rb.makeCall(AND, filter2.getCondition(), filter1.getCondition())),
                variablesSet.build()));
        }
    }
}
