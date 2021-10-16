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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;

public class FilterExchangeTransposeRule extends RelOptRule {

    public static final FilterExchangeTransposeRule INSTANCE =
        new FilterExchangeTransposeRule();

    FilterExchangeTransposeRule() {
        super(operand(Filter.class,
            operand(Exchange.class, any())), "FilterExchangeTransposeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        Exchange exchange = call.rel(1);
        RelNode inputOfExchange = exchange.getInput();
        RelNode newFilter = filter.copy(
            inputOfExchange.getTraitSet(), ImmutableList.of(inputOfExchange));
        RelNode newExchange = exchange.copy(exchange.getTraitSet(), ImmutableList.of(newFilter));
        call.transformTo(newExchange);
    }
}
