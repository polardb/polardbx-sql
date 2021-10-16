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

import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * @author shengyu
 * Planner rule to flatten a tree of LogicalJoin}s
 * into a single DrdsMultiJoin with N inputs.
 */
public class LogicalJoinToMultiJoinRule extends JoinToMultiJoinRule {
    static final LogicalJoinToMultiJoinRule INSTANCE =
        new LogicalJoinToMultiJoinRule(LogicalJoin.class, RelFactories.LOGICAL_BUILDER);

    public LogicalJoinToMultiJoinRule(Class<? extends Join> clazz,
                                      RelBuilderFactory relBuilderFactory) {
        super(clazz, relBuilderFactory);
    }

    /**
     * control whether or not to invoke this rule
     *
     * @param call Rule call which has been determined to match all operands of
     * this rule
     * @return true if should use this rule
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call)
            .isShouldUseHeuOrder();
    }

}