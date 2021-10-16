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

import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

/**
 * @author dylan
 */
public class ExpandViewPlanRule extends RelOptRule {

    public ExpandViewPlanRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final ExpandViewPlanRule INSTANCE = new ExpandViewPlanRule(operand(ViewPlan.class, none()),
        "ExpandViewPlanRule");

    @Override
    public void onMatch(RelOptRuleCall call) {
        ViewPlan viewPlan = call.rel(0);
        call.transformTo(viewPlan.getPlan());
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return true;
    }
}
