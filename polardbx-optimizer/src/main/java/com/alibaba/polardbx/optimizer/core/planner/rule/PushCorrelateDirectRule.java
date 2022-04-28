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

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;

/**
 * @author shengyu
 */
public class PushCorrelateDirectRule extends PushCorrelateRule {
    public PushCorrelateDirectRule(RelOptRuleOperand operand, String description) {
        super(operand, "PushCorrelateDirectRule:" + description);
    }

    public static final PushCorrelateDirectRule INSTANCE = new PushCorrelateDirectRule(
        operand(Correlate.class, some(operand(LogicalView.class, none()), operand(RelNode.class, any()))),
        "INSTANCE");

    /**
     * used to unwrap subquery in LogicalView
     */
    @Override
    public boolean matches(RelOptRuleCall call) {
        // return !PlannerContext.getPlannerContext(call).getParamManager().getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP);
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // TODO: implement this part
        return;
    }
}
