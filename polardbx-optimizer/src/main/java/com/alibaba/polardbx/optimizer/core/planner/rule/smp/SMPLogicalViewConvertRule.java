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

package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRuleCall;

public class SMPLogicalViewConvertRule extends LogicalViewConvertRule {
    public static final LogicalViewConvertRule INSTANCE = new SMPLogicalViewConvertRule("INSTANCE");

    SMPLogicalViewConvertRule(String desc) {
        super("SMP_" + desc);
    }

    @Override
    protected void createLogicalview(RelOptRuleCall call, LogicalView logicalView) {
        LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().simplify().replace(outConvention));
        newLogicalView.optimize();
        call.transformTo(newLogicalView);
        // do nothing
    }
}
