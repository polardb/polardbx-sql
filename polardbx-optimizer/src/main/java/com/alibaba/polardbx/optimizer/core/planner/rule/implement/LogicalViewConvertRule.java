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

package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public abstract class LogicalViewConvertRule extends RelOptRule {

    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalViewConvertRule(String desc) {
        super(operand(LogicalView.class, Convention.NONE, any()), "DrdsLogicalViewConvertRule:" + desc);
    }

    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        createLogicalview(call, call.rel(0));
    }

    protected abstract void createLogicalview(RelOptRuleCall call, LogicalView logicalView);
}
