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
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class GenXplanRule extends RelOptRule {

    public static final GenXplanRule INSTANCE = new GenXplanRule();

    protected GenXplanRule() {
        super(operand(LogicalView.class, RelOptRule.none()),
            "GenXplanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getBoolean(ConnectionParams.CONN_POOL_XPROTO_XPLAN);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalView logicalView = call.rel(0);
        logicalView.getXPlan();
    }
}