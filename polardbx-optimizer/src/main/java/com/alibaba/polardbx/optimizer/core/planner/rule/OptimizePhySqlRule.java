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

public class OptimizePhySqlRule extends RelOptRule {

    public static final OptimizePhySqlRule INSTANCE = new OptimizePhySqlRule();

    protected OptimizePhySqlRule() {
        super(operand(LogicalView.class, RelOptRule.none()),
            "OptimizePhySqlRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // semi join could not exist when ENABLE_LV_SUBQUERY_UNWRAP is disabled
        // thus there is no need to transform
        return PlannerContext.getPlannerContext(call.rels[0]).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalView logicalView = call.rel(0);
        logicalView.optimizePhySql();
    }
}