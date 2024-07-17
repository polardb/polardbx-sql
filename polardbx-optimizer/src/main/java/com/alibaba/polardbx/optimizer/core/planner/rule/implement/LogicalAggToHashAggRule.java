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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

public abstract class LogicalAggToHashAggRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    public LogicalAggToHashAggRule(String desc) {
        super(operand(LogicalAggregate.class, Convention.NONE, any()), "LogicalAggToHashAggRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_HASH_AGG);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        RelNode input = agg.getInput();
        RelTraitSet inputTraitSet = agg.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        RelNode newInput = convert(input, inputTraitSet);

        createHashAgg(call, agg, newInput);
    }

    protected abstract void createHashAgg(
        RelOptRuleCall call,
        LogicalAggregate agg,
        RelNode newInput);
}
