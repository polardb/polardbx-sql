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
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class LogicalAggToHashAggRule extends ConverterRule {

    public static final LogicalAggToHashAggRule INSTANCE = new LogicalAggToHashAggRule();

    public LogicalAggToHashAggRule() {
        super(LogicalAggregate.class, Convention.NONE, DrdsConvention.INSTANCE, "LogicalAggToHashAggRule");
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_HASH_AGG);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalAggregate agg = (LogicalAggregate) rel;
        RelNode input = agg.getInput();

        RelTraitSet inputTraitSet = rel.getCluster().getPlanner().emptyTraitSet().replace(DrdsConvention.INSTANCE);
        RelNode newInput = convert(input, inputTraitSet);

        HashAgg hashAgg = HashAgg.create(
            agg.getTraitSet().replace(DrdsConvention.INSTANCE),
            newInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());

        return hashAgg;
    }

}
