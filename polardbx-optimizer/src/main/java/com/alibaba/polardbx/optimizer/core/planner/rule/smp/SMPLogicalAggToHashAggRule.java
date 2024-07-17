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

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalAggToHashAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class SMPLogicalAggToHashAggRule extends LogicalAggToHashAggRule {

    public static final LogicalAggToHashAggRule INSTANCE = new SMPLogicalAggToHashAggRule("INSTANCE");

    public SMPLogicalAggToHashAggRule(String desc) {
        super("SMP_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createHashAgg(RelOptRuleCall call, LogicalAggregate agg, RelNode newInput) {
        HashAgg hashAgg = HashAgg.create(
            agg.getTraitSet().replace(outConvention),
            newInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());

        call.transformTo(hashAgg);
    }
}
