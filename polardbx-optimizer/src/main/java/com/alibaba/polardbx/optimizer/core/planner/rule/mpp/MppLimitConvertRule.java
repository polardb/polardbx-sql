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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

/**
 * @author dylan
 */
public class MppLimitConvertRule extends RelOptRule {

    public static final MppLimitConvertRule INSTANCE = new MppLimitConvertRule();

    private MppLimitConvertRule() {
        super(operand(Limit.class, any()), "MppLimitConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Limit limit = call.rel(0);
        return limit.getConvention() == DrdsConvention.INSTANCE;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        transformToOnePhaseLimit(call);
        transformToTwoPhaseLimit(call);
    }

    private void transformToOnePhaseLimit(RelOptRuleCall call) {
        Limit limit = call.rel(0);
        RelNode limitInput = limit.getInput();
        RelNode input = convert(limitInput, limitInput.getTraitSet().replace(MppConvention.INSTANCE));
        RelNode ensureNode = convert(input, input.getTraitSet().replace(RelDistributions.SINGLETON));
        Limit globalLimit = limit.copy(
            limit.getTraitSet().replace(MppConvention.INSTANCE).replace(RelDistributions.SINGLETON),
            ensureNode, limit.getCollation(), limit.offset, limit.fetch);
        call.transformTo(globalLimit);
    }

    private void transformToTwoPhaseLimit(RelOptRuleCall call) {
        Limit limit = call.rel(0);
        RelNode limitInput = limit.getInput();
        RelNode input = convert(limitInput, limitInput.getTraitSet().replace(MppConvention.INSTANCE));
        RexNode fetch = RuleUtils.getPartialFetch(limit);

        Limit partialLimit = limit.copy(
            limitInput.getTraitSet().replace(MppConvention.INSTANCE),
            input, limit.getCollation(), null, fetch);

        RelNode ensureNode = convert(partialLimit,
            partialLimit.getTraitSet().replace(RelDistributions.SINGLETON));

        Limit globalLimit = limit.copy(
            limit.getTraitSet().replace(MppConvention.INSTANCE).replace(RelDistributions.SINGLETON),
            ensureNode, limit.getCollation(), limit.offset, limit.fetch);

        call.transformTo(globalLimit);
    }
}
