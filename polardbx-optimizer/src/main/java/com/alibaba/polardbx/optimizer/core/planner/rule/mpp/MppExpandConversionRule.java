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

import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * @author dylan
 */
public class MppExpandConversionRule extends RelOptRule {
    public static final MppExpandConversionRule INSTANCE =
        new MppExpandConversionRule(RelFactories.LOGICAL_BUILDER);

    public MppExpandConversionRule(RelBuilderFactory relBuilderFactory) {
        super(operand(AbstractConverter.class, operand(RelSubset.class, none())), relBuilderFactory,
            "MppExpandConversionRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        AbstractConverter converter = call.rel(0);
        final RelNode child = call.rel(1);
        RelTraitSet toTraitSet = converter.getTraitSet();
        RelTraitSet fromTraitSet = child.getTraitSet();
        return toTraitSet.contains(MppConvention.INSTANCE)
            && !fromTraitSet.satisfies(toTraitSet);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AbstractConverter converter = call.rel(0);
        RelNode child = call.rel(1);
        child = convert(child, child.getTraitSet().replace(MppConvention.INSTANCE));
        RelNode output = enforce(child, converter.getTraitSet());
        call.transformTo(output);
    }

    public static RelNode enforce(RelNode input, RelTraitSet toTraitSet) {
        assert input.getConvention() == MppConvention.INSTANCE;
        RelDistribution toDistribution = toTraitSet.getTrait(RelDistributionTraitDef.INSTANCE);
        RelCollation toCollation = toTraitSet.getTrait(RelCollationTraitDef.INSTANCE);

        RelNode output;

        if (toDistribution.getType() == RelDistribution.Type.SINGLETON
            && !RuleUtils.satisfyDistribution(toDistribution, input)) {
            // use merge sort exchange
            output = ensureCollation(input, toCollation);
            output = MppExchange.create(output, toCollation, RelDistributions.SINGLETON);
        } else if (!toCollation.getFieldCollations().isEmpty()
            && RuleUtils.satisfyCollation(toCollation, input)
            && !RuleUtils.satisfyDistribution(toDistribution, input)) {
            output = MppExchange.create(input, toCollation, toDistribution);
        } else {
            output = ensureDistribution(input, toDistribution);
            output = ensureCollation(output, toCollation);
        }
        return output;
    }

    private static RelNode ensureDistribution(RelNode node, RelDistribution requiredDistribution) {
        if (!RuleUtils.satisfyDistribution(requiredDistribution, node)) {
            MppExchange mppExchange = MppExchange.create(node, requiredDistribution);
            return mppExchange;
        } else {
            return node;
        }
    }

    private static RelNode ensureCollation(RelNode node, RelCollation requiredCollation) {
        if (!RuleUtils.satisfyCollation(requiredCollation, node)) {
            MemSort memSort = MemSort.create(
                node.getTraitSet().replace(MppConvention.INSTANCE),
                node,
                requiredCollation);
            return memSort;
        } else {
            return node;
        }
    }
}


