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

package com.alibaba.polardbx.optimizer.core;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.mpp.ColumnarExchange;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;

public class DrdsConvention extends Convention.Impl {
    public static final DrdsConvention INSTANCE = new DrdsConvention();

    private DrdsConvention() {
        super("DRDS", RelNode.class);
    }

    @Override
    public boolean canConvertConvention(Convention toConvention) {
        return toConvention == MppConvention.INSTANCE;
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
                                                      RelTraitSet toTraits) {
        return true;
    }

    @Override
    public RelNode enforce(final RelNode input, final RelTraitSet required) {
        if (input.getConvention() == Convention.NONE) {
            // left Convert Rule to deal with
            return null;
        } else if (input.getConvention() == DrdsConvention.INSTANCE) {
            RelCollation toCollation = required.getTrait(RelCollationTraitDef.INSTANCE);
            RelDistribution toDistribution = required.getTrait(RelDistributionTraitDef.INSTANCE);
            if (CBOUtil.isColumnarOptimizer(input)) {
                RelNode output = input;
                if (toDistribution.getType() == RelDistribution.Type.SINGLETON
                    && !RuleUtils.satisfyDistribution(toDistribution, input)) {
                    // use merge sort + exchange(single)
                    output = ensureCollation(output, toCollation);
                    output = ColumnarExchange.create(output, toCollation, RelDistributions.SINGLETON);
                } else if (!toCollation.getFieldCollations().isEmpty()
                    && RuleUtils.satisfyCollation(toCollation, input)
                    && !RuleUtils.satisfyDistribution(toDistribution, input)) {
                    output = ColumnarExchange.create(output, toCollation, toDistribution);
                } else {
                    output = ensureDistribution(output, toDistribution);
                    output = ensureCollation(output, toCollation);
                }
                return output;
            }
            // only deal with collation
            if (!RuleUtils.satisfyCollation(toCollation, input)) {
                RelTraitSet emptyTraitSet = input.getCluster().getPlanner().emptyTraitSet();
                MemSort memSort = MemSort.create(
                    emptyTraitSet.replace(DrdsConvention.INSTANCE).replace(toCollation).replace(toDistribution),
                    input,
                    toCollation);
                return memSort;
            } else {
                return input;
            }
        } else {
            throw new AssertionError("Unable to convert input to " + INSTANCE + ", input = " + input);
        }
    }

    private static RelNode ensureDistribution(RelNode node, RelDistribution requiredDistribution) {
        if (!RuleUtils.satisfyDistribution(requiredDistribution, node)) {
            ColumnarExchange mppExchange = ColumnarExchange.create(node, requiredDistribution);
            return mppExchange;
        } else {
            return node;
        }
    }

    private static RelNode ensureCollation(RelNode node, RelCollation requiredCollation) {
        if (!RuleUtils.satisfyCollation(requiredCollation, node)) {
            MemSort memSort = MemSort.create(
                node.getTraitSet().replace(DrdsConvention.INSTANCE),
                node,
                requiredCollation);
            return memSort;
        } else {
            return node;
        }
    }
}
