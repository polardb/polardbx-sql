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

import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
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
            // only deal with collation
            RelCollation toCollation = required.getTrait(RelCollationTraitDef.INSTANCE);
            RelDistribution toDistribution = required.getTrait(RelDistributionTraitDef.INSTANCE);
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
}
