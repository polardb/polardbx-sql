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

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSortToSortRule;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;

public class SMPLogicalSortToSortRule extends LogicalSortToSortRule {
    public static final SMPLogicalSortToSortRule INSTANCE = new SMPLogicalSortToSortRule(false, "INSTANCE");

    public static final SMPLogicalSortToSortRule TOPN = new SMPLogicalSortToSortRule(true, "TOPN");

    private SMPLogicalSortToSortRule(boolean isTopNRule, String desc) {
        super(isTopNRule, "SMP_" + desc);
    }

    @Override
    protected void createSort(RelOptRuleCall call, LogicalSort sort) {
        if (isTopNRule) {
            if (sort.withLimit() && sort.withOrderBy()) {
                RelNode input =
                    convert(sort.getInput(), sort.getInput().getTraitSet().replace(DrdsConvention.INSTANCE));
                call.transformTo(TopN.create(
                    sort.getTraitSet().replace(DrdsConvention.INSTANCE),
                    input, sort.getCollation(), sort.offset, sort.fetch));
            }
        } else {
            RelNode input =
                convert(sort.getInput(), sort.getInput().getTraitSet().replace(DrdsConvention.INSTANCE));
            call.transformTo(convertLogicalSort(sort, input));
        }
    }

    public static RelNode convertLogicalSort(LogicalSort sort, RelNode input) {
        final boolean hasOrdering = sort.withOrderBy();
        final boolean hasLimit = sort.withLimit();

        if (hasOrdering && !hasLimit) {
            RelDistribution relDistribution = sort.getTraitSet().getDistribution();
            return convert(input,
                input.getTraitSet()
                    .replace(DrdsConvention.INSTANCE)
                    .replace(relDistribution)
                    .replace(sort.getCollation()));
        } else if (hasOrdering && hasLimit) {
            return Limit.create(
                sort.getTraitSet().replace(DrdsConvention.INSTANCE),
                convert(input,
                    input.getTraitSet()
                        .replace(DrdsConvention.INSTANCE)
                        .replace(RelDistributions.SINGLETON)
                        .replace(sort.getCollation())), sort.offset, sort.fetch);
        } else { // !hasOrdering
            return Limit.create(
                sort.getTraitSet().replace(DrdsConvention.INSTANCE).replace(RelDistributions.SINGLETON),
                input, sort.offset, sort.fetch);
        }
    }
}

