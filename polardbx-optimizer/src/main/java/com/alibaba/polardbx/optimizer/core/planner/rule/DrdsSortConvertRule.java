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

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalSort;

public class DrdsSortConvertRule extends ConverterRule {
    public static final DrdsSortConvertRule INSTANCE = new DrdsSortConvertRule(false);

    public static final DrdsSortConvertRule TOPN = new DrdsSortConvertRule(true);

    private final boolean isTopNRule;

    private DrdsSortConvertRule(boolean isTopNRule) {
        super(LogicalSort.class, Convention.NONE, DrdsConvention.INSTANCE, "DrdsSortConvertRule:"
            + (isTopNRule ? "TOPN" : "INSTANCE"));
        this.isTopNRule = isTopNRule;
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalSort sort = (LogicalSort) rel;

        if (isTopNRule) {
            if (sort.withLimit() && sort.withOrderBy()) {
                RelNode input =
                    convert(sort.getInput(), sort.getInput().getTraitSet().replace(DrdsConvention.INSTANCE));
                return TopN.create(
                    sort.getTraitSet().replace(DrdsConvention.INSTANCE),
                    input, sort.getCollation(), sort.offset, sort.fetch);

            } else {
                return null;
            }
        } else {
            RelNode input =
                convert(sort.getInput(), sort.getInput().getTraitSet().replace(DrdsConvention.INSTANCE));
            return convertLogicalSort(sort, input);
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

