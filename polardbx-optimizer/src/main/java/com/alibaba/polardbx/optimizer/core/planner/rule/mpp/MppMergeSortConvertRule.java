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
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * @author dylan
 */
public class MppMergeSortConvertRule extends ConverterRule {

    public static final MppMergeSortConvertRule INSTANCE = new MppMergeSortConvertRule();

    private MppMergeSortConvertRule() {
        super(MergeSort.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppMergeSortConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        MergeSort mergeSort = (MergeSort) rel;
        RelNode input = mergeSort.getInput();

        RelDistribution relDistribution = mergeSort.getTraitSet().getDistribution();

        RelNode output = convert(input,
            input.getTraitSet()
                .replace(MppConvention.INSTANCE)
                .replace(relDistribution)
                .replace(mergeSort.getCollation()));

        final boolean hasLimit = mergeSort.fetch != null;
        if (hasLimit) {
            output = Limit.create(
                output.getTraitSet(),
                output, mergeSort.offset, mergeSort.fetch);
        }
        return output;
    }
}
