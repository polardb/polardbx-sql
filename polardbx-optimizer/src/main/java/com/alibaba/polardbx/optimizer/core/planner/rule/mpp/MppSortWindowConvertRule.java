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

import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Window;

import java.util.Arrays;
import java.util.List;

/**
 * @author xiaoying
 */
public class MppSortWindowConvertRule extends ConverterRule {

    public static final MppSortWindowConvertRule INSTANCE = new MppSortWindowConvertRule();

    public MppSortWindowConvertRule() {
        super(SortWindow.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppWindowFunctionsConvertRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        SortWindow overWindow = (SortWindow) call.rels[0];
        return overWindow.getTraitSet().containsIfApplicable(DrdsConvention.INSTANCE);
    }

    @Override
    public RelNode convert(RelNode rel) {
        SortWindow sortWindow = (SortWindow) rel;
        RelNode input = sortWindow.getInput();
        List<Integer> keyInnerIndexes = sortWindow.groups.get(0).keys.toList();
        for (int i = 1; i < sortWindow.groups.size(); i++) {
            Window.Group group = sortWindow.groups.get(i);
            if (keyInnerIndexes.size() < group.keys.cardinality()) {
                keyInnerIndexes = group.keys.toList();
            }

        }
        List<Integer> groupSet = Lists.newArrayList(keyInnerIndexes);

        //exchange
        //sort
        RelDistribution relDistribution = RelDistributions.SINGLETON;
        if (keyInnerIndexes.size() == 0) {
            input = convert(input, input.getTraitSet().replace(relDistribution).replace(MppConvention.INSTANCE));
        } else {
            RelCollation relCollation = sortWindow.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
            relDistribution = RelDistributions.hash(groupSet);
            input = convert(input,
                input.getTraitSet().replace(MppConvention.INSTANCE).replace(relDistribution).replace(relCollation));
        }
        SortWindow newSortWindow = sortWindow.copy(
            sortWindow.getTraitSet().replace(MppConvention.INSTANCE).replace(RelDistributions.ANY),
            Arrays.asList(input));

        return newSortWindow;
    }
}

