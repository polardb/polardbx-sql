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
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.google.common.collect.Lists;
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

public class MppHashWindowConvertRule extends ConverterRule {

    public static final MppHashWindowConvertRule INSTANCE = new MppHashWindowConvertRule();

    public MppHashWindowConvertRule() {
        super(HashWindow.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppHashWindowFunctionsConvertRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        HashWindow overWindow = (HashWindow) call.rels[0];
        return overWindow.getTraitSet().containsIfApplicable(DrdsConvention.INSTANCE);
    }

    @Override
    public RelNode convert(RelNode rel) {
        HashWindow hashWindow = (HashWindow) rel;
        RelNode input = hashWindow.getInput();
        List<Integer> keyInnerIndexes = hashWindow.groups.get(0).keys.toList();
        for (int i = 1; i < hashWindow.groups.size(); i++) {
            Window.Group group = hashWindow.groups.get(i);
            if (keyInnerIndexes.size() < group.keys.cardinality()) {
                keyInnerIndexes = group.keys.toList();
            }

        }
        List<Integer> groupSet = Lists.newArrayList(keyInnerIndexes);

        //exchange
        RelDistribution relDistribution = RelDistributions.SINGLETON;
        if (keyInnerIndexes.size() == 0) {
            input = convert(input, input.getTraitSet().replace(relDistribution).replace(MppConvention.INSTANCE));
        } else {
            RelCollation relCollation = hashWindow.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
            relDistribution = RelDistributions.hash(groupSet);
            input = convert(input,
                input.getTraitSet().replace(MppConvention.INSTANCE).replace(relDistribution).replace(relCollation));
        }
        HashWindow newHashWindow = hashWindow.copy(
            hashWindow.getTraitSet().replace(MppConvention.INSTANCE).replace(relDistribution),
            Arrays.asList(input));

        return newHashWindow;
    }
}

