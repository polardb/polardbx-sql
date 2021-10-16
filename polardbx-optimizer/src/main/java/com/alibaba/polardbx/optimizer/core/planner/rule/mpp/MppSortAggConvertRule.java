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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.SortAgg;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class MppSortAggConvertRule extends RelOptRule {

    public static final MppSortAggConvertRule INSTANCE = new MppSortAggConvertRule();

    public MppSortAggConvertRule() {
        super(operand(SortAgg.class, any()), "MppSortAggConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        SortAgg agg = (SortAgg) call.rels[0];
        return agg.getTraitSet().containsIfApplicable(DrdsConvention.INSTANCE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SortAgg sortAgg = call.rel(0);
        RelNode input = sortAgg.getInput();
        ImmutableBitSet groupIndex = sortAgg.getGroupSet();

        // <aggDistribution, inputRelTraitSet>
        List<Pair<RelDistribution, RelTraitSet>> implementationList = new ArrayList<>();

        if (groupIndex.cardinality() == 0) {
            implementationList.add(Pair.of(RelDistributions.SINGLETON,
                input.getTraitSet().replace(RelDistributions.SINGLETON).replace(MppConvention.INSTANCE)));
        } else {
            //exchange
            RelDistribution inputDistribution = RelDistributions.hash(groupIndex.toList());
            RelDistribution aggDistribution =
                RelDistributions.hash(ImmutableIntList.identity(groupIndex.cardinality()));
            // use input collation

            implementationList.add(Pair.of(aggDistribution,
                input.getTraitSet().replace(inputDistribution).replace(MppConvention.INSTANCE)));

            if (PlannerContext.getPlannerContext(sortAgg).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_SHUFFLE_BY_PARTIAL_KEY)
                && groupIndex.cardinality() > 1) {
                for (int i = 0; i < groupIndex.cardinality(); i++) {
                    List<Integer> aggDistributionKeyList = new ArrayList<>();
                    aggDistributionKeyList.add(i);
                    aggDistribution = RelDistributions.hash(aggDistributionKeyList);
                    List<Integer> inputDistributionKeyList = new ArrayList<>();
                    int cnt = 0;
                    for (int j = 0; j < groupIndex.size(); j++) {
                        if (groupIndex.get(j)) {
                            cnt++;
                            if (cnt == i + 1) {
                                inputDistributionKeyList.add(j);
                                break;
                            }
                        }
                    }
                    inputDistribution = RelDistributions.hash(inputDistributionKeyList);
                    implementationList.add(Pair.of(aggDistribution,
                        input.getTraitSet().replace(inputDistribution).replace(MppConvention.INSTANCE)));
                }
            }
        }

        for (Pair<RelDistribution, RelTraitSet> implementation : implementationList) {
            SortAgg newSortAgg = sortAgg.copy(
                sortAgg.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                convert(input, implementation.right),
                sortAgg.indicator,
                sortAgg.getGroupSet(),
                sortAgg.getGroupSets(),
                sortAgg.getAggCallList());

            if (groupIndex.cardinality() != 0
                && sortAgg.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                RelNode output = convert(newSortAgg, newSortAgg.getTraitSet().replace(RelDistributions.SINGLETON));
                call.transformTo(output);
            } else {
                call.transformTo(newSortAgg);
            }
        }

    }
}

