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
import com.alibaba.polardbx.optimizer.core.planner.rule.util.TwoPhaseAggUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class MppHashAggConvertRule extends RelOptRule {

    public static final MppHashAggConvertRule INSTANCE = new MppHashAggConvertRule();

    public MppHashAggConvertRule() {
        super(operand(HashAgg.class, any()), "MppHashAggConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        HashAgg agg = (HashAgg) call.rels[0];
        return agg.getTraitSet().containsIfApplicable(DrdsConvention.INSTANCE);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        HashAgg hashAgg = call.rel(0);
        RelNode input = hashAgg.getInput();

        RelTraitSet emptyTraitSet = hashAgg.getCluster().getPlanner().emptyTraitSet();
        input = convert(input, emptyTraitSet.replace(MppConvention.INSTANCE));

        RelNode node = tryConvertToPartialAgg(hashAgg, input);
        if (node != null) {
            call.transformTo(node);
        }

        ImmutableBitSet groupIndex = hashAgg.getGroupSet();
        // <aggDistribution, inputDistribution>
        List<Pair<RelDistribution, RelDistribution>> implementationList = new ArrayList<>();
        if (groupIndex.cardinality() == 0) {
            implementationList.add(Pair.of(RelDistributions.SINGLETON, RelDistributions.SINGLETON));
        } else {
            RelDistribution aggDistribution =
                RelDistributions.hash(ImmutableIntList.identity(groupIndex.cardinality()));
            RelDistribution inputDistribution = RelDistributions.hash(groupIndex.toList());
            implementationList.add(Pair.of(aggDistribution, inputDistribution));
            if (PlannerContext.getPlannerContext(hashAgg).getParamManager()
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
                    implementationList.add(Pair.of(aggDistribution, inputDistribution));
                }
            }
        }

        for (Pair<RelDistribution, RelDistribution> implementation : implementationList) {
            HashAgg newAgg = hashAgg.copy(
                hashAgg.getTraitSet().replace(MppConvention.INSTANCE).replace(implementation.left),
                convert(input, input.getTraitSet().replace(implementation.right)),
                hashAgg.indicator,
                hashAgg.getGroupSet(),
                hashAgg.getGroupSets(),
                hashAgg.getAggCallList());

            call.transformTo(newAgg);
        }
    }

    private RelNode tryConvertToPartialAgg(HashAgg hashAgg, RelNode input) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(hashAgg);
        boolean enablePartialAgg = plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PARTIAL_AGG);
        if (!enablePartialAgg) {
            return null;
        }
        if (!noDistinctOrFilter(hashAgg)) {
            return null;
        }

        RelMetadataQuery metadataQuery = hashAgg.getCluster().getMetadataQuery();
        double outputRowCount = metadataQuery.getRowCount(hashAgg);
        double inputRowCount = metadataQuery.getRowCount(hashAgg.getInput());
        double partialAggSelectivityThreshold =
            plannerContext.getParamManager().getFloat(ConnectionParams.PARTIAL_AGG_SELECTIVITY_THRESHOLD);
        int partialAggBucketThreshold =
            plannerContext.getParamManager().getInt(ConnectionParams.PARTIAL_AGG_BUCKET_THRESHOLD);
        if (inputRowCount * partialAggSelectivityThreshold < outputRowCount) {
            return null;
        }
        if (outputRowCount > partialAggBucketThreshold) {
            return null;
        }

        TwoPhaseAggUtil.TwoPhaseAggComponent twoPhaseAggComponent = TwoPhaseAggUtil.splitAgg(hashAgg);
        if (twoPhaseAggComponent == null) {
            return null;
        }
        List<RexNode> childExps = twoPhaseAggComponent.getProjectChildExps();
        List<AggregateCall> globalAggCalls = twoPhaseAggComponent.getGlobalAggCalls();
        ImmutableBitSet globalAggGroupSet = twoPhaseAggComponent.getGlobalAggGroupSet();
        List<AggregateCall> partialAggCalls = twoPhaseAggComponent.getPartialAggCalls();
        ImmutableBitSet partialAggGroupSet = twoPhaseAggComponent.getPartialAggGroupSet();

        RelNode partialHashAgg =
            HashAgg.createPartial(hashAgg.getTraitSet().replace(MppConvention.INSTANCE), input, partialAggGroupSet,
                hashAgg.getGroupSets(), // FIXME if groupSets used ?
                partialAggCalls);

        ImmutableBitSet groupIndex = hashAgg.getGroupSet();
        final RelDistribution relDistribution;
        final RelDistribution selfDistribution;
        if (groupIndex.isEmpty()) {
            relDistribution = RelDistributions.SINGLETON;
            selfDistribution = RelDistributions.SINGLETON;
        } else {
            List<Integer> newAggFields = new ArrayList<>(groupIndex.cardinality());
            for (int i = 0; i < groupIndex.cardinality(); i++) {
                newAggFields.add(i);
            }
            relDistribution = RelDistributions.hash(newAggFields);
            selfDistribution = RelDistributions.hash(ImmutableIntList.identity(groupIndex.cardinality()));
        }

        partialHashAgg = convert(partialHashAgg, partialHashAgg.getTraitSet().replace(relDistribution));

        HashAgg globalHashAgg = HashAgg.create(
            hashAgg.getTraitSet().replace(MppConvention.INSTANCE).replace(selfDistribution),
            partialHashAgg,
            globalAggGroupSet,
            ImmutableList.of(globalAggGroupSet),
            // FIXME if groupSets used ?
            globalAggCalls);
        LogicalProject project = new LogicalProject(hashAgg.getCluster(),
            globalHashAgg.getTraitSet(),
            globalHashAgg,
            childExps,
            hashAgg.getRowType());
        if (ProjectRemoveRule.isTrivial(project)) {
            return globalHashAgg;
        } else {
            return project;
        }
    }

    private boolean noDistinctOrFilter(Aggregate hashAgg) {
        for (AggregateCall aggregateCall : hashAgg.getAggCallList()) {
            if (aggregateCall.isDistinct() || aggregateCall.filterArg > -1) {
                return false;
            }
            if (aggregateCall.hasFilter()) {
                return false;
            }
        }
        return true;
    }
}
