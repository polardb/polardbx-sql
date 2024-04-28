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

package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.CBOPushAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalAggToHashAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class COLLogicalAggToHashAggRule extends LogicalAggToHashAggRule {

    public static final LogicalAggToHashAggRule INSTANCE = new COLLogicalAggToHashAggRule("INSTANCE");

    public COLLogicalAggToHashAggRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createHashAgg(RelOptRuleCall call, LogicalAggregate agg, RelNode newInput) {
        List<Pair<RelDistribution, RelDistribution>> implementationList = new ArrayList<>();
        if (tryConvertToPartialAgg(call, agg, newInput)) {
            if (PlannerContext.getPlannerContext(agg).getParamManager().getBoolean(ConnectionParams.PARTIAL_AGG_ONLY)) {
                return;
            }
        }
        ImmutableBitSet groupIndex = agg.getGroupSet();
        if (groupIndex.cardinality() == 0) {
            implementationList.add(Pair.of(RelDistributions.SINGLETON, RelDistributions.SINGLETON));
        } else {
            if (PlannerContext.getPlannerContext(agg).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_AGG)) {
                int inputLoc = -1;
                for (int i = 0; i < groupIndex.cardinality(); i++) {
                    inputLoc = groupIndex.nextSetBit(inputLoc + 1);
                    RelDistribution aggDistribution = RelDistributions.hashOss(ImmutableList.of(i),
                        PlannerContext.getPlannerContext(agg).getColumnarMaxShardCnt());
                    RelDistribution inputDistribution = RelDistributions.hashOss(ImmutableList.of(inputLoc),
                        PlannerContext.getPlannerContext(agg).getColumnarMaxShardCnt());
                    implementationList.add(Pair.of(aggDistribution, inputDistribution));
                }
            } else {
                RelDistribution aggDistribution =
                    RelDistributions.hash(ImmutableIntList.identity(groupIndex.cardinality()));
                RelDistribution inputDistribution = RelDistributions.hash(groupIndex.toList());
                implementationList.add(Pair.of(aggDistribution, inputDistribution));
            }
        }
        for (Pair<RelDistribution, RelDistribution> implementation : implementationList) {
            HashAgg newAgg = HashAgg.create(
                agg.getTraitSet().replace(outConvention).replace(implementation.getKey()),
                convert(newInput, newInput.getTraitSet().replace(implementation.getValue())),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());
            call.transformTo(newAgg);
        }
    }

    protected boolean tryConvertToPartialAgg(RelOptRuleCall call, LogicalAggregate agg, RelNode input) {
        if (!checkPartialAggPass(agg)) {
            return false;
        }

        // split to two phase agg
        CBOPushAggRule.TwoPhaseAggComponent twoPhaseAggComponent = CBOPushAggRule.splitAgg(agg);
        if (twoPhaseAggComponent == null) {
            return false;
        }
        List<RexNode> childExps = twoPhaseAggComponent.getProjectChildExps();
        List<AggregateCall> globalAggCalls = twoPhaseAggComponent.getGlobalAggCalls();
        ImmutableBitSet globalAggGroupSet = twoPhaseAggComponent.getGlobalAggGroupSet();
        List<AggregateCall> partialAggCalls = twoPhaseAggComponent.getPartialAggCalls();
        ImmutableBitSet partialAggGroupSet = twoPhaseAggComponent.getPartialAggGroupSet();

        // no distribution needed
        RelNode partialHashAgg =
            HashAgg.createPartial(agg.getTraitSet().replace(outConvention),
                input,
                partialAggGroupSet,
                agg.getGroupSets(),
                partialAggCalls);

        List<Pair<RelDistribution, RelDistribution>> implementationList = new ArrayList<>();
        ImmutableBitSet groupIndex = agg.getGroupSet();

        if (groupIndex.isEmpty()) {
            implementationList.add(Pair.of(RelDistributions.SINGLETON, RelDistributions.SINGLETON));
        }
        // pairwise agg
        if (PlannerContext.getPlannerContext(agg).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_AGG)) {
            for (int i = 0; i < groupIndex.cardinality(); i++) {
                RelDistribution aggDistribution = RelDistributions.hashOss(ImmutableList.of(i),
                    PlannerContext.getPlannerContext(agg).getColumnarMaxShardCnt());
                RelDistribution inputDistribution = RelDistributions.hashOss(ImmutableList.of(i),
                    PlannerContext.getPlannerContext(agg).getColumnarMaxShardCnt());
                implementationList.add(Pair.of(aggDistribution, inputDistribution));
            }
        } else {
            implementationList.add(Pair.of(
                RelDistributions.hash(ImmutableIntList.identity(groupIndex.cardinality())),
                RelDistributions.hash(ImmutableIntList.identity(groupIndex.cardinality()))));
        }

        for (Pair<RelDistribution, RelDistribution> implementation : implementationList) {
            HashAgg globalHashAgg = HashAgg.create(
                agg.getTraitSet().replace(outConvention).replace(implementation.getValue()),
                convert(partialHashAgg, partialHashAgg.getTraitSet().replace(implementation.getKey())),
                globalAggGroupSet,
                ImmutableList.of(globalAggGroupSet),
                globalAggCalls);

            LogicalProject project = new LogicalProject(agg.getCluster(),
                globalHashAgg.getTraitSet(),
                globalHashAgg,
                childExps,
                agg.getRowType());
            if (ProjectRemoveRule.isTrivial(project)) {
                call.transformTo(globalHashAgg);
            } else {
                call.transformTo(project);
            }
        }

        return !CollectionUtils.isEmpty(implementationList);
    }

    protected boolean checkPartialAggPass(Aggregate hashAgg) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(hashAgg);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PARTIAL_AGG)) {
            return false;
        }

        // no distinct
        List<AggregateCall> aggregateCalls = hashAgg.getAggCallList();
        for (AggregateCall aggregateCall : aggregateCalls) {
            if (aggregateCall.isDistinct() || aggregateCall.filterArg > -1) {
                return false;
            }
        }

        if (plannerContext.getParamManager().getBoolean(ConnectionParams.PARTIAL_AGG_ONLY)) {
            return true;
        }

        // cardinality good enough
        RelMetadataQuery metadataQuery = hashAgg.getCluster().getMetadataQuery();
        double outputRowCount = metadataQuery.getRowCount(hashAgg);
        double inputRowCount = metadataQuery.getRowCount(hashAgg.getInput());
        double partialAggSelectivityThreshold =
            plannerContext.getParamManager().getFloat(ConnectionParams.PARTIAL_AGG_SELECTIVITY_THRESHOLD);
        if (inputRowCount * partialAggSelectivityThreshold < outputRowCount) {
            return false;
        }
        return true;
    }
}
