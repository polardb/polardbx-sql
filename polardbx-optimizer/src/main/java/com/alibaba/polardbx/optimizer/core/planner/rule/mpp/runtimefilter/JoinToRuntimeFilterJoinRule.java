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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlRuntimeFilterBuildFunction;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class JoinToRuntimeFilterJoinRule extends RelOptRule {

    public static final JoinToRuntimeFilterJoinRule HASHJOIN_INSTANCE =
        new JoinToRuntimeFilterJoinRule(HashJoin.class, "HashJoinToRuntimeFilterJoin");
    public static final JoinToRuntimeFilterJoinRule SEMIJOIN_INSTANCE =
        new JoinToRuntimeFilterJoinRule(SemiHashJoin.class, "SemiJoinToRuntimeFilterJoin");

    private static final String ALL_KEYS = "*";

    JoinToRuntimeFilterJoinRule(Class<? extends Join> joinClass, String desc) {
        super(operand(joinClass, any()), desc);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        boolean enableRuntimeFilter = PlannerContext.getPlannerContext(call).getParamManager().getBoolean(
            ConnectionParams.ENABLE_RUNTIME_FILTER);
        if (!enableRuntimeFilter) {
            return false;
        }

        Join join = call.rel(0);
        boolean runtimeFilterPushedDown = true;
        if (join instanceof HashJoin) {
            runtimeFilterPushedDown = ((HashJoin) join).isRuntimeFilterPushedDown();
        } else if (join instanceof SemiHashJoin) {
            runtimeFilterPushedDown = ((SemiHashJoin) join).isRuntimeFilterPushedDown();
        }

        return (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.SEMI)
            && !runtimeFilterPushedDown;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelNode buildNode = join.getInner();
        RelNode probeNode = join.getOuter();
        if (join instanceof HashJoin) {
            buildNode = ((HashJoin) join).getBuildNode();
            probeNode = ((HashJoin) join).getProbeNode();
        } else if (join instanceof SemiHashJoin) {
            buildNode = ((SemiHashJoin) join).getBuildNode();
            probeNode = ((SemiHashJoin) join).getProbeNode();
        }

        RelDataType buildType = buildNode.getRowType();
        RelDataType probeType = probeNode.getRowType();

        Map<Integer, Integer> buildKeyToRuntimeFilterId = new HashMap<>(8);
        Set<Pair<Integer, Integer>> probeKeyRFilterIdPairs = new HashSet<>(8);

        // create runtime filter
        RelBuilder relBuilder = this.relBuilderFactory.create(join.getCluster(), null);

        RexNode equalCondition = null;
        if (join instanceof HashJoin) {
            equalCondition = ((HashJoin) join).getEqualCondition();
        } else if (join instanceof SemiHashJoin) {
            equalCondition = ((SemiHashJoin) join).getEqualCondition();
        }

        JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), equalCondition);
        ImmutableIntList buildKeys, probeKeys;
        if (buildNode == join.getLeft()) {
            buildKeys = joinInfo.leftKeys;
            probeKeys = joinInfo.rightKeys;
        } else {
            buildKeys = joinInfo.rightKeys;
            probeKeys = joinInfo.leftKeys;
        }

        RelMetadataQuery mq = join.getCluster().getMetadataQuery();
        double buildKeyNdv = RuntimeFilterUtil.convertDouble(
            mq.getDistinctRowCount(buildNode, ImmutableBitSet.of(buildKeys), null));
        Double probeRowCount = RuntimeFilterUtil.convertDouble(mq.getRowCount(probeNode));
        PlannerContext context = PlannerContext.getPlannerContext(join);
        ParamManager paramManager = context.getParamManager();
        float fpp = paramManager.getFloat(ConnectionParams.RUNTIME_FILTER_FPP);

        // Force disable runtime filter when all keys are disabled
        String forceDisableKeys =
            paramManager.getString(ConnectionParams.FORCE_DISABLE_RUNTIME_FILTER_COLUMNS).toUpperCase();
        boolean forceDisabled = Stream.concat(buildKeys.stream().map(k -> buildType.getFieldNames().get(k)),
                probeKeys.stream().map(k -> probeType.getFieldNames().get(k)))
            .map(String::toUpperCase)
            .allMatch(forceDisableKeys::contains);
        if (forceDisabled) {
            //close runtime filter force.
            return;
        }

        String forceEnabledKeys =
            paramManager.getString(ConnectionParams.FORCE_ENABLE_RUNTIME_FILTER_COLUMNS).toUpperCase();
        boolean forceEnabled = ALL_KEYS.equals(forceEnabledKeys) || buildKeys.stream()
            .map(k -> buildType.getFieldNames().get(k))
            .map(String::toUpperCase)
            .allMatch(forceEnabledKeys::contains);

        if (!forceEnabled) {
            if ((probeRowCount < paramManager.getLong(ConnectionParams.RUNTIME_FILTER_PROBE_MIN_ROW_COUNT))) {
                return;
            }

            // This is a simplified method of estimating bloom filter size since in current implementation
            // we always split join keys
            if (BloomFilterUtil.optimalNumOfBits((long) buildKeyNdv, fpp) > paramManager
                .getLong(ConnectionParams.BLOOM_FILTER_MAX_SIZE)) {
                return;
            }

            if (!RuntimeFilterUtil.satisfyFilterRatio(paramManager, buildKeyNdv, probeRowCount, fpp)) {
                return;
            }
        }

        boolean usingXxHash = paramManager.getBoolean(ConnectionParams.ENABLE_RUNTIME_FILTER_XXHASH);
        List<RexNode> buildConditions = new ArrayList<>();
        List<RexNode> probeConditions = new ArrayList<>();
        for (int i = 0; i < buildKeys.size(); i++) {
            int buildKey = buildKeys.get(i);
            int probeKey = probeKeys.get(i);

            // Verify data type matches
            DataType<?> buildKeyType = DataTypeUtil.calciteToDrdsType(buildType.getFieldList().get(buildKey).getType());
            DataType<?> probeKeyType = DataTypeUtil.calciteToDrdsType(probeType.getFieldList().get(probeKey).getType());

            if (!DataTypeUtil.equalsSemantically(buildKeyType, probeKeyType) || !RuntimeFilterUtil
                .supportsRuntimeFilter(buildKeyType)) {
                // Skip if this type can't be pushed down
                continue;
            }

            double buildNdv = RuntimeFilterUtil.convertDouble(mq.getDistinctRowCount(
                buildNode, ImmutableBitSet.of(buildKeys.get(i)), null));
            double probeCount = RuntimeFilterUtil.convertDouble(mq.getRowCount(probeNode));
            double guessSelectivity = 0.25;
            if (buildNdv > 0 && probeCount > 0) {
                guessSelectivity = Math.max(buildNdv / probeCount * (1 - fpp), guessSelectivity);
            }

            if (!buildKeyToRuntimeFilterId.containsKey(buildKey)) {
                int runtimeFilterId = context.nextRuntimeFilterId();
                SqlRuntimeFilterBuildFunction buildFunction = new SqlRuntimeFilterBuildFunction(
                    ImmutableList.of(runtimeFilterId), buildKeyNdv);
                RexInputRef buildRexNode = new RexInputRef(
                    buildKeys.get(i), buildType.getFieldList().get(buildKeys.get(i)).getType());
                buildConditions.add(relBuilder.call(buildFunction, buildRexNode));
                buildKeyToRuntimeFilterId.put(buildKey, runtimeFilterId);
            }

            int runTimeFilterId = buildKeyToRuntimeFilterId.get(buildKey);
            Pair<Integer, Integer> probeKeyRFilterIdPair = Pair.of(probeKeys.get(i), runTimeFilterId);
            if (!probeKeyRFilterIdPairs.contains(probeKeyRFilterIdPair)) {
                SqlRuntimeFilterFunction filterFunction =
                    new SqlRuntimeFilterFunction(runTimeFilterId, guessSelectivity, usingXxHash);
                RexInputRef probeRexNode = new RexInputRef(
                    probeKeys.get(i), probeType.getFieldList().get(probeKeys.get(i)).getType());
                probeConditions.add(relBuilder.call(filterFunction, probeRexNode));
                probeKeyRFilterIdPairs.add(probeKeyRFilterIdPair);
            }
        }

        if (buildConditions.isEmpty()) {
            return;
        }

        RexNode buildCondition = RexUtil
            .composeConjunction(
                join.getCluster().getRexBuilder(), buildConditions, true);
        RexNode probeCondition = RexUtil.composeConjunction(
            join.getCluster().getRexBuilder(), probeConditions, true);

        RuntimeFilterBuilder buildFilterInput = RuntimeFilterBuilder.create(
            buildNode, buildCondition);
        PhysicalFilter probFilterInput = PhysicalFilter.create(
            probeNode, probeCondition);

        RelNode leftNode = (buildNode == join.getLeft()) ? buildFilterInput : probFilterInput;
        RelNode rightNode = (buildNode == join.getLeft()) ? probFilterInput : buildFilterInput;

        if (join instanceof HashJoin) {
            HashJoin newHashJoin = ((HashJoin) join).copy(
                join.getTraitSet(),
                join.getCondition(),
                leftNode,
                rightNode,
                join.getJoinType(),
                join.isSemiJoinDone(),
                true);
            call.transformTo(newHashJoin);
        } else if (join instanceof SemiHashJoin) {
            SemiHashJoin newHashJoin = ((SemiHashJoin) join).copy(
                join.getTraitSet(),
                join.getCondition(),
                leftNode,
                rightNode,
                join.getJoinType(),
                join.isSemiJoinDone(),
                true);
            call.transformTo(newHashJoin);
        }
    }
}
