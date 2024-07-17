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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.ParallelHashJoinExec;
import com.alibaba.polardbx.executor.operator.Synchronizer;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelHashJoinExecutorFactory extends ExecutorFactory {
    private boolean driverBuilder;

    private PipelineFragment pipelineFragment;

    private Join join;
    private List<Executor> executors = new ArrayList<>();
    private RexNode otherCond;
    private boolean maxOneRow;
    private List<RexNode> operands;
    private RexNode equalCond;
    private int probeParallelism;
    private int numPartitions;
    private boolean streamJoin;

    int localPartitionCount;

    boolean keepPartition;

    public ParallelHashJoinExecutorFactory(PipelineFragment pipelineFragment,
                                           Join join, RexNode otherCond, RexNode equalCond, boolean maxOneRow,
                                           List<RexNode> operands, ExecutorFactory build, ExecutorFactory probe,
                                           int probeParallelism, int numPartitions, boolean driverBuilder,
                                           int localPartitionCount, boolean keepPartition) {
        this.pipelineFragment = pipelineFragment;

        this.join = join;
        this.otherCond = otherCond;
        this.equalCond = equalCond;
        this.maxOneRow = maxOneRow;
        this.operands = operands;
        this.probeParallelism = probeParallelism;
        this.numPartitions = numPartitions;
        addInput(build);
        addInput(probe);
        this.driverBuilder = driverBuilder;
        this.localPartitionCount = localPartitionCount;
        this.keepPartition = keepPartition;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutor(context);
        return executors.get(index);
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutor(context);
    }

    private synchronized List<Executor> createAllExecutor(ExecutionContext context) {
        if (executors.isEmpty()) {

            // Parse the item keys from the RelNode of the join.
            List<FragmentRFItemKey> rfItemKeys = FragmentRFItemKey.buildItemKeys(join);

            boolean alreadyUseRuntimeFilter = join instanceof HashJoin && ((HashJoin) join).isRuntimeFilterPushedDown();
            List<Synchronizer> synchronizers = new ArrayList<>();
            List<Integer> assignResult = null;

            List<EquiJoinKey> joinKeys = EquiJoinUtils
                .buildEquiJoinKeys(join, join.getOuter(), join.getInner(), (RexCall) equalCond, join.getJoinType());
            IExpression otherCondition = convertExpression(otherCond, context);

            boolean useBloomFilter =
                context.getParamManager().getBoolean(ConnectionParams.ENABLE_HASH_TABLE_BLOOM_FILTER);
            int chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);

            if (localPartitionCount > 0) {
                Map<Integer, Integer> partitionParallelism = new HashMap<>();
                // local partition wise mode
                assignResult = ExecUtils.assignPartitionToExecutor(localPartitionCount, probeParallelism);
                for (Integer i : assignResult) {
                    partitionParallelism.merge(i, 1, Integer::sum);
                }
                int hashTableNum = Math.min(localPartitionCount, probeParallelism);

                int[] partitionsOfEachBucket = ExecUtils.partitionsOfEachBucket(localPartitionCount, probeParallelism);
                for (int hashTableIndex = 0; hashTableIndex < hashTableNum; hashTableIndex++) {

                    // how many degrees of parallelism in this synchronizer instance.
                    int numberOfExec = localPartitionCount >= probeParallelism
                        ? 1 : partitionParallelism.get(hashTableIndex);

                    // how many data partitions in this synchronizer instance.
                    int numberOfPartition = partitionsOfEachBucket[hashTableIndex];

                    Synchronizer synchronizer =
                        new Synchronizer(join.getJoinType(), driverBuilder, numberOfExec, alreadyUseRuntimeFilter,
                            useBloomFilter, chunkLimit, numberOfExec, numberOfPartition, false);

                    synchronizers.add(synchronizer);
                }

                // If using fragment-level runtime filter in this join operator, register the
                // synchronizer into RF-manager.
                if (pipelineFragment.getFragmentRFManager() != null) {

                    for (int itemKeyIndex = 0; itemKeyIndex < rfItemKeys.size(); itemKeyIndex++) {
                        FragmentRFItemKey itemKey = rfItemKeys.get(itemKeyIndex);

                        // Find the ordinal in the build side.
                        int ordinal;
                        for (ordinal = 0; ordinal < joinKeys.size(); ordinal++) {
                            // Consider two cases:
                            // 1. join input side is not reversed
                            // 2. join input side is reversed
                            if ((!driverBuilder && joinKeys.get(ordinal).getInnerIndex() == itemKey.getBuildIndex())
                                || (driverBuilder
                                && joinKeys.get(ordinal).getOuterIndex() == itemKey.getBuildIndex())) {

                                // Find the items from pipeline fragment that matching the given item key.
                                Map<FragmentRFItemKey, FragmentRFItem> allItems =
                                    pipelineFragment.getFragmentRFManager().getAllItems();
                                FragmentRFItem rfItem;
                                if ((rfItem = allItems.get(itemKey)) != null) {
                                    // The channel for build side in join operator is the ordinal of the item key.
                                    rfItem.setBuildSideChannel(ordinal);

                                    // register multiple synchronizer and share the rf item.
                                    rfItem.registerBuilder(
                                        ordinal, probeParallelism, synchronizers.toArray(new Synchronizer[0]));
                                }
                            }
                        }

                    }
                }

            } else {
                Synchronizer synchronizer =
                    new Synchronizer(join.getJoinType(), driverBuilder, numPartitions,
                        alreadyUseRuntimeFilter,
                        useBloomFilter, chunkLimit, probeParallelism, -1, true);

                synchronizers.add(synchronizer);

                // If using fragment-level runtime filter in this join operator, register the
                // synchronizer into RF-manager.
                if (pipelineFragment.getFragmentRFManager() != null) {

                    for (int itemKeyIndex = 0; itemKeyIndex < rfItemKeys.size(); itemKeyIndex++) {
                        FragmentRFItemKey itemKey = rfItemKeys.get(itemKeyIndex);

                        // Find the ordinal in the build side.
                        int ordinal;
                        for (ordinal = 0; ordinal < joinKeys.size(); ordinal++) {

                            // Consider two cases:
                            // 1. join input side is not reversed
                            // 2. join input side is reversed
                            if ((!driverBuilder && joinKeys.get(ordinal).getInnerIndex() == itemKey.getBuildIndex())
                                || (driverBuilder
                                && joinKeys.get(ordinal).getOuterIndex() == itemKey.getBuildIndex())) {

                                // Find the items from pipeline fragment that matching the given item key.
                                Map<FragmentRFItemKey, FragmentRFItem> allItems =
                                    pipelineFragment.getFragmentRFManager().getAllItems();
                                FragmentRFItem rfItem;
                                if ((rfItem = allItems.get(itemKey)) != null) {
                                    // The channel for build side in join operator is the ordinal of the item key.
                                    rfItem.setBuildSideChannel(ordinal);

                                    // register multiple synchronizer and share the rf item.
                                    rfItem.registerBuilder(ordinal, probeParallelism, synchronizer);
                                }
                            }
                        }
                    }

                }

            }

            Map<Integer, Integer> partitionOperatorIdx = new HashMap<>();
            for (int i = 0; i < probeParallelism; i++) {
                Executor inner;
                Executor outerInput;
                if (driverBuilder) {
                    outerInput = getInputs().get(0).createExecutor(context, i);
                    inner = getInputs().get(1).createExecutor(context, i);
                } else {
                    inner = getInputs().get(0).createExecutor(context, i);
                    outerInput = getInputs().get(1).createExecutor(context, i);
                }

                List<IExpression> antiJoinOperands = null;
                if (containAntiJoinOperands(operands, join)) {
                    antiJoinOperands =
                        operands.stream().map(ele -> convertExpression(ele, context)).collect(Collectors.toList());
                }

                int operatorIdx = i;
                if (localPartitionCount > 0) {
                    int partition = assignResult.get(i);
                    operatorIdx = partitionOperatorIdx.getOrDefault(partition, 0);
                    partitionOperatorIdx.put(partition, operatorIdx + 1);
                }

                ParallelHashJoinExec exec =
                    new ParallelHashJoinExec(
                        localPartitionCount > 0 ? synchronizers.get(assignResult.get(i)) : synchronizers.get(0),
                        outerInput, inner, join.getJoinType(), maxOneRow,
                        joinKeys, otherCondition, antiJoinOperands, driverBuilder, context, operatorIdx,
                        probeParallelism, keepPartition);
                exec.setStreamJoin(streamJoin);
                registerRuntimeStat(exec, join, context);
                executors.add(exec);
            }
        }
        return executors;
    }

    private IExpression convertExpression(RexNode rexNode, ExecutionContext context) {
        return RexUtils.buildRexNode(rexNode, context, new ArrayList<>());
    }

    public void enableStreamJoin(boolean streamJoin) {
        this.streamJoin = streamJoin;
    }

    public static boolean containAntiJoinOperands(List<RexNode> operands, Join join) {
        return operands != null && join.getJoinType() == JoinRelType.ANTI && !operands.isEmpty();
    }
}
