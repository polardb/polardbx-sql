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

import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.HashGroupJoinExec;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.mpp.operator.factory.HashAggExecutorFactory.MAX_HASH_TABLE_SIZE;
import static com.alibaba.polardbx.executor.mpp.operator.factory.HashAggExecutorFactory.MIN_HASH_TABLE_SIZE;
import static com.alibaba.polardbx.executor.mpp.operator.factory.HashAggExecutorFactory.convertFrom;

public class HashGroupJoinExecutorFactory extends ExecutorFactory {

    private HashGroupJoin hashAggJoin;
    private int parallelism;
    private int taskNumber;

    private List<Executor> executors = new ArrayList<>();

    private RexNode otherCond;
    private boolean maxOneRow;
    private RexNode equalCond;

    private int rowCount;

    List<DataType> outputDataTypes;

    public HashGroupJoinExecutorFactory(HashGroupJoin hashAgg, int parallelism, int taskNumber,
                                        RexNode otherCond, RexNode equalCond, boolean maxOneRow,
                                        Integer rowCount) {
        this.hashAggJoin = hashAgg;
        this.parallelism = parallelism;
        this.taskNumber = taskNumber;
        this.rowCount = rowCount;
        this.otherCond = otherCond;
        this.equalCond = equalCond;
        this.maxOneRow = maxOneRow;
        this.outputDataTypes = CalciteUtils.getTypes(hashAgg.getRowType());
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

            Integer expectedOutputRowCount = rowCount / (taskNumber * parallelism);
            if (expectedOutputRowCount == null) {
                expectedOutputRowCount = MIN_HASH_TABLE_SIZE;
            } else if (expectedOutputRowCount > MAX_HASH_TABLE_SIZE) {
                expectedOutputRowCount = MAX_HASH_TABLE_SIZE;
            } else if (expectedOutputRowCount < MIN_HASH_TABLE_SIZE) {
                expectedOutputRowCount = MIN_HASH_TABLE_SIZE;
            }

            ImmutableBitSet gp = hashAggJoin.getGroupSet();
            int[] groups = convertFrom(gp);

            for (int i = 0; i < parallelism; i++) {
                final Executor outerInput = getInputs().get(0).createExecutor(context, i);
                final Executor innerInput = getInputs().get(1).createExecutor(context, i);
                IExpression otherCondition = convertExpression(otherCond, context);

                List<AggregateCall> aggCalls = new ArrayList<>(hashAggJoin.getAggCallList());
                if (hashAggJoin.getJoinType() != JoinRelType.RIGHT) {
                    int offset = outerInput.getDataTypes().size();
                    for (int j = 0; j < aggCalls.size(); ++j) {
                        List<Integer> aggIndexInProbeChunk =
                            aggCalls.get(j).getArgList().stream().map(t -> t - offset).collect(Collectors.toList());
                        aggCalls.set(j, aggCalls.get(j).copy(aggIndexInProbeChunk));
                    }
                }

                List<EquiJoinKey> joinKeys = EquiJoinUtils
                    .buildEquiJoinKeys(hashAggJoin, hashAggJoin.getOuter(), hashAggJoin.getInner(), (RexCall) equalCond,
                        hashAggJoin.getJoinType());

                MemoryAllocatorCtx memoryAllocator = context.getMemoryPool().getMemoryAllocatorCtx();
                List<DataType> dataTypes = new ArrayList<DataType>() {
                    {
                        addAll(innerInput.getDataTypes());
                    }
                };
                List<Aggregator> aggregators =
                    AggregateUtils.convertAggregators(dataTypes,
                        outputDataTypes.subList(groups.length, groups.length + aggCalls.size()),
                        aggCalls, context, memoryAllocator);

                Executor exec =
                    new HashGroupJoinExec(outerInput, innerInput, hashAggJoin.getJoinType(),
                        outputDataTypes,
                        maxOneRow,
                        joinKeys, otherCondition, null, groups, aggregators,
                        context,
                        expectedOutputRowCount
                    );
                exec.setId(hashAggJoin.getRelatedId());
                if (context.getRuntimeStatistics() != null) {
                    RuntimeStatHelper.registerStatForExec(hashAggJoin, exec, context);
                }
                executors.add(exec);
            }
        }
        return executors;
    }

    private IExpression convertExpression(RexNode rexNode, ExecutionContext context) {
        return RexUtils.buildRexNode(rexNode, context, new ArrayList<>());
    }

}
