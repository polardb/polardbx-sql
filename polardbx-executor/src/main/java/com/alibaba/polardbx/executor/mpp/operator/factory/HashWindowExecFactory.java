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

import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.HashWindowExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HashWindowExecFactory extends ExecutorFactory {

    private final HashWindow overWindow;
    private final int parallelism;
    private List<Executor> executors = new ArrayList<>();

    private final SpillerFactory spillerFactory;

    private final List<DataType> inputDataTypes;

    private final int expectedOutputRowCount;

    public HashWindowExecFactory(HashWindow overWindow, int parallelism, int taskNumber, SpillerFactory spillerFactory,
                                 Integer rowCount, List<DataType> inputDataTypes) {
        this.overWindow = overWindow;
        this.parallelism = parallelism;
        this.spillerFactory = spillerFactory;
        this.inputDataTypes = inputDataTypes;
        this.expectedOutputRowCount = rowCount / (taskNumber * parallelism);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutors(context);
        return executors.get(index);
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutors(context);
    }

    private synchronized List<Executor> createAllExecutors(ExecutionContext context) {
        if (executors.isEmpty()) {
            ImmutableBitSet groupSet = overWindow.groups.get(0).keys;
            int[] groups = AggregateUtils.convertBitSet(groupSet);

            int estimateHashTableSize = AggregateUtils.estimateHashTableSize(expectedOutputRowCount, context);
            for (int j = 0; j < parallelism; j++) {
                MemoryAllocatorCtx memoryAllocator = context.getMemoryPool().getMemoryAllocatorCtx();
                List<DataType> outputDataTypes = overWindow.groups.get(0).getAggregateCalls(overWindow).stream()
                    .map(call -> DataTypeUtil.calciteToDrdsType(call.getType()))
                    .collect(Collectors.toList());
                // notice: filter args in window function is always -1
                List<Aggregator> aggregators =
                    AggregateUtils.convertAggregators(inputDataTypes, outputDataTypes,
                        overWindow.groups.get(0).getAggregateCalls(overWindow), context,
                        memoryAllocator);

                HashWindowExec exec =
                    new HashWindowExec(inputDataTypes, groups, aggregators,
                        CalciteUtils.getTypes(overWindow.getRowType()),
                        estimateHashTableSize, spillerFactory, context);
                exec.setId(overWindow.getRelatedId());
                if (context.getRuntimeStatistics() != null) {
                    RuntimeStatHelper.registerStatForExec(overWindow, exec, context);
                }
                executors.add(exec);
            }
        }
        return executors;
    }
}
