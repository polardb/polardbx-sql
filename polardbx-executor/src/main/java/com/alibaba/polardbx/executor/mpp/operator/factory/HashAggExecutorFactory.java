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
import com.alibaba.polardbx.executor.operator.HashAggExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class HashAggExecutorFactory extends ExecutorFactory {

    public static final int MAX_HASH_TABLE_SIZE = 131064;
    public static final int MIN_HASH_TABLE_SIZE = 1024;

    private HashAgg hashAgg;
    private int parallelism;
    private int taskNumber;
    private Integer rowCount;
    private List<Executor> executors = new ArrayList<>();

    private final SpillerFactory spillerFactory;

    private final List<DataType> inputDataTypes;

    public HashAggExecutorFactory(HashAgg hashAgg, int parallelism, int taskNumber, SpillerFactory spillerFactory,
                                  Integer rowCount, List<DataType> inputDataTypes) {
        this.hashAgg = hashAgg;
        this.parallelism = parallelism;
        this.taskNumber = taskNumber;
        this.spillerFactory = spillerFactory;
        this.rowCount = rowCount;
        this.inputDataTypes = inputDataTypes;
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
            ImmutableBitSet gp = hashAgg.getGroupSet();
            int[] groups = convertFrom(gp);

            Integer expectedOutputRowCount = rowCount / (taskNumber * parallelism);
            if (expectedOutputRowCount == null) {
                expectedOutputRowCount = MIN_HASH_TABLE_SIZE;
            } else if (expectedOutputRowCount > MAX_HASH_TABLE_SIZE) {
                expectedOutputRowCount = MAX_HASH_TABLE_SIZE;
            } else if (expectedOutputRowCount < MIN_HASH_TABLE_SIZE) {
                expectedOutputRowCount = MIN_HASH_TABLE_SIZE;
            }
            for (int j = 0; j < parallelism; j++) {
                MemoryAllocatorCtx memoryAllocator = context.getMemoryPool().getMemoryAllocatorCtx();

                List<DataType> outputDataTypes = CalciteUtils.getTypes(hashAgg.getRowType());
                List<Aggregator> aggregators =
                    AggregateUtils.convertAggregators(inputDataTypes,
                        outputDataTypes.subList(groups.length, groups.length + hashAgg.getAggCallList().size()),
                        hashAgg.getAggCallList(), context, memoryAllocator);

                HashAggExec exec =
                    new HashAggExec(inputDataTypes, groups, aggregators, CalciteUtils.getTypes(hashAgg.getRowType()),
                        expectedOutputRowCount, spillerFactory, context);
                exec.setId(hashAgg.getRelatedId());
                if (context.getRuntimeStatistics() != null) {
                    RuntimeStatHelper.registerStatForExec(hashAgg, exec, context);
                }
                executors.add(exec);
            }
        }
        return executors;
    }

    public static int[] convertFrom(ImmutableBitSet gp) {
        List<Integer> list = gp.asList();
        int[] groups = new int[list.size()];
        for (int i = 0, n = list.size(); i < n; i++) {
            groups[i] = list.get(i);
        }
        return groups;
    }
}
