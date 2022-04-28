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
import com.alibaba.polardbx.executor.operator.SortAggExec;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.rel.SortAgg;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.executor.mpp.operator.factory.HashAggExecutorFactory.convertFrom;

public class SortAggExecFactory extends ExecutorFactory {

    private SortAgg sortAgg;
    private int parallelism;
    private List<Executor> executors = new ArrayList<>();

    public SortAggExecFactory(SortAgg sortAgg, ExecutorFactory executorFactory, int parallelism) {
        this.sortAgg = sortAgg;
        this.parallelism = parallelism;
        addInput(executorFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        getAllExecutors(context);
        return executors.get(index);
    }

    @Override
    public synchronized List<Executor> getAllExecutors(ExecutionContext context) {

        if (executors.isEmpty()) {
            for (int k = 0; k < parallelism; k++) {
                MemoryAllocatorCtx memoryAllocator = context.getMemoryPool().getMemoryAllocatorCtx();
                Executor input = getInputs().get(0).createExecutor(context, k);
                List<DataType> outputDataTypes = CalciteUtils.getTypes(sortAgg.getRowType());
                List<Aggregator> aggregators =
                    AggregateUtils.convertAggregators(input.getDataTypes(), outputDataTypes
                            .subList(sortAgg.getGroupCount(), sortAgg.getGroupCount() + sortAgg.getAggCallList().size()),
                        sortAgg.getAggCallList(), context, memoryAllocator);

                ImmutableBitSet gp = sortAgg.getGroupSet();
                int[] groups = convertFrom(gp);
                Executor exec =
                    new SortAggExec(
                        input, groups, aggregators, CalciteUtils.getTypes(sortAgg.getRowType()), context);
                exec.setId(sortAgg.getRelatedId());
                if (context.getRuntimeStatistics() != null) {
                    RuntimeStatHelper.registerStatForExec(sortAgg, exec, context);
                }
                executors.add(exec);
            }
        }
        return executors;
    }
}
