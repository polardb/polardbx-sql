package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.HashWindowExec;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

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

                // notice: filter args in window function is always -1
                List<Aggregator> aggregators =
                    AggregateUtils.convertAggregators(overWindow.groups.get(0).getAggregateCalls(overWindow), context,
                        memoryAllocator);

                HashWindowExec exec =
                    new HashWindowExec(inputDataTypes, groups, aggregators,
                        CalciteUtils.getTypes(overWindow.getRowType()),
                        estimateHashTableSize, spillerFactory, context);
                registerRuntimeStat(exec, overWindow, context);
                executors.add(exec);
            }
        }
        return executors;
    }
}
