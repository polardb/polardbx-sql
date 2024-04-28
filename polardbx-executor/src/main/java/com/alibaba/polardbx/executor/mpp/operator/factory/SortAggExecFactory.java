package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.SortAggExec;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.rel.SortAgg;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

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
                List<Aggregator> aggregators =
                    AggregateUtils.convertAggregators(sortAgg.getAggCallList(), context, memoryAllocator);

                Executor input = getInputs().get(0).createExecutor(context, k);
                ImmutableBitSet gp = sortAgg.getGroupSet();
                int[] groups = AggregateUtils.convertBitSet(gp);
                Executor exec =
                    new SortAggExec(
                        input, groups, aggregators, CalciteUtils.getTypes(sortAgg.getRowType()), context);
                registerRuntimeStat(exec, sortAgg, context);
                executors.add(exec);
            }
        }
        return executors;
    }

}
