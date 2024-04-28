package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;

public abstract class AbstractPlanFragmentTester {
    private final int parallelism;
    private final int taskNumber;
    private final int localPartitionCount;

    public AbstractPlanFragmentTester(int parallelism, int taskNumber, int localPartitionCount) {
        this.parallelism = parallelism;
        this.taskNumber = taskNumber;
        this.localPartitionCount = localPartitionCount;
    }

    abstract boolean test(List<PipelineFactory> pipelineFactories, ExecutionContext executionContext);

    public int getParallelism() {
        return parallelism;
    }

    public int getTaskNumber() {
        return taskNumber;
    }

    public int getLocalPartitionCount() {
        return localPartitionCount;
    }
}
