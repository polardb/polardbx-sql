package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.utils.ExecUtils;

import static com.google.common.base.Preconditions.checkState;

public class LocalHashBucketFunction implements PartitionFunction {
    protected final int partitionCount;
    protected final boolean isPowerOfTwo;

    public LocalHashBucketFunction(int partitionCount) {
        this.partitionCount = partitionCount;
        this.isPowerOfTwo = MathUtils.isPowerOfTwo(partitionCount);
    }

    @Override
    public int getPartition(Chunk page, int position) {
        int partition = ExecUtils.partition(page.hashCode(position), partitionCount, isPowerOfTwo);
        checkState(partition >= 0 && partition < partitionCount);
        return partition;
    }
}
