package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.memory.DefaultMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class BufferInputBatchQueueTest {

    private BufferInputBatchQueue bufferInputBatchQueue;

    @Test
    public void testPop() {
        MemoryPool root = new MemoryPool("root", Integer.MAX_VALUE, MemoryType.OTHER);
        bufferInputBatchQueue = new BufferInputBatchQueue(100, ImmutableList.of(DataTypes.IntegerType),
            new DefaultMemoryAllocatorCtx(root), new ExecutionContext());
        Assert.assertTrue(bufferInputBatchQueue.pop() == null);
    }

    @Test(expected = RuntimeException.class)
    public void testLookupPop() {
        MemoryPool root = new MemoryPool("root", Integer.MAX_VALUE, MemoryType.OTHER);
        bufferInputBatchQueue = new BufferInputBatchQueue(
            100, ImmutableList.of(DataTypes.IntegerType), new DefaultMemoryAllocatorCtx(root), 100,
            new ExecutionContext(), true);
        bufferInputBatchQueue.addChunk(new Chunk());
        bufferInputBatchQueue.addChunk(new Chunk());
        Assert.assertTrue(bufferInputBatchQueue.pop() != null);
        bufferInputBatchQueue.pop();
    }

}
