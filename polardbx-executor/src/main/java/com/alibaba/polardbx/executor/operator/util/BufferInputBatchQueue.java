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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferInputBatchQueue extends AbstractBatchQueue {

    LinkedList<Chunk> chunks;
    MemoryAllocatorCtx allocator;
    ChunkHashSet chunkHashSet;

    AtomicInteger totalRowCount = new AtomicInteger(0);

    public BufferInputBatchQueue(int batchSize, List<DataType> columns, MemoryAllocatorCtx allocator,
                                 ExecutionContext context) {
        this(batchSize, columns, allocator, 1024, context);
    }

    public BufferInputBatchQueue(int batchSize, List<DataType> columns, MemoryAllocatorCtx allocator, int chunkSize,
                                 ExecutionContext context) {
        super(batchSize, columns, context);
        this.allocator = allocator;
        this.chunks = new LinkedList<>();
        DataType[] inputType = new DataType[columns.size()];
        for (int i = 0; i < inputType.length; i++) {
            inputType[i] = columns.get(i);
        }
        this.chunkHashSet = new ChunkHashSet(inputType, 1024, chunkSize, context);
    }

    public boolean isEmpty() {
        return chunks.isEmpty();
    }

    public void addDistinctChunk(Chunk chunk) {
        long beforeEstimateSize = chunkHashSet.estimateSize();
        chunkHashSet.addChunk(chunk);
        long afterEstimateSize = chunkHashSet.estimateSize();
        allocator.allocateReservedMemory(afterEstimateSize - beforeEstimateSize);
    }

    public void buildChunks() {
        if (chunkHashSet.estimateSize() > 0) {
            List<Chunk> groupChunks = chunkHashSet.buildChunks();
            chunks.addAll(groupChunks);
        }
    }

    public void addChunk(Chunk chunk) {
        long chunkEstimateSize = chunk.estimateSize();
        allocator.allocateReservedMemory(chunkEstimateSize);
        chunks.add(chunk);
        totalRowCount.getAndAdd(chunk.getPositionCount());
    }

    @Override
    protected Chunk nextChunk() {
        Chunk chunk = chunks.pollFirst();
        if (chunk != null) {
            long chunkEstimateSize = chunk.estimateSize();
            allocator.releaseReservedMemory(chunkEstimateSize, true);
        }
        return chunk;
    }

    public AtomicInteger getTotalRowCount() {
        return totalRowCount;
    }
}