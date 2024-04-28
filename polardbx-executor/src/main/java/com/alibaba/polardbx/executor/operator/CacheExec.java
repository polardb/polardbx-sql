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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CacheExec extends AbstractExecutor implements ConsumerExecutor {

    private List<DataType> columnMetas;

    private List<Chunk> chunks = Lists.newArrayList();

    private AtomicBoolean isReady = new AtomicBoolean(false);
    private int currentChunkIndex = 0;

    private MemoryPool memoryPool;
    private MemoryAllocatorCtx memoryAllocator;

    private boolean finished = false;

    /**
     * reusing mode
     */
    public CacheExec(List<DataType> columnMetas, List<Chunk> chunks, ExecutionContext context) {
        super(context);
        this.columnMetas = columnMetas;
        this.chunks = chunks;
    }

    public CacheExec(List<DataType> columnMetas, ExecutionContext context) {
        super(context);
        this.columnMetas = columnMetas;
    }

    @Override
    public List<DataType> getDataTypes() {
        return columnMetas;
    }

    @Override
    void doOpen() {
    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    Chunk doNextChunk() {
        if (chunks.size() > currentChunkIndex) {
            return chunks.get(currentChunkIndex++);
        } else {
            finished = true;
            return null;
        }
    }

    /**
     * choose array list for read perf
     */
    public List<Chunk> getChunks() {
        return chunks;
    }

    public boolean isReady() {
        return isReady.get();
    }

    @Override
    public void openConsume() {
        memoryPool = MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = memoryPool.getMemoryAllocatorCtx();
    }

    @Override
    public void consumeChunk(Chunk chunk) {
        memoryAllocator.allocateReservedMemory(chunk.estimateSize());
        chunks.add(chunk);
    }

    @Override
    public void buildConsume() {
        isReady.set(true);
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public void closeConsume(boolean force) {
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
