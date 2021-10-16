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

import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.ChunkWithPositionComparator;
import com.alibaba.polardbx.executor.operator.util.SpilledTopNHeap;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;

import java.util.List;

/**
 * Top-N sort chunk executor
 */
public class SpilledTopNExec extends AbstractExecutor implements ConsumerExecutor, MemoryRevoker {

    private static final int COMPACT_THRESHOLD = 2;

    private final List<DataType> dataTypeList;
    private final List<OrderByOption> orderBys;
    private final long topSize;

    private MemoryPool memoryPool;
    private OperatorMemoryAllocatorCtx memoryAllocator;

    private boolean passNothing = false;

    private SpilledTopNHeap topNHeap;

    private SpillerFactory spillerFactory;

    private boolean finished;

    public SpilledTopNExec(List<DataType> dataTypeList, List<OrderByOption> orderBys, long topSize,
                           ExecutionContext context) {
        this(dataTypeList, orderBys, topSize, context, null);
    }

    public SpilledTopNExec(List<DataType> dataTypeList, List<OrderByOption> orderBys, long topSize,
                           ExecutionContext context, SpillerFactory spillerFactory) {
        super(context);
        this.dataTypeList = dataTypeList;
        this.orderBys = orderBys;
        if (topSize < 0) {
            throw new IllegalArgumentException("topN not support top size:" + topSize);
        } else if (topSize == 0) {
            passNothing = true;
        }
        this.topSize = topSize;
        this.spillerFactory = spillerFactory;
    }

    @Override
    void doOpen() {

    }

    @Override
    Chunk doNextChunk() {
        if (passNothing) {
            return null;
        }
        Chunk ret = topNHeap.nextChunk();
        if (ret == null) {
            finished = true;
        }
        return ret;
    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypeList;
    }

    @Override
    public void openConsume() {
        if (!passNothing) {
            boolean spillEnabled = spillerFactory != null;
            memoryPool =
                MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
            memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
            ChunkWithPositionComparator comparator = new ChunkWithPositionComparator(orderBys, dataTypeList);
            topNHeap =
                new SpilledTopNHeap(
                    dataTypeList, comparator, spillerFactory, topSize, COMPACT_THRESHOLD, memoryAllocator,
                    chunkLimit, context.getQuerySpillSpaceMonitor(), context);
        }
    }

    @Override
    public void closeConsume(boolean force) {
        if (!passNothing) {
            if (memoryPool != null) {
                collectMemoryUsage(memoryPool);
                memoryPool.destroy();
            }
            if (topNHeap != null) {
                topNHeap.close();
            }
        }
    }

    @Override
    public void consumeChunk(Chunk c) {
        if (!passNothing) {
            topNHeap.processChunk(c);
        }
    }

    @Override
    public void buildConsume() {
        if (topNHeap != null) {
            topNHeap.buildResult();
        }
    }

    @Override
    public boolean needsInput() {
        return !passNothing;
    }

    @Override
    public boolean consumeIsFinished() {
        return passNothing;
    }

    @Override
    public boolean produceIsFinished() {
        return passNothing || finished;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        addSpillCnt(1);
        return topNHeap.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke() {
        topNHeap.finishMemoryRevoke();
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocator;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
