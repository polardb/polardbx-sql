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

import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggHashMap;
import com.alibaba.polardbx.executor.operator.util.AggResultIterator;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.operator.util.HashWindowOpenHashMap;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class HashWindowExec extends AbstractExecutor implements ConsumerExecutor {

    protected final ChunkConverter inputKeyChunkGetter;

    protected final List<Aggregator> aggregators;

    protected final List<DataType> outputColumnMeta;

    protected final int[] groups;

    protected AggHashMap hashTable;

    AggResultIterator resultIterator;

    MemoryPool memoryPool;

    OperatorMemoryAllocatorCtx memoryAllocator;

    protected boolean finished = false;
    private final DataType[] groupKeyType;
    private final DataType[] aggValueType;
    private final DataType[] inputType;

    private final int expectedGroups;

    private SpillerFactory spillerFactory;

    private long needMemoryAllocated = 0;

    public HashWindowExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        int expectedGroups,
        SpillerFactory spillerFactory,
        ExecutionContext context) {
        super(context);
        this.groups = groups;
        this.aggregators = aggregators;
        this.outputColumnMeta = outputColumns;
        this.expectedGroups = expectedGroups;
        this.spillerFactory = spillerFactory;
        this.groupKeyType = AggregateUtils.collectDataTypes(inputDataTypes, groups);
        this.aggValueType = AggregateUtils.collectDataTypes(outputColumns, inputDataTypes.size(), outputColumns.size());
        this.inputType = AggregateUtils.collectDataTypes(inputDataTypes);
        this.inputKeyChunkGetter = Converters.createChunkConverter(inputDataTypes, groups, groupKeyType, context);
    }

    @Override
    public void openConsume() {
        // TODO: support spill hash window
        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, false);
        hashTable =
            new HashWindowOpenHashMap(groupKeyType, aggregators, aggValueType, inputType, expectedGroups, chunkLimit,
                context, memoryAllocator);
    }

    @Override
    public void closeConsume(boolean force) {
        if (hashTable != null) {
            hashTable.close();
        }
        hashTable = null;
        needMemoryAllocated = 0;
        resultIterator = null;
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        // TODO should be optimized
        Chunk inputKeyChunk = groups.length == 0 ? inputChunk : inputKeyChunkGetter.apply(inputChunk);
        long beforeEstimateSize = hashTable.estimateSize();
        hashTable.putChunk(inputKeyChunk, inputChunk, null);
        long afterEstimateSize = hashTable.estimateSize();
        // inputChunk will be cached in HashWindowAggMap
        long cachedChunkMemory = inputChunk.estimateSize();
        this.needMemoryAllocated = Math.max(afterEstimateSize - beforeEstimateSize + cachedChunkMemory, 0);
    }

    @Override
    public void buildConsume() {
        if (hashTable != null) {
            resultIterator = hashTable.buildChunks();
        }
    }

    @Override
    void doOpen() {

    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public boolean needsInput() {
        if (needMemoryAllocated > 0) {
            memoryAllocator.allocateReservedMemory(needMemoryAllocated);
            needMemoryAllocated = 0;
        }
        return true;
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        return ConsumerExecutor.NOT_BLOCKED;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }

    @Override
    Chunk doNextChunk() {
        Chunk ret = resultIterator.nextChunk();
        if (ret == null) {
            finished = true;
        }
        return ret;
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

   
}
