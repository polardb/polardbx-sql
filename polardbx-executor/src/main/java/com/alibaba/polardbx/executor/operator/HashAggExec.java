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

import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggOpenHashMap;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.operator.util.SpillableAggHashMap;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.Aggregator;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class HashAggExec extends AbstractHashAggExec implements ConsumerExecutor, MemoryRevoker {
    protected final ChunkConverter inputKeyChunkGetter;

    private final DataType[] groupKeyType;
    private final DataType[] aggValueType;
    private final DataType[] inputType;

    private final int expectedGroups;

    private SpillerFactory spillerFactory;

    private long needMemoryAllocated = 0;

    public HashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        int expectedGroups,
        ExecutionContext context) {
        this(inputDataTypes, groups, aggregators, outputColumns,
            AggregateUtils.collectDataTypes(outputColumns, groups.length, outputColumns.size()), expectedGroups, null,
            context);
    }

    public HashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        int expectedGroups,
        SpillerFactory spillerFactory,
        ExecutionContext context) {
        this(inputDataTypes, groups, aggregators, outputColumns,
            AggregateUtils.collectDataTypes(outputColumns, groups.length, outputColumns.size()), expectedGroups,
            spillerFactory,
            context);
    }

    public HashAggExec(
        List<DataType> inputDataTypes,
        int[] groups,
        List<Aggregator> aggregators,
        List<DataType> outputColumns,
        DataType[] aggValueType,
        int expectedGroups,
        SpillerFactory spillerFactory,
        ExecutionContext context) {
        super(groups, aggregators, outputColumns, context);
        this.expectedGroups = expectedGroups;
        this.spillerFactory = spillerFactory;
        this.groupKeyType = AggregateUtils.collectDataTypes(inputDataTypes, groups);
        this.aggValueType = aggValueType;
        this.inputType = AggregateUtils.collectDataTypes(inputDataTypes);
        this.inputKeyChunkGetter = Converters.createChunkConverter(inputDataTypes, groups, groupKeyType, context);

    }

    @Override
    public void openConsume() {
        boolean spillEnabled = spillerFactory != null;
        for (Aggregator aggCall : aggregators) {
            if (((AbstractAggregator) aggCall).isDistinct() || !AggregateUtils.supportSpill(aggCall)) {
                spillEnabled = false;
                break;
            }
        }
        memoryPool =
            MemoryPoolUtils.createOperatorTmpTablePool(getExecutorName(), context.getMemoryPool());
        memoryAllocator = new OperatorMemoryAllocatorCtx(memoryPool, spillEnabled);
        if (!memoryAllocator.isRevocable()) {
            hashTable =
                new AggOpenHashMap(groupKeyType, aggregators, aggValueType, inputType, expectedGroups, chunkLimit,
                    context);
        } else {
            hashTable = new SpillableAggHashMap(groupKeyType, aggregators, aggValueType, outputColumnMeta, inputType,
                expectedGroups, chunkLimit, context, memoryAllocator, spillerFactory);
        }
        //FIXME The allocate memory for the initial hashMap can't be release in fact!
        //memoryAllocator.allocateReservedMemory(hashTable.estimateSize());
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
        Chunk inputKeyChunk;
        // no group by
        if (groups.length == 0) {
            inputKeyChunk = inputChunk;
        } else {
            inputKeyChunk = inputKeyChunkGetter.apply(inputChunk);
        }
        long beforeEstimateSize = hashTable.estimateSize();
        hashTable.putChunk(inputKeyChunk, inputChunk);
        long afterEstimateSize = hashTable.estimateSize();
        this.needMemoryAllocated = Math.max(afterEstimateSize - beforeEstimateSize, 0);
    }

    @Override
    void doOpen() {

    }

    @Override
    void doClose() {
        closeConsume(true);
    }

    @Override
    public void buildConsume() {
        if (hashTable != null) {
            resultIterator = hashTable.buildChunks();
        }
    }

    @Override
    public boolean produceIsFinished() {
        return finished;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        addSpillCnt(1);
        return hashTable.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke() {
        hashTable.finishMemoryRevoke();
    }

    @Override
    public OperatorMemoryAllocatorCtx getMemoryAllocatorCtx() {
        return memoryAllocator;
    }

    @Override
    public boolean needsInput() {
        boolean ret;
        if (needMemoryAllocated > 0) {
            if (memoryAllocator.isRevocable()) {
                ret = memoryAllocator.tryAllocateRevocableMemory(needMemoryAllocated);
            } else {
                memoryAllocator.allocateReservedMemory(needMemoryAllocated);
                ret = true;
            }
            if (ret) {
                needMemoryAllocated = 0;
            }
        } else {
            return true;
        }
        return ret;
    }

    @Override
    public ListenableFuture<?> consumeIsBlocked() {
        if (memoryAllocator.isRevocable()) {
            return memoryAllocator.isWaitingForTryMemory();
        } else {
            return ConsumerExecutor.NOT_BLOCKED;
        }
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
