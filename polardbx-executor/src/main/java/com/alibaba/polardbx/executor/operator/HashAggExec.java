package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.operator.util.AggOpenHashMap;
import com.alibaba.polardbx.executor.operator.util.AggregateUtils;
import com.alibaba.polardbx.executor.operator.util.SpillableAggHashMap;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.util.concurrent.ListenableFuture;

import java.text.MessageFormat;
import java.util.List;

public class HashAggExec extends AbstractHashAggExec implements ConsumerExecutor, MemoryRevoker {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashAggExec.class);

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
            if (aggCall.isDistinct() || aggCall instanceof AvgV2) {
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
                    context, memoryAllocator);
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
        if (groups.length == 0) { // no group by
            inputKeyChunk = inputChunk;
        } else {
            inputKeyChunk = inputKeyChunkGetter.apply(inputChunk);
        }
        long beforeEstimateSize = hashTable.estimateSize();
        hashTable.putChunk(inputKeyChunk, inputChunk, null);
        long afterEstimateSize = hashTable.estimateSize();
        this.needMemoryAllocated = Math.max(afterEstimateSize - beforeEstimateSize, 0);

        // release input chunk
        inputChunk.recycle();
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
        long start = System.nanoTime();
        if (hashTable != null) {
            resultIterator = hashTable.buildChunks();
        }
        long end = System.nanoTime();
        LOGGER.debug(MessageFormat.format("HashAggExec: {0} build consume time cost = {1} ns, "
            + "start = {2}, end = {3}", this.toString(), (end - start), start, end));
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
