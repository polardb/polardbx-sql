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

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountRow;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Count;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Long2LongSum0;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilders;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessorExec;
import com.alibaba.polardbx.executor.operator.ProducerExecutor;
import com.alibaba.polardbx.executor.operator.SortAggExec;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class SpillableAggHashMap implements AggHashMap {

    private static final Logger log = LoggerFactory.getLogger(SpillableAggHashMap.class);

    private DataType[] groupKeyType;
    private List<Aggregator> aggregators;
    private DataType[] aggValueType;
    private List<DataType> outputColumnMeta;
    private DataType[] inputType;
    private int expectedSize;
    private int chunkSize;
    private ExecutionContext context;
    private OperatorMemoryAllocatorCtx memoryAllocator;

    private AggOpenHashMap aggHashMap;
    private SpillableAggResultIterator aggFinalResultIterator;
    private SpillableChunkIterator sortAggResultIterator;
    private Optional<Spiller> spiller = Optional.empty();
    private Optional<SortAggExec> merger = Optional.empty();
    private Runnable finishMemoryRevoke = () -> {
    };

    private final SpillerFactory spillerFactory;
    private ListenableFuture<?> spillInProgress = immediateFuture(null);
    private final BlockBuilder[] blockBuilders;
    private int spillCount = 0;
    private List<DataType> spillTypes;
    private Spiller memSpiller;

    public SpillableAggHashMap(DataType[] groupKeyType, List<Aggregator> aggregators, DataType[] aggValueType,
                               List<DataType> outputColumnMeta, DataType[] inputType, int expectedSize,
                               int chunkSize, ExecutionContext context, OperatorMemoryAllocatorCtx memoryAllocator,
                               SpillerFactory spillerFactory) {
        this.groupKeyType = groupKeyType;
        this.aggregators = aggregators;
        this.aggValueType = aggValueType;
        this.outputColumnMeta = outputColumnMeta;
        this.inputType = inputType;
        this.expectedSize = expectedSize;
        this.chunkSize = chunkSize;
        this.context = context;
        this.memoryAllocator = memoryAllocator;

        aggHashMap = new AggOpenHashMap(groupKeyType, aggregators, aggValueType, inputType,
            expectedSize, chunkSize, context);
        this.spillerFactory = spillerFactory;
        this.spillTypes = new ArrayList<>();
        blockBuilders = new BlockBuilder[groupKeyType.length + aggValueType.length];
        for (int i = 0; i < groupKeyType.length; i++) {
            blockBuilders[i] = BlockBuilders.create(groupKeyType[i], context);
            spillTypes.add(groupKeyType[i]);
        }
        for (int j = 0; j < aggValueType.length; j++) {
            blockBuilders[groupKeyType.length + j] = BlockBuilders.create(aggValueType[j], context);
            spillTypes.add(aggValueType[j]);
        }
    }

    @Override
    public void putChunk(Chunk keyChunk, Chunk inputChunk) {
        checkState(spillInProgress.isDone());
        aggHashMap.putChunk(keyChunk, inputChunk);
    }

    @Override
    public List<Chunk> getGroupChunkList() {
        return null;
    }

    @Override
    public List<Chunk> getValueChunkList() {
        return null;
    }

    private List<WorkProcessor<Chunk>> getSpilledPages() {
        if (!spiller.isPresent()) {
            return ImmutableList.of();
        }

        return spiller.get().getSpills().stream().map(WorkProcessor::fromIterator).collect(toImmutableList());
    }

    @Override
    public AggResultIterator buildChunks() {
        if (!spiller.isPresent()) {
            aggFinalResultIterator = new SpillableAggResultIterator(aggHashMap.buildChunks());
            return aggFinalResultIterator;
        }
        sortAggResultIterator = new SpillableChunkIterator(aggHashMap.buildHashSortedResult().iterator());

        List<WorkProcessor<Chunk>> spilledPages = getSpilledPages();

        List<WorkProcessor<Chunk>> sortedStreams = ImmutableList.<WorkProcessor<Chunk>>builder()
            .addAll(spilledPages)
            .add(WorkProcessor.fromIterator(sortAggResultIterator))
            .build();

        List<OrderByOption> orderBys = new ArrayList<>();
        List<DataType> dataTypes = new ArrayList<>();
        for (int i = 0; i < groupKeyType.length; i++) {
            orderBys.add(
                new OrderByOption(i, RelFieldCollation.Direction.ASCENDING,
                    RelFieldCollation.NullDirection.FIRST));
            dataTypes.add(groupKeyType[i]);
        }

        ChunkWithPositionComparator comparator = new ChunkWithPositionComparator(orderBys, dataTypes);

        BiPredicate<ChunkBuilder, ChunkWithPosition> chunkBreakPredicate =
            (chunkBuilder, ChunkWithPosition) -> chunkBuilder.isFull();
        WorkProcessor<Chunk> sortedChunks = MergeSortedChunks.mergeSortedPages(
            sortedStreams, comparator, spillTypes, chunkSize, chunkBreakPredicate, null, context);

        int[] groups = new int[groupKeyType.length];
        for (int i = 0; i < groupKeyType.length; i++) {
            groups[i] = i;
        }

        SortAggExec sortAggExec = new SortAggExec(new WorkProcessorExec(sortedChunks), groups, getGlobalAggregators(),
            outputColumnMeta, context);
        sortAggExec.open();
        merger = Optional.of(sortAggExec);
        return () -> sortAggExec.nextChunk();
    }

    private List<Aggregator> getGlobalAggregators() {
        List<Aggregator> aggList = new ArrayList<>(aggregators.size());
        int groupKeySize = groupKeyType.length;
        for (Aggregator aggCall : aggregators) {
            if (aggCall instanceof Count || aggCall instanceof CountRow) {
                aggList.add(
                    new Long2LongSum0(groupKeySize + aggList.size(), ((AbstractAggregator) aggCall).isDistinct(),
                        DataTypes.LongType, DataTypes.DecimalType,
                        ((AbstractAggregator) aggCall).getFilterArg()));
            } else {
                ((AbstractAggregator) aggCall).setAggIndexInChunk(new int[] {groupKeySize + aggList.size()});
                aggList.add(aggCall);
            }
        }
        return aggList;
    }

    private ListenableFuture<?> spillToDisk() {
        if (aggHashMap.getGroupCount() < 1) {
            spillInProgress = ProducerExecutor.NOT_BLOCKED;
            return spillInProgress;
        }
        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(spillTypes, context.getQuerySpillSpaceMonitor(), null));
        }
        spillCount++;

        spillInProgress = spiller.get().spill(aggHashMap.buildHashSortedResult().iterator(), false);
        finishMemoryRevoke = () -> {
            memoryAllocator.releaseRevocableMemory(memoryAllocator.getRevocableAllocated(), true);
            if (aggHashMap != null) {
                aggHashMap.close();
            }
            aggHashMap = new AggOpenHashMap(groupKeyType, aggregators, aggValueType, inputType,
                expectedSize, chunkSize, context);
        };
        return spillInProgress;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        checkState(spillInProgress.isDone());

        log.info(String.format("MemoryPool %s spilling memory data to disk %s, and it will release %s memory",
            memoryAllocator.getName(), spillCount, memoryAllocator.getRevocableAllocated()));
        if (aggFinalResultIterator != null) {
            // spill memory aggr result when no disk
            return spillMemoryAggFinalResultToDisk();
        }
        if (sortAggResultIterator != null) {
            // spill memory sort aggr result table when it has disk
            return spillMemorySortIntermediateResultToDisk();
        }
        return spillToDisk();
    }

    private ListenableFuture<?> spillMemoryAggFinalResultToDisk() {
        if (aggHashMap.getGroupCount() < 1) {
            spillInProgress = ProducerExecutor.NOT_BLOCKED;
            return spillInProgress;
        }

        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(spillTypes, context.getQuerySpillSpaceMonitor(), null));
        }

        spillCount++;

        spillInProgress = aggFinalResultIterator.spill(spiller.get());

        finishMemoryRevoke = () -> {
            memoryAllocator.releaseRevocableMemory(memoryAllocator.getRevocableAllocated(), true);
            aggFinalResultIterator.setSpiller(spiller.get());
            if (aggHashMap != null) {
                aggHashMap.close();
            }
            aggHashMap = null;
        };
        return spillInProgress;
    }

    private ListenableFuture<?> spillMemorySortIntermediateResultToDisk() {
        checkState(memSpiller == null, "MemSpiller already is set!");
        spillCount++;
        this.memSpiller = spillerFactory.create(spillTypes, context.getQuerySpillSpaceMonitor(), null);
        spillInProgress = sortAggResultIterator.spill(memSpiller);
        finishMemoryRevoke = () -> {
            sortAggResultIterator.setIterator(memSpiller.getSpills().get(0));
            memoryAllocator.releaseRevocableMemory(memoryAllocator.getRevocableAllocated(), true);
            aggHashMap = null;
        };
        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke() {
        checkState(spillInProgress.isDone());
        finishMemoryRevoke.run();
        finishMemoryRevoke = () -> {
            memoryAllocator.releaseRevocableMemory(memoryAllocator.getRevocableAllocated(), true);
        };
    }

    @Override
    public void close() {
        try {
            if (aggHashMap != null) {
                aggHashMap.close();
            }
            if (merger.isPresent()) {
                merger.get().close();
            }
            if (memSpiller != null) {
                memSpiller.close();
            }
            if (spiller.isPresent()) {
                spiller.get().close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long estimateSize() {
        if (aggHashMap != null) {
            return aggHashMap.estimateSize();
        } else {
            return 0;
        }
    }

    class SpillableAggResultIterator implements AggResultIterator {

        private AggResultIterator aggResultIterator;

        public SpillableAggResultIterator(AggResultIterator aggResultIterator) {
            this.aggResultIterator = aggResultIterator;
        }

        public void setSpiller(Spiller spiller) {
            Iterator<Chunk> iterator = spiller.getSpills().get(0);
            this.aggResultIterator = () -> {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            };
        }

        public ListenableFuture<?> spill(Spiller spiller) {
            return spiller.spill(new AbstractIterator<Chunk>() {

                private Chunk chunk = null;

                @Override
                public Chunk computeNext() {
                    chunk = aggResultIterator.nextChunk();
                    if (chunk == null) {
                        return endOfData();
                    }
                    return chunk;
                }
            }, false);
        }

        @Override
        public Chunk nextChunk() {
            return this.aggResultIterator.nextChunk();
        }
    }
}
