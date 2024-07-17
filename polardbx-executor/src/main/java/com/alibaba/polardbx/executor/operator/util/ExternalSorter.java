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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class ExternalSorter extends Sorter {

    private static final Logger log = LoggerFactory.getLogger(ExternalSorter.class);

    private MemSortor memSortor;
    private SpillerFactory spillerFactory;

    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = immediateFuture(null);
    private Runnable finishMemoryRevoke = () -> {
    };

    private Iterator<Optional<Chunk>> sortedPages;
    private SpillableChunkIterator resultIterator;
    private Spiller memSpiller;

    private int spillCount = 0;
    private SpillMonitor spillMonitor;
    private ExecutionContext context;

    public ExternalSorter(OperatorMemoryAllocatorCtx memoryAllocator,
                          List<OrderByOption> orderBys,
                          List<DataType> columnMetas,
                          int chunkLimit,
                          SpillerFactory spillerFactory,
                          SpillMonitor spillMonitor,
                          ExecutionContext context) {
        super(memoryAllocator, orderBys, columnMetas, chunkLimit);
        this.context = context;
        this.spillerFactory = spillerFactory;
        this.memSortor = new MemSortor(memoryAllocator, orderBys, columnMetas, chunkLimit, true, context);
        this.spillMonitor = spillMonitor;
    }

    @Override
    public void addChunk(Chunk chunk) {
        this.memSortor.addChunk(chunk);
    }

    @Override
    public void sort() {
        this.memSortor.sort();
        resultIterator = new SpillableChunkIterator(this.memSortor.getSortedChunks());

        List<WorkProcessor<Chunk>> spilledPages = getSpilledPages();
        if (spilledPages.isEmpty()) {
            sortedPages = transform(resultIterator, Optional::of);
        } else {
            sortedPages = mergeSpilledAndMemoryPages(spilledPages, resultIterator).yieldingIterator();
        }
    }

    private List<WorkProcessor<Chunk>> getSpilledPages() {
        if (!spiller.isPresent()) {
            return ImmutableList.of();
        }

        return spiller.get().getSpills().stream()
            .map(WorkProcessor::fromIterator)
            .collect(toImmutableList());
    }

    private WorkProcessor<Chunk> mergeSpilledAndMemoryPages(List<WorkProcessor<Chunk>> spilledPages,
                                                            Iterator<Chunk> sortedPagesIndex) {
        List<WorkProcessor<Chunk>> sortedStreams = ImmutableList.<WorkProcessor<Chunk>>builder()
            .addAll(spilledPages)
            .add(WorkProcessor.fromIterator(sortedPagesIndex))
            .build();

        ChunkWithPositionComparator comparator = new ChunkWithPositionComparator(orderBys, columnMetas);

        BiPredicate<ChunkBuilder, ChunkWithPosition> chunkBreakPredicate =
            (chunkBuilder, ChunkWithPosition) -> chunkBuilder.isFull();
        return MergeSortedChunks.mergeSortedPages(
            sortedStreams, comparator, columnMetas, chunkLimit, chunkBreakPredicate, null, context);
    }

    @Override
    public Chunk nextChunk() {
        if (!sortedPages.hasNext()) {
            return null;
        }

        Optional<Chunk> chunk = sortedPages.next();
        if (!chunk.isPresent()) {
            return null;
        }
        return chunk.get();
    }

    @Override
    public void close() {
        this.memSortor.close();
        if (spiller.isPresent()) {
            spiller.get().close();
        }
        if (memSpiller != null) {
            memSpiller.close();
        }
    }

    public int getSpillCount() {
        return spillCount;
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke() {
        checkState(spillInProgress.isDone());
        if (memSortor.isEmpty()) {
            spillInProgress = immediateFuture(null);
            return spillInProgress;
        }
        if (resultIterator != null) {
            checkState(memSpiller == null, "MemSpiller already is set!");
            this.memSpiller = spillerFactory.create(columnMetas, spillMonitor, null);
            log.info(String.format("MemoryPool %s spilling memory data to disk %s, and it will release %s memory",
                memoryAllocator.getName(), spillCount, memoryAllocator.getRevocableAllocated()));
            spillInProgress = resultIterator.spill(memSpiller);
            finishMemoryRevoke = () -> {
                resultIterator.setIterator(memSpiller.getSpills().get(0));
                memSortor.reset();
            };
        } else {
            memSortor.sort();
            Iterator<Chunk> chunks = memSortor.getSortedChunks();
            if (!spiller.isPresent()) {
                spiller = Optional.of(spillerFactory.create(columnMetas, spillMonitor, null));
            }
            spillCount++;
            log.info(String.format("MemoryPool %s spilling memory data to disk %s, and it will release %s memory",
                memoryAllocator.getName(), spillCount, memoryAllocator.getRevocableAllocated()));
            spillInProgress = spiller.get().spill(chunks, false);
            finishMemoryRevoke = () -> {
                memSortor.reset();
            };
        }
        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke() {
        checkState(spillInProgress.isDone());
        finishMemoryRevoke.run();
        finishMemoryRevoke = () -> {
        };
    }
}
