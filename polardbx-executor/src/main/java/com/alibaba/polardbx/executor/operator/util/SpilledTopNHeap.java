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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SpilledTopNHeap {

    private static final Logger log = LoggerFactory.getLogger(SpilledTopNHeap.class);

    private static final int MIN_POSITIONS_TO_COMPACT = 8 * 1024;
    private final int chunkLimit;

    private List<DataType> sourceTypes;

    // for heap element comparison
    private final ChunkWithPositionComparator pageWithPositionComparator;
    private final Comparator<IndexRow> comparator;

    private SpillerFactory spillerFactory;
    private long topN;
    private int compactThreshold;

    private RowHeap rowHeap;

    // used for spill
    private Optional<Spiller> spiller = Optional.empty();
    private Spiller memSpiller;
    private ListenableFuture<?> spillInProgress = immediateFuture(null);
    private Optional<Row> spilledNThRow = Optional.empty();

    private long spilledRows;
    private long totalSpilledRows;

    private long savedPositions;
    private long usedPositions;

    // a list of input pages, each of which has information of which row in which heap references which position
    private final ObjectBigArray<PageReference> pageReferences = new ObjectBigArray<>();

    // when there is no row referenced in a page, it will be removed instead of compacted;
    // use a list to record those empty slots to reuse them
    private final IntArrayFIFOQueue emptyPageReferenceSlots;
    private int maxPageId;

    private SpillableChunkIterator spillIterator;
    private Iterator<Optional<Chunk>> outputIterator;

    // keeps track sizes of input pages and heaps
    private OperatorMemoryAllocatorCtx memoryAllocator;
    // record the really memory size
    private long memorySizeInBytes;

    private long lastMemorySizeInBytes;

    private SpillMonitor spillMonitor;

    private int spillCount;

    private ExecutionContext context;

    public SpilledTopNHeap(List<DataType> sourceTypes, ChunkWithPositionComparator pageWithPositionComparator,
                           SpillerFactory spillerFactory, long topN, int compactThreshold,
                           OperatorMemoryAllocatorCtx memoryAllocator, int chunkLimit, SpillMonitor spillMonitor,
                           ExecutionContext context) {
        requireNonNull(pageWithPositionComparator, "comparator is null");
        this.context = context;
        this.chunkLimit = chunkLimit;
        this.sourceTypes = sourceTypes;
        this.pageWithPositionComparator = pageWithPositionComparator;
        this.spillerFactory = spillerFactory;
        this.topN = topN;
        this.compactThreshold = compactThreshold;

        // init PageBuilder
        this.emptyPageReferenceSlots = new IntArrayFIFOQueue();
        this.comparator = (left, right) -> pageWithPositionComparator.compareTo(
            pageReferences.get(left.getPageId()).getPage(),
            left.getPosition(),
            pageReferences.get(right.getPageId()).getPage(),
            right.getPosition());
        this.rowHeap = new RowHeap(Ordering.from(comparator).reversed());
        this.memoryAllocator = memoryAllocator;
        this.memorySizeInBytes = rowHeap.getEstimatedSizeInBytes();
        this.spillMonitor = spillMonitor;
        adjustMemoryPool();
    }

    public void processChunk(Chunk newPage) {

        checkArgument(newPage != null);
        PageReference newPageReference = new PageReference(newPage, sourceTypes, context);
        memorySizeInBytes += newPageReference.getEstimatedSizeInBytes();
        int newPageId;
        if (emptyPageReferenceSlots.isEmpty()) {
            // all the previous slots are full; create a new one
            pageReferences.ensureCapacity(maxPageId + 1);
            newPageId = maxPageId;
            maxPageId++;
        } else {
            // reuse a previously removed page's slot
            newPageId = emptyPageReferenceSlots.dequeueInt();
        }
        verify(pageReferences.get(newPageId) == null, "should not overwrite a non-empty slot");
        pageReferences.set(newPageId, newPageReference);

        memorySizeInBytes -= rowHeap.getEstimatedSizeInBytes();
        for (int position = 0; position < newPage.getPositionCount(); position++) {
            if (rowHeap.size() < topN) {
                boolean enqueue = true;
                if (spilledNThRow.isPresent()) {
                    if (spilledRows < topN && spilledRows + rowHeap.size() >= topN) {
                        // not merge if spilledRows + rows.size() < topN
                        IndexRow nThRow = rowHeap.first();
                        if (pageWithPositionComparator.compareTo(
                            spilledNThRow.get(), pageReferences.get(nThRow.getPageId()).getPage(), nThRow.getPosition())
                            < 0) {
                            // spilledNThRow has higher priority
                            spilledNThRow = Optional.of(getTheRow(nThRow));
                        }
                        spilledRows = topN;
                    }

                    if (spilledRows >= topN) {
                        // need compare with the spilledNThRow
                        if (pageWithPositionComparator.compareTo(spilledNThRow.get(), newPage, position) < 0) {
                            // spilledNThRow has higher priority
                            enqueue = false;
                        }
                    }
                }

                if (enqueue) {
                    // still have space for the current group
                    IndexRow row = new IndexRow(newPageId, position);
                    rowHeap.enqueue(row);
                    newPageReference.reference(row);

                    savedPositions++;
                    usedPositions++;
                }
            } else {
                // may compare with the topN-th element with in the heap to decide if update is necessary
                IndexRow nThRow = rowHeap.first();
                // still have space for the current group
                IndexRow newRow = new IndexRow(newPageId, position);
                if (comparator.compare(newRow, nThRow) < 0) {
                    // update reference and the heap
                    rowHeap.dequeue();
                    PageReference previousPageReference = pageReferences.get(nThRow.getPageId());
                    previousPageReference.dereference(nThRow.getPosition());
                    newPageReference.reference(newRow);
                    rowHeap.enqueue(newRow);

                    savedPositions++;
                }
            }
        }
        memorySizeInBytes += rowHeap.getEstimatedSizeInBytes();
        if (newPageReference.getUsedPositionCount() == 0) {
            pageReferences.set(newPageId, null);
            emptyPageReferenceSlots.enqueue(newPageId);
            memorySizeInBytes -= newPageReference.getEstimatedSizeInBytes();
        }

        if (savedPositions > MIN_POSITIONS_TO_COMPACT && compactThreshold * usedPositions <= savedPositions) {
            for (int i = 0; i < maxPageId; i++) {
                PageReference pageReference = pageReferences.get(i);
                if (pageReferences.get(i) != null
                    && pageReference.getUsedPositionCount() * compactThreshold < pageReference.getPage()
                    .getPositionCount()) {
                    if (pageReference.getUsedPositionCount() == 0) {
                        pageReferences.set(i, null);
                        emptyPageReferenceSlots.enqueue(i);
                        memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                    } else {
                        memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                        pageReference.compact();
                        memorySizeInBytes += pageReference.getEstimatedSizeInBytes();
                    }
                }
            }
        }
        adjustMemoryPool();
    }

    private void adjustMemoryPool() {
        memorySizeInBytes = Math.max(memorySizeInBytes, 0);
        if (memoryAllocator.isRevocable()) {
            long needAllocateSize = memorySizeInBytes - lastMemorySizeInBytes;
            if (needAllocateSize > 0) {
                memoryAllocator.allocateRevocableMemory(needAllocateSize);
            } else if (needAllocateSize < 0) {
                memoryAllocator.releaseRevocableMemory(-needAllocateSize, false);
            }
        } else {
            long needAllocateSize = memorySizeInBytes - lastMemorySizeInBytes;
            if (needAllocateSize > 0) {
                memoryAllocator.allocateReservedMemory(needAllocateSize);
            } else if (needAllocateSize < 0) {
                memoryAllocator.releaseReservedMemory(-needAllocateSize, false);
            }
        }
        lastMemorySizeInBytes = memorySizeInBytes;
    }

    public Chunk nextChunk() {
        if (!outputIterator.hasNext()) {
            return null;
        }

        Optional<Chunk> chunk = outputIterator.next();
        if (!chunk.isPresent()) {
            return null;
        }
        return chunk.get();
    }

    public void buildResult() {
        this.spillIterator = new SpillableChunkIterator(new ResultIterator());
        List<WorkProcessor<Chunk>> spilledPages = getSpilledPages();
        if (spilledPages.isEmpty()) {
            this.outputIterator = transform(spillIterator, Optional::of);
        } else {
            this.outputIterator = mergeSpilledAndMemoryPages(spilledPages, spillIterator).yieldingIterator();
        }
    }

    private List<WorkProcessor<Chunk>> getSpilledPages() {
        if (!spiller.isPresent()) {
            return ImmutableList.of();
        }
        return spiller.get().getSpills().stream().map(WorkProcessor::fromIterator).collect(toImmutableList());
    }

    private WorkProcessor<Chunk> mergeSpilledAndMemoryPages(List<WorkProcessor<Chunk>> spilledPages,
                                                            Iterator<Chunk> sortedPagesIndex) {
        log.debug(String
            .format("mergeFromDisk with %s channels, totalSpilledRows:%s", spilledPages.size(), totalSpilledRows));
        List<WorkProcessor<Chunk>> sortedStreams = ImmutableList.<WorkProcessor<Chunk>>builder()
            .addAll(spilledPages)
            .add(WorkProcessor.fromIterator(sortedPagesIndex))
            .build();

        BiPredicate<ChunkBuilder, ChunkWithPosition> chunkBreakPredicate =
            (chunkBuilder, ChunkWithPosition) -> chunkBuilder.isFull();
        return MergeSortedChunks.mergeSortedPages(
            sortedStreams, pageWithPositionComparator, sourceTypes, chunkLimit, chunkBreakPredicate, null, context);
    }

    private Row getTheRow(IndexRow rowAddress) {

        return pageReferences.get(rowAddress.getPageId()).getPage().rowAt(rowAddress.getPosition());
    }

    public ListenableFuture<?> startMemoryRevoke() {
        checkState(spillInProgress.isDone());

        // record the Nth row of spilled rows
        if (rowHeap == null || rowHeap.isEmpty()) {
            // empty group
            spillInProgress = immediateFuture(null);
            return spillInProgress;
        }

        log.info(String.format(
            "startMemoryRevoke with topN:%s maxGroupId:%s usedPositions:%s savedPositions:%s lastSpilledRows:%s totalSpilledRows:%s, and it will release %s memory",
            topN, maxPageId, usedPositions, savedPositions, spilledRows, totalSpilledRows,
            memoryAllocator.getRevocableAllocated()));

        if (spillIterator != null) {
            checkState(memSpiller == null, "MemSpiller already is set!");
            this.memSpiller = spillerFactory.create(sourceTypes, spillMonitor, null);
            spillInProgress = spillIterator.spill(memSpiller);
            spillIterator.setIterator(memSpiller.getSpills().get(0));
            return spillInProgress;
        } else {
            IndexRow nThRowAddress = rowHeap.first();
            long heapSize = rowHeap.size();

            int pageIndex = nThRowAddress.getPageId();
            int position = nThRowAddress.getPosition();
            if (!spilledNThRow.isPresent()) {
                spilledNThRow = Optional.of(getTheRow(nThRowAddress));
                spilledRows = heapSize;
            } else if (spilledRows >= topN && heapSize >= topN) {
                if (pageWithPositionComparator.compareTo(
                    spilledNThRow.get(), pageReferences.get(pageIndex).page, position) > 0) {
                    // in memory nTh row has the higher priority
                    spilledNThRow = Optional.of(getTheRow(nThRowAddress));
                    spilledRows = topN;
                }
            } else if (spilledRows >= topN && heapSize < topN) {
                // ignore
            } else if (spilledRows < topN && heapSize >= topN) {
                spilledNThRow = Optional.of(getTheRow(nThRowAddress));
                spilledRows = topN;
            } else {
                // both < topN
                if (pageWithPositionComparator.compareTo(
                    spilledNThRow.get(), pageReferences.get(pageIndex).page, position) <= 0) {
                    // in memory nTh row has the lower priority
                    spilledNThRow = Optional.of(getTheRow(nThRowAddress));
                }
                spilledRows = (spilledRows + heapSize >= topN ? topN : spilledRows + heapSize);
            }
            spillCount++;
            return spillToDisk();
        }
    }

    public ListenableFuture<?> spillToDisk() {

        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.create(sourceTypes, spillMonitor, null));
        }

        spillInProgress = spiller.get().spill(new ResultIterator(), false);
        // record spilled rows
        totalSpilledRows += usedPositions;
        return spillInProgress;
    }

    public void finishMemoryRevoke() {
        checkState(spillInProgress.isDone());
        resetBuilder();
        memoryAllocator.releaseRevocableMemory(memoryAllocator.getRevocableAllocated(), true);
    }

    private void resetBuilder() {
        savedPositions = 0L;
        usedPositions = 0L;
    }

    public void close() {
        if (spiller.isPresent()) {
            spiller.get().close();
        }
        if (memSpiller != null) {
            memSpiller.close();
        }
    }

    private static class PageReference {
        private static final long INSTANCE_SIZE = ClassLayout.parseClass(PageReference.class).instanceSize();

        private Chunk page;
        private IndexRow[] reference;
        protected List<DataType> sourceTypes;

        private int usedPositionCount;
        private ExecutionContext context;

        public PageReference(Chunk page, List<DataType> sourceTypes, ExecutionContext context) {
            this.page = requireNonNull(page, "page is null");
            this.reference = new IndexRow[page.getPositionCount()];
            this.sourceTypes = sourceTypes;
            this.context = context;
        }

        public void reference(IndexRow row) {
            int position = row.getPosition();
            reference[position] = row;
            usedPositionCount++;
        }

        public void dereference(int position) {
            checkArgument(reference[position] != null && usedPositionCount > 0);
            reference[position] = null;
            usedPositionCount--;
        }

        public int getUsedPositionCount() {
            return usedPositionCount;
        }

        public void compact() {
            checkState(usedPositionCount > 0);

            if (usedPositionCount == page.getPositionCount()) {
                return;
            }
            // re-assign reference
            IndexRow[] newReference = new IndexRow[usedPositionCount];
            int[] positions = new int[usedPositionCount];
            int index = 0;
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (reference[i] != null) {
                    newReference[index] = reference[i];
                    positions[index] = i;
                    index++;
                }
            }
            verify(index == usedPositionCount);

            // compact page
            ChunkBuilder builder = new ChunkBuilder(sourceTypes, positions.length, context);
            for (int pos : positions) {
                builder.declarePosition();
                for (int i = 0; i < page.getBlockCount(); i++) {
                    builder.appendTo(page.getBlock(i), i, pos);
                }
            }
            // update all the elements in the heaps that reference the current page
            for (int i = 0; i < usedPositionCount; i++) {
                // this does not change the elements in the heap;
                // it only updates the value of the elements; while keeping the same order
                newReference[i].reset(i);
            }
            page = builder.build();
            reference = newReference;
        }

        public Chunk getPage() {
            return page;
        }

        public long getEstimatedSizeInBytes() {
            return page.estimateSize() + sizeOf(reference) + INSTANCE_SIZE;
        }
    }

    // this class is for precise memory tracking
    protected static class RowHeap
        extends ObjectHeapPriorityQueue<IndexRow> {
        protected static final long INSTANCE_SIZE = ClassLayout.parseClass(RowHeap.class).instanceSize();
        protected static final long ROW_ENTRY_SIZE = ClassLayout.parseClass(IndexRow.class).instanceSize();

        private RowHeap(Comparator<IndexRow> comparator) {
            super(1, comparator);
        }

        private long getEstimatedSizeInBytes() {
            return INSTANCE_SIZE + sizeOf(heap) + size() * ROW_ENTRY_SIZE;
        }
    }

    /**
     * The class is a pointer to a row in a page.
     * The actual position in the page is mutable because as pages are compacted, the position will change.
     */
    private class IndexRow {
        private final int pageId;
        private int position;

        private IndexRow(int pageId, int position) {
            this.pageId = pageId;
            reset(position);
        }

        public void reset(int position) {
            this.position = position;
        }

        public int getPageId() {
            return pageId;
        }

        public int getPosition() {
            return position;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                .add("pageId", pageId)
                .add("position", position)
                .toString();
        }
    }

    private class ResultIterator extends AbstractIterator<Chunk> {
        private final ChunkBuilder pageBuilder;

        // the row number of the current position
        private int currentPosition;
        // number of rows
        private int currentSize;
        private ObjectBigArray<IndexRow> currentRows;
        private long heapSizeInBytes;

        ResultIterator() {
            this.pageBuilder = new ChunkBuilder(sourceTypes, chunkLimit, context);
            this.currentRows = nextRows();
        }

        @Override
        protected Chunk computeNext() {
            pageBuilder.reset();
            while (!pageBuilder.isFull() && currentRows != null) {
                if (currentPosition == currentSize) {
                    // the current group has produced all its rows
                    memorySizeInBytes -= heapSizeInBytes;
                    adjustMemoryPool();
                    break;
                }

                IndexRow indexRow = currentRows.get(currentPosition);

                pageBuilder.declarePosition();
                for (int i = 0; i < sourceTypes.size(); i++) {
                    pageBuilder.appendTo(pageReferences.get(
                        indexRow.getPageId()).getPage().getBlock(i), i, indexRow.getPosition());
                }

                currentPosition++;

                // deference the row; no need to compact the pages but remove them if completely unused
                PageReference pageReference = pageReferences.get(indexRow.getPageId());
                pageReference.dereference(indexRow.getPosition());
                if (pageReference.getUsedPositionCount() == 0) {
                    pageReferences.set(indexRow.getPageId(), null);
                    memorySizeInBytes -= pageReference.getEstimatedSizeInBytes();
                    adjustMemoryPool();
                }
            }

            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            return pageBuilder.build();
        }

        private ObjectBigArray<IndexRow> nextRows() {
            if (rowHeap != null && !rowHeap.isEmpty()) {
                heapSizeInBytes = rowHeap.getEstimatedSizeInBytes();
                currentSize = rowHeap.size();
                // sort output rows in a big array in case there are too many rows
                ObjectBigArray<IndexRow> sortedRows = new ObjectBigArray<>();
                sortedRows.ensureCapacity(currentSize);
                int index = currentSize - 1;
                while (!rowHeap.isEmpty()) {
                    sortedRows.set(index, rowHeap.dequeue());
                    index--;
                }
                return sortedRows;
            } else {
                return null;
            }
        }
    }
}
