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

import com.alibaba.polardbx.common.utils.bloomfilter.ConcurrentIntBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.operator.util.ConcurrentBitSet;
import com.alibaba.polardbx.executor.operator.util.ConcurrentBitSetImpl;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawDirectHashTable;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildOneChunk;

public class Synchronizer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);
    private final static int DEFAULT_CHUNK_LIMIT = 1000;

    /**
     * How many degrees of parallelism in this Synchronizer instance.
     */
    final int numberOfExec;

    /**
     * How many partitions in this Synchronizer instance.
     * It's useful in local partition mode.
     */
    final int numberOfDataPartition;

    final AtomicInteger buildCount = new AtomicInteger();

    /**
     * used to synchronize probe side of reverse anti join
     */
    final AtomicInteger antiProbeCount = new AtomicInteger();

    ConcurrentRawDirectHashTable antiProbeFinished;

    volatile List<Integer> antJoinOutputRowId;

    // Shared States
    final ChunksIndex builderChunks;
    final ChunksIndex builderKeyChunks;

    volatile ConcurrentRawHashTable hashTable;
    int[] positionLinks;
    ConcurrentIntBloomFilter bloomFilter;
    boolean alreadyUseRuntimeFilter;
    boolean useBloomFilter;

    // exception during initializing hash table (e.g. MemoryNotEnoughException)
    volatile Throwable initException;

    volatile ConcurrentBitSet joinNullRowBitSet;
    Object isBitSetInitialized = new Object();

    int maxIndex = -1;
    AtomicInteger nextIndexId = new AtomicInteger(0);

    // To Record if an executor in thread have been finished.
    final ConcurrentHashMap<Integer, Object> operatorIdBitmap = new ConcurrentHashMap<>();

    ConcurrentRawDirectHashTable matchedPosition;

    // Thread Local arrays for hash code vector
    final int chunkLimit;
    final ThreadLocal<int[]> hashCodeResultsThreadLocal;
    final ThreadLocal<int[]> intermediatesThreadLocal;
    final ThreadLocal<int[]> blockHashCodesThreadLocal;

    final JoinRelType joinType;
    final boolean outerDriver;

    // partition-level chunk index to avoid lock
    final ParallelHashJoinExec.PartitionChunksIndex[] partitionChunksIndexes;
    final AtomicLong nextPartition;

    final boolean isHashTableShared;
    Map<Integer, SynchronizerRFMerger> synchronizerRFMergers;

    private int probeParallelism;

    public Synchronizer(JoinRelType joinType, boolean outerDriver, int numberOfExec,
                        boolean alreadyUseRuntimeFilter, boolean useBloomFilter, int chunkLimit,
                        int probeParallelism, int numberOfDataPartition, boolean isHashTableShared) {
        this.isHashTableShared = isHashTableShared;
        this.joinType = joinType;
        this.outerDriver = outerDriver;

        this.numberOfExec = numberOfExec;
        this.alreadyUseRuntimeFilter = alreadyUseRuntimeFilter;
        this.useBloomFilter = useBloomFilter;
        this.builderChunks = new ChunksIndex();
        this.builderKeyChunks = new ChunksIndex();

        this.chunkLimit = chunkLimit;
        hashCodeResultsThreadLocal = ThreadLocal.withInitial(() -> new int[chunkLimit]);
        intermediatesThreadLocal = ThreadLocal.withInitial(() -> new int[chunkLimit]);
        blockHashCodesThreadLocal = ThreadLocal.withInitial(() -> new int[chunkLimit]);

        // avoid unnecessary allocate
        if (joinType == JoinRelType.ANTI && outerDriver) {
            this.antiProbeFinished = new ConcurrentRawDirectHashTable(probeParallelism);
        }

        this.partitionChunksIndexes = new ParallelHashJoinExec.PartitionChunksIndex[numberOfExec];
        this.nextPartition = new AtomicLong(0L);
        for (int i = 0; i < numberOfExec; i++) {
            partitionChunksIndexes[i] = new ParallelHashJoinExec.PartitionChunksIndex();
        }
        this.numberOfDataPartition = numberOfDataPartition;
        this.probeParallelism = probeParallelism;
        this.synchronizerRFMergers = new TreeMap<>();
    }

    // Just for test
    public Synchronizer(JoinRelType joinType, boolean outerDriver, int numberOfExec,
                        boolean alreadyUseRuntimeFilter, int probeParallelism) {
        this(joinType, outerDriver, numberOfExec, alreadyUseRuntimeFilter, true, DEFAULT_CHUNK_LIMIT,
            probeParallelism, -1, false);
    }

    public void putSynchronizerRFMerger(int ordinal, SynchronizerRFMerger synchronizerRFMerger) {
        synchronizerRFMergers.put(ordinal, synchronizerRFMerger);
    }

    public ParallelHashJoinExec.PartitionChunksIndex nextPartition() {
        return partitionChunksIndexes[(int) (nextPartition.getAndIncrement() % numberOfExec)];
    }

    public void initHashTable(MemoryAllocatorCtx ctx) {
        if (hashTable == null) {
            long start = System.nanoTime();

            // merge chunk index
            List<ChunksIndex> partitionBuilderChunks = Arrays.stream(partitionChunksIndexes)
                .map(ParallelHashJoinExec.PartitionChunksIndex::getBuilderChunks).collect(Collectors.toList());
            List<ChunksIndex> partitionBuilderKeyChunks = Arrays.stream(partitionChunksIndexes)
                .map(ParallelHashJoinExec.PartitionChunksIndex::getBuilderKeyChunks).collect(Collectors.toList());
            builderChunks.merge(partitionBuilderChunks);
            builderKeyChunks.merge(partitionBuilderKeyChunks);

            final int size = builderKeyChunks.getPositionCount();

            // large memory allocation: hash table for build-side
            ctx.allocateReservedMemory(ConcurrentRawHashTable.estimateSizeInBytes(size));
            hashTable = new ConcurrentRawHashTable(size);

            if (outerDriver && (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI)) {
                // large memory allocation: hash table for reversed anti/semi join
                ctx.allocateReservedMemory(ConcurrentRawDirectHashTable.estimatedSizeInBytes(size));
                matchedPosition = new ConcurrentRawDirectHashTable(size);
            }

            // large memory allocation: linked list for build-side.
            ctx.allocateReservedMemory(SizeOf.sizeOfIntArray(size));
            positionLinks = new int[size];
            Arrays.fill(positionLinks, AbstractHashJoinExec.LIST_END);

            // large memory allocation: type-specific lists in key columns chunks index.
            ctx.allocateReservedMemory(builderKeyChunks.estimateTypedListSizeInBytes());
            builderKeyChunks.openTypedHashTable();

            // large memory allocation: type-specific lists in full columns chunks index.
            ctx.allocateReservedMemory(builderChunks.estimateTypedListSizeInBytes());
            builderChunks.openTypedHashTable();

            if (useBloomFilter && !alreadyUseRuntimeFilter
                && size <= AbstractJoinExec.BLOOM_FILTER_ROWS_LIMIT_FOR_PARALLEL
                && size > 0) {
                // large memory allocation: bloom-filter
                ctx.allocateReservedMemory(
                    ConcurrentIntBloomFilter.estimatedSizeInBytes(size, ConcurrentIntBloomFilter.DEFAULT_FPP));
                bloomFilter = ConcurrentIntBloomFilter.create(size);
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    MessageFormat.format("initialize hash table time cost = {0} ns, positionCount = {1}, "
                        + "hash table size = {2}", (System.nanoTime() - start), size, hashTable.size()));
            }

        }
    }

    public void buildHashTable(int partition, MemoryAllocatorCtx ctx, int[] ignoreNullBlocks,
                               int ignoreNullBlocksSize) {

        synchronized (this) {
            if (hashTable == null && initException == null) {
                try {
                    initHashTable(ctx);
                } catch (Throwable t) {
                    // Avoid allocating hash table after encountering out-of-memory exception.
                    this.initException = t;
                    throw t;
                }
            }
        }

        final int partitionSize = -Math.floorDiv(-builderKeyChunks.getChunkCount(), numberOfExec);
        final int startChunkId = partitionSize * partition;

        if (startChunkId >= builderKeyChunks.getChunkCount()) {
            return; // skip invalid chunk ranges
        }

        final int endChunkId = Math.min(startChunkId + partitionSize, builderKeyChunks.getChunkCount());
        final int startPosition = builderKeyChunks.getChunkOffset(startChunkId);
        final int endPosition = builderKeyChunks.getChunkOffset(endChunkId);

        int[] hashCodeResults = hashCodeResultsThreadLocal.get();
        int[] intermediates = intermediatesThreadLocal.get();
        int[] blockHashCodes = blockHashCodesThreadLocal.get();

        int position = startPosition;
        for (int chunkId = startChunkId; chunkId < endChunkId; ++chunkId) {

            // step1. add chunk into type list
            builderChunks.addChunkToTypedList(chunkId);
            builderKeyChunks.addChunkToTypedList(chunkId);

            // step2. add chunk into hash table.
            final Chunk keyChunk = builderKeyChunks.getChunk(chunkId);
            buildOneChunk(keyChunk, position, hashTable, positionLinks,
                hashCodeResults, intermediates, blockHashCodes, bloomFilter, ignoreNullBlocks,
                ignoreNullBlocksSize);

            position += keyChunk.getPositionCount();
        }

        // step3. add fragment-level runtime filter.
        if (synchronizerRFMergers != null && !synchronizerRFMergers.isEmpty()) {

            for (Map.Entry<Integer, SynchronizerRFMerger> entry : synchronizerRFMergers.entrySet()) {
                SynchronizerRFMerger merger = entry.getValue();

                switch (merger.getFragmentItem().getRFType()) {
                case BROADCAST:
                    merger.addChunksForBroadcastRF(builderKeyChunks, startChunkId, endChunkId);
                    break;
                case LOCAL:
                    merger.addChunksForPartialRF(builderKeyChunks, startChunkId, endChunkId,
                        isHashTableShared, numberOfDataPartition);
                    break;
                }
            }
        }

        assert position == endPosition;
    }

    public synchronized void buildAntiJoinOutputRowIds() {
        if (antJoinOutputRowId != null) {
            return;
        }
        antJoinOutputRowId = matchedPosition.getNotMarkedPosition();
    }

    // No lock is needed here because it executes only during the operator creation.
    public void recordOperatorIds(int operatorId) {
        this.operatorIdBitmap.put(operatorId, new Object());
    }

    // This section does not require locking because each operator has a different operatorId,
    // and the container itself is thread-safe
    public boolean consumeInputIsFinish(int operatorId) {
        this.operatorIdBitmap.remove(operatorId);
        return this.operatorIdBitmap.isEmpty();
    }

    public void buildNullBitSets(int buildSize) {
        if (joinNullRowBitSet == null) {
            synchronized (isBitSetInitialized) {
                if (joinNullRowBitSet == null) {
                    joinNullRowBitSet = new ConcurrentBitSetImpl(buildSize);
                    maxIndex = buildSize;
                }
            }
        }
    }

    public void markUsedKeys(int matchedPosition) {
        joinNullRowBitSet.set(matchedPosition);
    }

    public int nextUnmatchedPosition() {
        synchronized (joinNullRowBitSet) {
            final int currentIndexId = nextIndexId.get();
            if (maxIndex > currentIndexId) {

                // Each thread retrieves the next unmarked position as nextIndexId,
                // which must be ensured to be monotonically increasing and unique.
                int unmatchedPosition = joinNullRowBitSet.nextClearBit(currentIndexId);
                nextIndexId.set(unmatchedPosition + 1);

                if (maxIndex > unmatchedPosition) {
                    return unmatchedPosition;
                } else {
                    return -1;
                }

            } else {
                return -1;
            }
        }
    }

    public boolean finishIterator() {
        return nextIndexId.get() >= maxIndex;
    }

    @VisibleForTesting
    public int getNumberOfExec() {
        return numberOfExec;
    }

    public int getProbeParallelism() {
        return probeParallelism;
    }

    public ConcurrentRawDirectHashTable getMatchedPosition() {
        return matchedPosition;
    }

    public void close() {
        this.builderChunks.close();
        this.builderKeyChunks.close();
    }

}
