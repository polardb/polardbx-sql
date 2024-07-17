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
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SynchronizerRFMerger implements RuntimeFilterMerger {
    private static final Logger LOGGER = LoggerFactory.getLogger(FragmentRFManager.class);

    // plan fragment level runtime filter manager.
    private final FragmentRFManager fragmentRFManager;
    private final FragmentRFItem rfItem;

    private final int buildSideParallelism;
    private final int blockChannel;

    private RFBloomFilter[] rfBloomFilters;
    private Lock[] bfInitializingLocks;
    private AtomicInteger bfParallelismCounter;
    private AtomicInteger bfPartitionCounter;

    private volatile RFBloomFilter globalRFBloomFilter;

    public SynchronizerRFMerger(FragmentRFManager fragmentRFManager, FragmentRFItem rfItem, int buildSideParallelism,
                                int blockChannel) {
        this.fragmentRFManager = fragmentRFManager;
        this.rfItem = rfItem;
        this.buildSideParallelism = buildSideParallelism;

        final int runtimeFilterCount =
            (rfItem.getRFType() == FragmentRFManager.RFType.BROADCAST
                || fragmentRFManager.getTotalPartitionCount() <= 0)
                ? 1 : fragmentRFManager.getTotalPartitionCount();
        this.rfBloomFilters = new RFBloomFilter[runtimeFilterCount];
        this.bfInitializingLocks = new Lock[runtimeFilterCount];
        this.blockChannel = blockChannel;

        for (int i = 0; i < bfInitializingLocks.length; i++) {
            bfInitializingLocks[i] = new ReentrantLock();
        }
        bfParallelismCounter = new AtomicInteger(buildSideParallelism);

        final int partitionsOfNode = fragmentRFManager.getPartitionsOfNode();
        bfPartitionCounter = new AtomicInteger(partitionsOfNode);
    }

    @Override
    public FragmentRFManager getFragmentRFManager() {
        return this.fragmentRFManager;
    }

    @Override
    public FragmentRFItem getFragmentItem() {
        return this.rfItem;
    }

    @Override
    public void addChunksForBroadcastRF(ChunksIndex builderKeyChunks, final int startChunkId, final int endChunkId) {
        final long buildSideRows = builderKeyChunks.getPositionCount();
        final List<ColumnarScanExec> registeredSource = rfItem.getRegisteredSource();
        final long rowUpperBound = fragmentRFManager.getUpperBound();
        final long rowLowerBound = fragmentRFManager.getLowerBound();

        if (registeredSource == null || registeredSource.isEmpty()) {
            // No source has been registered, don't build RF.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format(
                    "Do not have any source exec for join buildSide = {0}, probeSide = {1}",
                    rfItem.getBuildColumnName(), rfItem.getProbeColumnName()
                ));
            }

            return;
        }

        if (buildSideRows > rowUpperBound || buildSideRows < rowLowerBound) {
            // For global RF, the rows in build side is more than the given threshold, so don't build RF.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format(
                    "Do not generate RF for join buildSide = {0}, probeSide = {1}, buildSideRows = {2}, threshold = {3}",
                    rfItem.getBuildColumnName(), rfItem.getProbeColumnName(),
                    buildSideRows, rowUpperBound
                ));
            }
            return;
        }

        // construct the global RF in concurrency safe mode.
        long startGlobalMode = System.nanoTime();
        if (globalRFBloomFilter == null) {
            synchronized (this) {
                if (globalRFBloomFilter == null) {
                    globalRFBloomFilter = RFBloomFilter.createBlockLongBloomFilter((int) buildSideRows);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("initialize global RF for buildSide = "
                            + rfItem.getBuildColumnName()
                            + ", probeSide = " + rfItem.getProbeColumnName()
                            + ", rowCount = "
                            + buildSideRows
                            + ", sizeInBytes = "
                            + (globalRFBloomFilter).sizeInBytes());
                    }

                }
            }
        }

        // create thread-local RF-BloomFilter component.
        RFBloomFilter rfComponent = RFBloomFilter.createBlockLongBloomFilter((int) buildSideRows);

        // Every thread has it's owned chunk id to build RF.
        for (int chunkId = startChunkId; chunkId < endChunkId; ++chunkId) {
            final Chunk keyChunk = builderKeyChunks.getChunk(chunkId);
            Block block = keyChunk.getBlock(blockChannel);
            block.addLongToBloomFilter(rfComponent);
        }

        synchronized (globalRFBloomFilter) {
            // merge thread-local component into globalRFBloomFilter
            globalRFBloomFilter.merge(rfComponent);

            // check if all thread has finished the global RF building.
            if (bfParallelismCounter.decrementAndGet() == 0) {
                rfItem.assignRF(new RFBloomFilter[] {globalRFBloomFilter});

                long endGlobalMode = System.nanoTime();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("build global RF timeCost = " + (endGlobalMode - startGlobalMode)
                        + "ns for buildSide = "
                        + rfItem.getBuildColumnName()
                        + ", probeSide = " + rfItem.getProbeColumnName());
                }
            }
        }
    }

    @Override
    public void addChunksForPartialRF(ChunksIndex builderKeyChunks, final int startChunkId,
                                      final int endChunkId, boolean isHashTableShared, int partitionsInSynchronizer) {
        final long buildSideRows = builderKeyChunks.getPositionCount();

        final int totalPartitionCount = fragmentRFManager.getTotalPartitionCount();
        final double fpp = fragmentRFManager.getDefaultFpp();
        final List<ColumnarScanExec> registeredSource = rfItem.getRegisteredSource();

        if (registeredSource == null || registeredSource.isEmpty()) {
            // No source has been registered, don't build RF.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format(
                    "Do not have any source exec for join buildSide = {0}, probeSide = {1}",
                    rfItem.getBuildColumnName(), rfItem.getProbeColumnName()
                ));
            }

            return;
        }

        if (rfItem.useXXHashInBuild()) {
            // For partition wise mode, a chunk is only from one partition.

            long bfSizeOfPartition = getBfSizeOfPartition(isHashTableShared, partitionsInSynchronizer, buildSideRows);

            for (int chunkId = startChunkId; chunkId < endChunkId; ++chunkId) {
                final Chunk keyChunk = builderKeyChunks.getChunk(chunkId);
                Block block = keyChunk.getBlock(blockChannel);

                // the partition number within a block is consistent.
                final int rfPartition = getPartition(block, 0, totalPartitionCount);

                // To initialization.
                if (rfBloomFilters[rfPartition] == null) {
                    bfInitializingLocks[rfPartition].lock();
                    try {
                        if (rfBloomFilters[rfPartition] == null) {

                            rfBloomFilters[rfPartition] = RFBloomFilter.createBlockLongBloomFilter(
                                (int) bfSizeOfPartition);

                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("initialize local RF for buildSide = "
                                    + rfItem.getBuildColumnName()
                                    + ", probeSide = " + rfItem.getProbeColumnName()
                                    + ", rowCount = "
                                    + bfSizeOfPartition
                                    + ", sizeInBytes = "
                                    + rfBloomFilters[rfPartition].sizeInBytes()
                                    + ", partition num = " + rfPartition);
                            }

                        }

                    } finally {
                        bfInitializingLocks[rfPartition].unlock();
                    }
                }

                // Get write lock for a chunk.
                bfInitializingLocks[rfPartition].lock();
                try {
                    // write into thread-local component.
                    block.addLongToBloomFilter(rfBloomFilters[rfPartition]);
                } finally {
                    bfInitializingLocks[rfPartition].unlock();
                }

            }
        } else {
            for (int chunkId = startChunkId; chunkId < endChunkId; ++chunkId) {
                final Chunk keyChunk = builderKeyChunks.getChunk(chunkId);
                Block block = keyChunk.getBlock(blockChannel);

                if (bfPartitionCounter.get() > 0) {
                    // Bad case for initialization.
                    for (int pos = 0; pos < keyChunk.getPositionCount(); pos++) {

                        final int rfPartition = getPartition(block, pos, totalPartitionCount);

                        if (rfBloomFilters[rfPartition] == null) {

                            bfInitializingLocks[rfPartition].lock();
                            try {
                                if (rfBloomFilters[rfPartition] == null) {
                                    // To initialize the bloom-filter for each partition.
                                    final int partitionsOfNode = fragmentRFManager.getPartitionsOfNode();
                                    final long bfSizeOfPartition = buildSideRows / partitionsOfNode;

                                    rfBloomFilters[rfPartition] =
                                        ConcurrentIntBloomFilter.create(bfSizeOfPartition, fpp);

                                    bfPartitionCounter.decrementAndGet();

                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("initialize local RF for buildSide = "
                                            + rfItem.getBuildColumnName()
                                            + ", probeSide = " + rfItem.getProbeColumnName()
                                            + ", rowCount = "
                                            + bfSizeOfPartition
                                            + ", sizeInBytes = "
                                            + rfBloomFilters[rfPartition].sizeInBytes()
                                            + ", partition num = " + rfPartition);
                                    }

                                }

                            } finally {
                                bfInitializingLocks[rfPartition].unlock();
                            }
                        }

                        rfBloomFilters[rfPartition].putInt(block.hashCode(pos));
                    }
                } else {

                    // CAS operation on array for each row.
                    block.addIntToBloomFilter(totalPartitionCount, rfBloomFilters);

                }
            }
        }

        // check if all thread has finished the partition RF building.
        if (bfParallelismCounter.decrementAndGet() == 0) {
            rfItem.assignRF(rfBloomFilters);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("assign RF : " + rfBloomFilters.length
                    + " for FragmentRFManager: " + fragmentRFManager);
            }

        }

    }

    // for test
    public AtomicInteger getBfParallelismCounter() {
        return bfParallelismCounter;
    }

    private long getBfSizeOfPartition(boolean isHashTableShared, int partitionsInSynchronizer, long buildSideRows) {
        long bfSizeOfPartition;
        final int partitionsOfNode = fragmentRFManager.getPartitionsOfNode();
        if (isHashTableShared) {
            // To initialize the bloom-filter for each partition.
            bfSizeOfPartition = (long) Math.ceil(buildSideRows * 1.0d / partitionsOfNode);
        } else if (partitionsInSynchronizer > 0) {
            // Valid partition number.
            bfSizeOfPartition = (long) Math.ceil(buildSideRows * 1.0d / partitionsInSynchronizer);
        } else {
            // Invalid partition number. Evenly divided according to total parallelism.
            bfSizeOfPartition = (long) Math.ceil(
                buildSideRows / (partitionsOfNode * 1.0d / buildSideParallelism));
        }
        return bfSizeOfPartition;
    }

    private static int getPartition(Block block, int position, int partitionCount) {

        // Convert the searchVal from field space to hash space
        long hashVal = block.hashCodeUseXxhash(position);
        int partition = (int) ((hashVal & Long.MAX_VALUE) % partitionCount);

        return partition;
    }
}
