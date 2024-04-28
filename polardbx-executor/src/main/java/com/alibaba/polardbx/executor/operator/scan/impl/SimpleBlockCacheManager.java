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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.SeekableIterator;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

import java.text.MessageFormat;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import static com.alibaba.polardbx.gms.engine.FileStoreStatistics.CACHE_STATS_FIELD_COUNT;

/**
 * A test implementation of BlockCacheManager.
 * The CacheId is combined by:
 * pathId (tableId + fileId) (32bit) + rowGroupId (16bit) + stripeId (4bit) + columnId (12bit).
 */
public class SimpleBlockCacheManager implements BlockCacheManager<Block> {
    public static final String BLOCK_CACHE = "BLOCK CACHE";
    public static final String IN_MEMORY = "IN MEMORY";

    // mark word (8 Bytes) + class pointer (4 Bytes) + long value (8 Bytes)
    private static final int LONG_OBJECT_IN_BYTES = 20;

    /**
     * Caching the block collections that covering all blocks in row-group which the block in collection belongs to.
     */
    private Cache<Long, SimplifiedBlockCache> validCache;

    /**
     * The in-flight cache is responsible for caching the block collection
     * that does not completely contain the all blocks in row-group they belong to.
     */
    private LoadingCache<Long, BlockCache> inFlightCache;

    private AtomicLong size;
    private AtomicLong hitCount;
    private AtomicLong flightCount;
    private AtomicLong missCount;
    private AtomicLong quotaExceedCount;

    /**
     * The CacheId is combined by:
     * pathId (tableId + fileId) (32bit) + rowGroupId (16bit) + stripeId (4bit) + columnId (12bit).
     */
    public static long buildBlockCacheKey(Path path, int stripeId, int columnId, int rowGroupId) {
        // The current method is not secure enough. The ideal situation is a compressed value of two digits:
        // table_id + file_id
        int pathHash = path.toString().hashCode();

        // rowGroupId (16bit) + stripeId (4bit) + columnId (12bit)
        int s = (stripeId << 12) | (columnId & 0x0FFF);
        int innerId = (rowGroupId << 16) | (s & 0xFFFF);

        return (((long) pathHash) << 32) | (innerId & 0xffffffffL);
    }

    public SimpleBlockCacheManager() {
        this.size = new AtomicLong(0L);
        this.hitCount = new AtomicLong(0L);
        this.missCount = new AtomicLong(0L);
        this.quotaExceedCount = new AtomicLong(0L);
        this.flightCount = new AtomicLong(0L);
        this.validCache = Caffeine.newBuilder()
            .maximumWeight(MAXIMUM_MEMORY_SIZE)
            .weigher((Weigher<Long, SimplifiedBlockCache>) (key, value) ->

                // calculate memory size of block cache for cache weight.
                LONG_OBJECT_IN_BYTES + value.memorySize()
            )
            .removalListener((longValue, simplifiedBlockCache, removalCause) -> {
                    // decrement memory size when invalidate block cache.
                    size.getAndAdd(-(LONG_OBJECT_IN_BYTES + simplifiedBlockCache.memorySize()));
                    quotaExceedCount.getAndIncrement();
                }
            )
            .build();

        this.inFlightCache = Caffeine.newBuilder()
            .maximumSize(MAXIMUM_IN_FLIGHT_ENTRIES)
            .removalListener((RemovalListener<Long, BlockCache>) (longValue, blockCache, removalCause) -> {
                // decrement memory size when invalidate block cache.
                size.getAndAdd(-(LONG_OBJECT_IN_BYTES + blockCache.memorySize()));
            })
            .expireAfterWrite(IN_FLIGHT_CACHE_TTL_IN_SECOND, TimeUnit.SECONDS)
            .build(new CacheLoader<Long, BlockCache>() {
                @Override
                public BlockCache load(Long key) {

                    // calculate memory size of block cache and cache key
                    BlockCache result = new BlockCache();
                    size.getAndAdd(result.memorySize() + LONG_OBJECT_IN_BYTES);
                    return result;
                }
            });
    }

    /**
     * A cache unit for all blocks within a row-group.
     */
    private static class BlockCache {
        private static final int BASE_MEMORY_SIZE = 96;
        public static final int UNSET_CHUNK_LIMIT = -1;

        /**
         * The fixed limitation of block position count.
         */
        private int chunkLimit;

        /**
         * The block cache is completed only if all slot in blockBitmap marked as TRUE.
         */
        private AtomicBoolean isCompleted;

        /**
         * The total number of rows in the row-group.
         */
        private int rowCountInGroup;

        /**
         * Responsible for marking the existence of all blocks within the row-group.
         */
        private boolean[] blockBitmap;

        /**
         * Storing the blocks within the row-group.
         * The index of this array is equal to block.positionCount() / chunkLimit
         */
        private Block[] blocks;

        private StampedLock rwLock;

        BlockCache() {
            isCompleted = new AtomicBoolean(false);
            rwLock = new StampedLock();
            rowCountInGroup = -1;
            chunkLimit = UNSET_CHUNK_LIMIT;
        }

        public SimplifiedBlockCache simplify() {
            return new SimplifiedBlockCache(chunkLimit, blocks);
        }

        public int memorySize() {
            int totalSize = BASE_MEMORY_SIZE;
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    if (blocks[i] != null) {
                        totalSize += blocks[i].getElementUsedBytes();
                    }
                }
            }

            if (blockBitmap != null) {
                totalSize += blockBitmap.length * Byte.BYTES;
            }
            return totalSize;
        }

        public boolean isCompleted() {
            return isCompleted.get();
        }

        /**
         * It's idempotent for store operation.
         *
         * @return TRUE if completed.
         */
        public boolean put(Block block, int rowCount, int position, int positionCount, int chunkLimit) {
            // check if rowCountInGroup has been set or consistent.
            Preconditions.checkArgument(this.rowCountInGroup == -1 || rowCount == rowCountInGroup);

            // check position alignment.
            Preconditions.checkArgument(position % chunkLimit == 0);
            Preconditions.checkArgument(positionCount == Math.min(chunkLimit, rowCount - position));

            if (this.chunkLimit == UNSET_CHUNK_LIMIT) {
                this.chunkLimit = chunkLimit;
            } else {
                Preconditions.checkArgument(this.chunkLimit == chunkLimit);
            }

            this.rowCountInGroup = rowCount;
            if (blockBitmap == null) {
                // Initialization if needed.
                int slots = (rowCountInGroup + chunkLimit - 1) / chunkLimit;
                blockBitmap = new boolean[slots];
                blocks = new Block[slots];
            }

            // mark and store the block, and check if all blocks have been stored.
            blockBitmap[position / chunkLimit] = true;
            blocks[position / chunkLimit] = block;
            for (boolean marked : blockBitmap) {
                if (!marked) {
                    return false;
                }
            }

            isCompleted.set(true);
            return true;
        }

        public SeekableIterator<Block> newIterator() {
            long stamp = rwLock.readLock();
            try {
                Preconditions.checkNotNull(blocks);
                return new BlockIterator();
            } finally {
                rwLock.unlockRead(stamp);
            }

        }

        /**
         * A simple block iterator that only maintain a block-index for randomly access.
         */
        private class BlockIterator implements SeekableIterator<Block> {

            @Override
            public Block seek(int position) {
                Preconditions.checkArgument(position < rowCountInGroup);
                Preconditions.checkArgument(chunkLimit > UNSET_CHUNK_LIMIT);
                int blockIndex = position / chunkLimit;
                if (blockIndex >= blocks.length) {
                    throw GeneralUtil.nestedException(MessageFormat.format(
                        "bad position: {0}, blocks array size: {1}, chunk limit: {2}",
                        position, blocks.length, chunkLimit));
                }
                return blocks[blockIndex];
            }

            @Override
            public boolean hasNext() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Block next() {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static class SimplifiedBlockCache {
        private static final int BASE_MEMORY_SIZE = 30;
        /**
         * The fixed limitation of block position count.
         * It must be less than 4096.
         */
        private final short chunkLimit;

        /**
         * Storing the blocks within the row-group.
         * The index of this array is equal to block.positionCount() / chunkLimit
         */
        private final Block[] blocks;

        SimplifiedBlockCache(int chunkLimit, Block[] blocks) {
            this.chunkLimit = (short) chunkLimit;
            this.blocks = blocks;
        }

        public SeekableIterator<Block> newIterator() {
            Preconditions.checkNotNull(blocks);
            return new BlockIterator();
        }

        public int memorySize() {
            int totalSize = BASE_MEMORY_SIZE;
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    if (blocks[i] != null) {
                        totalSize += blocks[i].getElementUsedBytes();
                    }
                }
            }
            return totalSize;
        }

        /**
         * A simple block iterator that only maintain a block-index for randomly access.
         */
        private class BlockIterator implements SeekableIterator<Block> {

            @Override
            public Block seek(int position) {
                int blockIndex = position / chunkLimit;
                if (blockIndex >= blocks.length) {
                    throw GeneralUtil.nestedException(MessageFormat.format(
                        "bad position: {0}, blocks array size: {1}, chunk limit: {2}",
                        position, blocks.length, chunkLimit));
                }
                return blocks[blockIndex];
            }

            @Override
            public boolean hasNext() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Block next() {
                throw new UnsupportedOperationException();
            }
        }
    }

    @Override
    public long getMemorySize() {
        return size.get();
    }

    @Override
    public void clear() {
        validCache.invalidateAll();
        inFlightCache.invalidateAll();
        size.set(0L);
        hitCount.set(0);
        missCount.set(0);
        quotaExceedCount.set(0);
        flightCount.set(0);
    }

    @Override
    public byte[][] generateCacheStatsPacket() {
        byte[][] results = new byte[CACHE_STATS_FIELD_COUNT][];
        int pos = 0;
        results[pos++] = BLOCK_CACHE.getBytes();
        results[pos++] = String.valueOf(size.get()).getBytes();
        results[pos++] = String.valueOf(validCache.estimatedSize()).getBytes();
        results[pos++] = String.valueOf(inFlightCache.estimatedSize()).getBytes();
        results[pos++] = String.valueOf(hitCount.get()).getBytes();
        results[pos++] = String.valueOf(flightCount.get()).getBytes();
        results[pos++] = String.valueOf(missCount.get()).getBytes();
        results[pos++] = String.valueOf(quotaExceedCount.get()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = IN_MEMORY.getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = new StringBuilder().append(MAXIMUM_MEMORY_SIZE).append(" BYTES").toString().getBytes();

        return results;
    }

    @Override
    public boolean isCached(Path path, int stripeId, int rowGroupId, int columnId) {
        return validCache.getIfPresent(buildBlockCacheKey(path, stripeId, columnId, rowGroupId)) != null;
    }

    @Override
    public SeekableIterator<Block> getCaches(Path path, int stripeId, int rowGroupId, int columnId) {
        SimplifiedBlockCache blockCache = validCache.getIfPresent(
            buildBlockCacheKey(path, stripeId, columnId, rowGroupId));
        if (blockCache != null) {
            hitCount.getAndIncrement();
            return blockCache.newIterator();
        }
        missCount.getAndIncrement();
        return null;
    }

    @Override
    public Map<Integer, SeekableIterator<Block>> getCachedRowGroups(Path path, int stripeId, int columnId,
                                                                    boolean[] rowGroupIncluded) {
        Map<Integer, SeekableIterator<Block>> result = new TreeMap<>();
        for (int groupId = 0; groupId < rowGroupIncluded.length; groupId++) {
            if (!rowGroupIncluded[groupId]) {
                continue;
            }
            // Collect all valid cache containing the blocks in selected row-groups.
            long cacheKey = buildBlockCacheKey(path, stripeId, columnId, groupId);
            SimplifiedBlockCache blockCache = validCache.getIfPresent(cacheKey);
            if (blockCache != null) {
                result.put(groupId, blockCache.newIterator());
                hitCount.getAndIncrement();
            } else {
                missCount.getAndIncrement();
            }
        }

        return result;
    }

    @Override
    public Map<Integer, SeekableIterator<Block>> getInFlightCachedRowGroups(Path path, int stripeId, int columnId,
                                                                            boolean[] rowGroupIncluded) {
        Map<Integer, SeekableIterator<Block>> result = new TreeMap<>();
        for (int groupId = 0; groupId < rowGroupIncluded.length; groupId++) {
            if (!rowGroupIncluded[groupId]) {
                continue;
            }
            // Collect all in-flight cache containing the part of blocks in selected row-groups.
            long cacheKey = buildBlockCacheKey(path, stripeId, columnId, groupId);
            BlockCache blockCache = inFlightCache.getIfPresent(cacheKey);
            if (blockCache != null) {
                flightCount.incrementAndGet();
                result.put(groupId, blockCache.newIterator());
            }
        }

        return result;
    }

    @Override
    public void putCache(Block block, int chunkLimit, int totalRows, Path path, int stripeId, int rowGroupId,
                         int columnId,
                         int position, int rows) {
        Preconditions.checkArgument(block != null && block.getPositionCount() == rows);
        long cacheKey = buildBlockCacheKey(path, stripeId, columnId, rowGroupId);

        try {
            // automatically increment size in flight
            BlockCache inFlight = inFlightCache.get(cacheKey);
            long stamp = inFlight.rwLock.writeLock();
            try {

                // Recheck the completed flag of this block cache. Do nothing if it's completed.
                if (inFlight.isCompleted()) {
                    return;
                }

                // Try to put block into block cache. It will strictly check the alignment and validity of block.
                boolean completed = inFlight.put(block, totalRows, position, rows, chunkLimit);
                size.addAndGet(block.getElementUsedBytes());

                // move the block-cache from in-flight cache into valid cache.
                if (completed) {
                    SimplifiedBlockCache simplifiedCache = inFlight.simplify();
                    validCache.put(cacheKey, simplifiedCache);

                    // automatically decrement size in flight
                    inFlightCache.invalidate(cacheKey);

                    size.addAndGet(LONG_OBJECT_IN_BYTES + simplifiedCache.memorySize());
                }
            } finally {
                inFlight.rwLock.unlockWrite(stamp);
            }

        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public long getHitCount() {
        return hitCount.get();
    }

    @Override
    public long getMissCount() {
        return missCount.get();
    }
}