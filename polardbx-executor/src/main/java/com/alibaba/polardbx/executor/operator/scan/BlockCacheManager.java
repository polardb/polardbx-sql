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

package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.operator.scan.impl.SimpleBlockCacheManager;
import org.apache.hadoop.fs.Path;

import java.util.Map;

/**
 * It is responsible for managing block-level in-memory cache.
 *
 * @param <VECTOR> class of block.
 */
public interface BlockCacheManager<VECTOR> {
    /**
     * The TTL of in-flight cache entries.
     */
    int IN_FLIGHT_CACHE_TTL_IN_SECOND = 5;

    /**
     * The limitation of in-flight entries
     */
    long MAXIMUM_IN_FLIGHT_ENTRIES = 1 << 12;

    float RATIO = DynamicConfig.getInstance().getBlockCacheMemoryFactor();
    long MAXIMUM_MEMORY_SIZE = (long) (Runtime.getRuntime().maxMemory() * RATIO);

    BlockCacheManager<Block> INSTANCE = new SimpleBlockCacheManager();

    static BlockCacheManager<Block> getInstance() {
        return INSTANCE;
    }

    /**
     * Get memory size in bytes held by block cache.
     *
     * @return size in bytes
     */
    long getMemorySize();

    /**
     * Clear all caches.
     */
    void clear();

    /**
     * Generate cache stats packet for `show cache stats` statement.
     *
     * @return cache stats packet
     */
    byte[][] generateCacheStatsPacket();

    /**
     * We consider {group_id, column_id} has already been cached only if all blocks in row-group are cached.
     *
     * @param path file path
     * @param stripeId stripe id
     * @param rowGroupId row group id
     * @param columnId column id
     * @return True if row group has been cached.
     */
    boolean isCached(Path path, int stripeId, int rowGroupId, int columnId);

    /**
     * Get a sequence of cached blocks from given row-group and column.
     *
     * @param path file path
     * @param stripeId stripe id
     * @param rowGroupId row group id
     * @param columnId column id
     * @return Iterator of cached blocks.
     */
    SeekableIterator<VECTOR> getCaches(Path path, int stripeId, int rowGroupId, int columnId);

    /**
     * Get all the cached blocks in this stripe with given column.
     *
     * @param path file path
     * @param stripeId stripe id
     * @param columnId column id
     * @param rowGroupIncluded selected row groups.
     * @return Mapping from row-group id to Iterator of cached blocks.
     */
    Map<Integer, SeekableIterator<VECTOR>> getCachedRowGroups(Path path, int stripeId, int columnId,
                                                              boolean[] rowGroupIncluded);

    Map<Integer, SeekableIterator<VECTOR>> getInFlightCachedRowGroups(Path path, int stripeId, int columnId,
                                                                      boolean[] rowGroupIncluded);

    /**
     * Put block into cache manager and check if rows of blocks in this row-group is out of total rows.
     * If true, we consider this row-group with given column has already been cached.
     *
     * @param block data block.
     * @param totalRows total rows of this row group.
     * @param path file path
     * @param stripeId stripe id
     * @param rowGroupId row group id
     * @param columnId column id
     * @param position starting position of block in this row group.
     * @param rows rows of block.
     */
    void putCache(VECTOR block, int chunkLimit, int totalRows,
                  Path path, int stripeId, int rowGroupId, int columnId, int position, int rows);

    long getHitCount();

    long getMissCount();
}
