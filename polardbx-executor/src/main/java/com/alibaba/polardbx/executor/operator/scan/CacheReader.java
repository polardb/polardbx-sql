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

import java.util.Map;

/**
 * Handle stripe-level cached blocks of given column at runtime.
 *
 * @param <VECTOR> class of block.
 */
public interface CacheReader<VECTOR> {
    /**
     * Initialize the cache reader with given cached blocks.
     *
     * @param allCaches given cached blocks.
     */
    void initialize(Map<Integer, SeekableIterator<VECTOR>> allCaches);

    /**
     * Initialize the cache reader with given cached blocks.
     *
     * @param allValidCaches valid caches that caching all blocks in total row-group.
     * @param inFlightCaches the in-flight caches that caching part of blocks in total row-group.
     */
    void initialize(Map<Integer, SeekableIterator<VECTOR>> allValidCaches,
                    Map<Integer, SeekableIterator<VECTOR>> inFlightCaches);

    /**
     * Check if this cache reader is initialized.
     *
     * @return TRUE if initialized.
     */
    boolean isInitialized();

    /**
     * The column id of this cache reader.
     *
     * @return The column id
     */
    int columnId();

    /**
     * The row-group bitmap covered by this cache reader.
     *
     * @return The row-group bitmap
     */
    boolean[] cachedRowGroupBitmap();

    /**
     * Get all caches held by this reader.
     *
     * @return mapping from row-group id to iterator of cached blocks.
     */
    Map<Integer, SeekableIterator<VECTOR>> allCaches();

    /**
     * Fetch the cache with given row-group and starting position.
     *
     * @param groupId row group
     * @param position starting position.
     * @return cached block.
     */
    VECTOR getCache(int groupId, int position);
}
