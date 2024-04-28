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

import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

/**
 * A Row Group Iterator holding a sequence of available row-group metas.
 * It's the unit of scan work scheduling, and maintain the stripe-level modules that
 * shared by all row-group and all columns in this stripe.
 *
 * @param <VECTOR> the class of a column in row group (value vector, array...)
 * @param <STATISTICS> the class of column statistics
 */
public interface RowGroupIterator<VECTOR, STATISTICS> extends Iterator<Void> {
    Path filePath();

    int stripeId();

    void noMoreChunks();

    boolean[] columnIncluded();

    /**
     * The bitmap of row groups included in this iterator.
     *
     * @return bitmap of row groups
     */
    boolean[] rgIncluded();

    /**
     * Seek to the first row group matched the clustering key range conjuncts.
     * After the seek, the current row group should contain the target row or it is on the left side of the row group that
     * contains the target row.
     *
     * @param rowId row id
     */
    void seek(int rowId);

    /**
     * Get the row group pointed by current iterator-pointer.
     */
    LogicalRowGroup<VECTOR, STATISTICS> current();

    /**
     * Get global block cache manager wrapped in this iterator.
     */
    BlockCacheManager<VECTOR> getCacheManager();

    /**
     * Get the stripe-loader of this row-group sequence.
     *
     * @return stripe loader.
     */
    StripeLoader getStripeLoader();

    /**
     * Get the column reader of given column-id.
     *
     * @param columnId column id
     * @return column reader
     */
    @Nullable
    ColumnReader getColumnReader(int columnId);

    /**
     * Get the block cache reader of given column-id.
     *
     * @param columnId column id
     * @return block cache reader
     */
    @Nullable
    CacheReader<VECTOR> getCacheReader(int columnId);

    void close(boolean force);
}
