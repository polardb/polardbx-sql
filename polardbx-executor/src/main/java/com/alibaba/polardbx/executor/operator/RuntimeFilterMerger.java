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

import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;

/**
 * A concurrency-safe runtime-filter merger, receiving chunks from different threads, writing into
 * runtime-filter components or partial runtime-filters, and finally merging into FragmentRFManager.
 */
public interface RuntimeFilterMerger {
    /**
     * A binding FragmentRFManager of this RuntimeFilterMerger object.
     *
     * @return FragmentRFManager
     */
    FragmentRFManager getFragmentRFManager();

    FragmentRFItem getFragmentItem();

    /**
     * Add chunks into global runtime filter.
     *
     * @param builderKeyChunks A chunk collection.
     * @param startChunkId The start position of this chunk in collection.
     * @param endChunkId The end position of this chunk in collection.
     */
    void addChunksForBroadcastRF(ChunksIndex builderKeyChunks, int startChunkId, int endChunkId);

    /**
     * Add chunks into local partitioned runtime filter.
     *
     * @param builderKeyChunks A chunk collection.
     * @param startChunkId The start position of this chunk in collection.
     * @param endChunkId The end position of this chunk in collection.
     * @param isHashTableShared If the hash table is shared by all threads, we will choose the
     * hash_table_size / (partition_num / worker_count) to be the estimated size of partial runtime-filter. And if not,
     * the estimated size of partial runtime-filter will be builderKeyChunks_length / partitionNum.
     */
    void addChunksForPartialRF(ChunksIndex builderKeyChunks, int startChunkId,
                               int endChunkId, boolean isHashTableShared, int partitionsInSynchronizer);
}
