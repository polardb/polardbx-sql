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
