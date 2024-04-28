package com.alibaba.polardbx.executor.chunk.columnar;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;

/**
 * The data block with delay materialization.
 */
public interface LazyBlock extends RandomAccessBlock, Block {
    /**
     * Get internal arrow-vector.
     */
    Block getLoaded();

    /**
     * Load arrow-block data lazily.
     */
    void load();

    /**
     * whether the array is already materialized.
     */
    boolean hasVector();

    BlockLoader getLoader();

    /**
     * Release the reference of column reader used by this lazy block.
     */
    void releaseRef();
}
