package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;

/**
 * Base class for Accumulator.
 * <p>
 * Override one of the three <code>accumulate</code> method to make it work
 *
 * @author Eric Fu
 */
abstract class AbstractAccumulator implements Accumulator {

    @Override
    public final void accumulate(int groupId, Chunk inputChunk, int position) {
        final int inputSize = getInputTypes().length;
        if (inputSize == 0) {
            accumulate(groupId);
        } else if (inputSize == 1) {
            accumulate(groupId, inputChunk.getBlock(0), position);
        } else {
            throw new UnsupportedOperationException(getClass().getName() + " has multiple arguments");
        }
    }

    /**
     * accumulate method with no arguments e.g. COUNT(*)
     */
    void accumulate(int groupId) {
        throw new AssertionError("not implemented");
    }

    /**
     * accumulate method with one arguments e.g. SUM(x)
     *
     * @param position value position in block
     */
    void accumulate(int groupId, Block block, int position) {
        throw new AssertionError("not implemented");
    }
}
