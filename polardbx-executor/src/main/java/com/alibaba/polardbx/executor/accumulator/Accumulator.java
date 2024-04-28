package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public interface Accumulator {

    /**
     * Get expected input types. Returns null if any type(s) are accepted
     */
    DataType[] getInputTypes();

    /**
     * Append a new group with initial value
     */
    void appendInitValue();

    /**
     * Accumulate a value into group
     */
    void accumulate(int groupId, Chunk inputChunk, int position);

    default void accumulate(int groupId, Chunk inputChunk, int[] groupIdSelection, int selSize) {
        // Fall back to normal processing if method is not override.
        for (int i = 0; i < selSize; i++) {
            accumulate(groupId, inputChunk, groupIdSelection[i]);
        }
    }

    default void accumulate(int groupId, Chunk inputChunk, int startIndexIncluded, int endIndexExcluded) {
        // Fall back to normal processing if method is not override.
        for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
            accumulate(groupId, inputChunk, i);
        }
    }

    default void accumulate(int[] groupIds, Chunk inputChunk, int positionCount) {
        // Fall back to normal processing if method is not override.
        for (int position = 0; position < positionCount; position++) {
            accumulate(groupIds[position], inputChunk, position);
        }
    }

    // for group join
    // the probe positions array may have repeated elements like {0, 0, 1, 1, 1, 2, 5, 5, 7 ...}
    default void accumulate(int[] groupIds, Chunk inputChunk, int[] probePositions, int selSize) {
        // Fall back to normal processing if method is not override.
        for (int i = 0; i < selSize; i++) {
            int position = probePositions[i];
            accumulate(groupIds[position], inputChunk, position);
        }
    }

    /**
     * Get the aggregated result
     */
    void writeResultTo(int groupId, BlockBuilder bb);

    /**
     * Estimate the memory consumption
     */
    long estimateSize();
}
