package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.chunk.Chunk;
import org.apache.hadoop.fs.Path;

/**
 * Logical information of an orc row group.
 *
 * @param <VECTOR> the class of a column in row group (arrow block, value vector, array...)
 * @param <STATISTICS> the class of column statistics
 */
public interface LogicalRowGroup<VECTOR, STATISTICS> {
    String BLOCK_LOAD_TIMER = "BlockLoadTimer";
    String BLOCK_MEMORY_COUNTER = "BlockMemoryCounter";

    Path path();

    int stripeId();

    int groupId();

    /**
     * The row count of the row group.
     */
    int rowCount();

    /**
     * The starting row id of the row group.
     */
    int startRowId();

    RowGroupReader<Chunk> getReader();
}
