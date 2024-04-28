package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.utils.Pair;

/**
 * A row group reader is responsible for lazy block allocation of all columns in a row group.
 *
 * @param <BATCH> the class of batch.
 */
public interface RowGroupReader<BATCH> {
    /**
     * Unique row group id in a ORC Stripe.
     */
    int groupId();

    /**
     * How many rows in this row group.
     */
    int rowCount();

    /**
     * How many batches exists in this row group.
     */
    int batches();

    /**
     * Batch batch = RowGroupReader.nextBatch
     * Block block = batch.blocks[col_id];
     * block->loader.load()  loader hold positions + length
     * block.column_reader.seek(row_index of this rg)
     * block.column_reader.nextVector();
     * we need boundary check
     */
    BATCH nextBatch();

    /**
     * Get the batch row range in total columnar file.
     *
     * @return pair of {start, length}
     */
    int[] batchRange();

}
