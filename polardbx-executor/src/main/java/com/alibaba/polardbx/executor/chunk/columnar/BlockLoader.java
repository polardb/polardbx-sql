package com.alibaba.polardbx.executor.chunk.columnar;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.io.IOException;

/**
 * A block-level loader of one column in one row group.
 * Several block-level loader will share the column reader spanning multiple row groups.
 */
public interface BlockLoader {
    /**
     * Trigger the processing of loading.
     */
    Block load(DataType dataType, int[] selection, int selSize) throws IOException;

    /**
     * Get the column reader inside this block loader.
     * Several block-level loader will share the column reader spanning multiple row groups.
     *
     * @return Column reader.
     */
    ColumnReader getColumnReader();

    CacheReader<Block> getCacheReader();

    int startPosition();

    int positionCount();
}
