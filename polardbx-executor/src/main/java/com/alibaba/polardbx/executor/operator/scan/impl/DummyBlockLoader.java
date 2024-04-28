package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.columnar.BlockLoader;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.io.IOException;

public class DummyBlockLoader implements BlockLoader {

    private final int startPosition;
    private final int positionCount;

    public DummyBlockLoader(int startPosition, int positionCount) {
        this.startPosition = startPosition;
        this.positionCount = positionCount;
    }

    @Override
    public Block load(DataType dataType, int[] selection, int selSize) throws IOException {
        return null;
    }

    @Override
    public ColumnReader getColumnReader() {
        return null;
    }

    @Override
    public CacheReader<Block> getCacheReader() {
        return null;
    }

    @Override
    public int startPosition() {
        return startPosition;
    }

    @Override
    public int positionCount() {
        return positionCount;
    }
}
