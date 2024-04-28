package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;

public abstract class AbstractBatchBlockBuilder implements BlockBuilder {
    // fixed size of capacity
    final int initialCapacity;
    protected boolean[] valueIsNull;
    int currentIndex;
    protected boolean containsNull;

    public AbstractBatchBlockBuilder(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.valueIsNull = null;
        this.currentIndex = 0;
        this.containsNull = false;
    }

    protected void allocateNulls() {
        if (valueIsNull == null) {
            this.valueIsNull = new boolean[initialCapacity];
        }
    }

    @Override
    public int getPositionCount() {
        return currentIndex;
    }

    @Override
    public boolean isNull(int position) {
        checkReadablePosition(position);
        if (valueIsNull == null) {
            return false;
        }
        return valueIsNull[position];
    }

    @Override
    public void ensureCapacity(int capacity) {
        throw new UnsupportedOperationException();
    }

    protected void appendNullInternal() {
        allocateNulls();
        valueIsNull[currentIndex] = true;
        containsNull = true;
        currentIndex++;
    }

    void setContainsNull() {
        this.containsNull = true;
    }

    @Override
    public boolean mayHaveNull() {
        return containsNull;
    }

    protected void checkReadablePosition(int position) {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public final boolean equals(int position, Block otherBlock, int otherPosition) {
        throw new UnsupportedOperationException("Please invoke from block instead of block builder");
    }

    @Override
    public final void writePositionTo(int position, BlockBuilder blockBuilder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long estimateSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        throw new UnsupportedOperationException();
    }

    protected int getCapacity() {
        return initialCapacity;
    }
}
