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

package com.alibaba.polardbx.executor.chunk;

public abstract class AbstractBlockBuilder implements BlockBuilder {

    final int initialCapacity;
    final protected BatchedArrayList.BatchBooleanArrayList valueIsNull;
    protected boolean containsNull;

    public AbstractBlockBuilder(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.valueIsNull = new BatchedArrayList.BatchBooleanArrayList(initialCapacity);
        this.containsNull = false;
    }

    @Override
    public int getPositionCount() {
        return valueIsNull.size();
    }

    @Override
    public boolean isNull(int position) {
        checkReadablePosition(position);
        return valueIsNull.getBoolean(position);
    }

    @Override
    public void ensureCapacity(int capacity) {
        valueIsNull.ensureCapacity(capacity);
    }

    protected void appendNullInternal() {
        valueIsNull.add(true);
        containsNull = true;
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

    protected int getCapacity() {
        return valueIsNull.elements().length;
    }

    @Override
    public final long hashCodeUseXxhash(int pos) {
        throw new UnsupportedOperationException(
            "Block builder not support hash code calculated by xxhash, you should convert it to block first");
    }
}
