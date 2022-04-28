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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;

public abstract class AbstractBlockBuilder implements BlockBuilder {

    public final int initialCapacity;
    public final BooleanArrayList valueIsNull;
    protected boolean containsNull;

    AbstractBlockBuilder(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.valueIsNull = new BooleanArrayList(initialCapacity);
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

    void appendNullInternal() {
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

    void checkReadablePosition(int position) {
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

    int getCapacity() {
        return valueIsNull.elements().length;
    }
}
