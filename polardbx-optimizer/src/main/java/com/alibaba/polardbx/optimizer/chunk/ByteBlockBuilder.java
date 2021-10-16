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

package com.alibaba.polardbx.optimizer.chunk;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

/**
 * Byte Block Builder
 *
 */
public class ByteBlockBuilder extends AbstractBlockBuilder {

    protected final ByteArrayList values;

    public ByteBlockBuilder(int capacity) {
        super(capacity);
        this.values = new ByteArrayList(capacity);
    }

    @Override
    public void writeByte(byte value) {
        values.add(value);
        valueIsNull.add(false);
    }

    @Override
    public byte getByte(int position) {
        checkReadablePosition(position);
        return values.getByte(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getByte(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Byte);
        writeByte((Byte) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new ByteBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null, values.elements());
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add((byte) 0);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new ByteBlockBuilder(getCapacity());
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Byte.hashCode(values.getByte(position));
    }
}
