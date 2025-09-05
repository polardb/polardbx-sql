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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

/**
 * Byte Array (<pre>byte[]</pre>) Block Builder
 *
 */
public class ByteArrayBlockBuilder extends AbstractBlockBuilder {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ByteArrayBlockBuilder.class).instanceSize();

    private final int initialByteArrayLength;
    final MemoryCountableIntArrayList offsets; // records where the bytes end at
    final MemoryCountableByteArrayList data;

    public ByteArrayBlockBuilder(int capacity, int expectedItemLength) {
        super(capacity);
        this.offsets = new MemoryCountableIntArrayList(capacity);
        this.data = new MemoryCountableByteArrayList(capacity * expectedItemLength);
        this.initialByteArrayLength = expectedItemLength;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + offsets.getMemoryUsage() + data.getMemoryUsage() + valueIsNull.getMemoryUsage();
    }

    @Override
    public void writeByteArray(byte[] value) {
        data.addElements(data.size(), value);
        offsets.add(data.size());
        valueIsNull.add(false);
    }

    @Override
    public void writeByteArray(byte[] value, int offset, int length) {
        data.addElements(data.size(), value, offset, length);
        offsets.add(data.size());
        valueIsNull.add(false);
    }

    @Override
    public byte[] getByteArray(int position) {
        checkReadablePosition(position);
        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return Arrays.copyOfRange(data.elements(), beginOffset, endOffset);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getByteArray(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof byte[]);
        writeByteArray((byte[]) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        offsets.ensureCapacity(capacity);
        data.ensureCapacity(capacity * initialByteArrayLength);
    }

    @Override
    public Block build() {
        byte[] validData = Arrays.copyOf(data.elements(), data.size());
        return new ByteArrayBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            offsets.elements(), validData);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        offsets.add(data.size());
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new ByteArrayBlockBuilder(getCapacity(), initialByteArrayLength);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        int beginOffset = position > 0 ? offsets.getInt(position - 1) : 0;
        int endOffset = offsets.getInt(position);
        return ChunkUtil.hashCode(data.elements(), beginOffset, endOffset);
    }

    int beginOffset(int position) {
        return position > 0 ? offsets.getInt(position - 1) : 0;
    }

    int endOffset(int position) {
        return offsets.getInt(position);
    }
}
