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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

/**
 * String Block Builder
 *
 */
public class StringBlockBuilder extends AbstractBlockBuilder {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StringBlockBuilder.class).instanceSize();

    private final int initialStringLength;
    final MemoryCountableIntArrayList offsets; // records where the bytes end at
    final MemoryCountableCharArrayList data;

    @FieldMemoryCounter(value = false)
    final DataType dataType;

    public StringBlockBuilder(int capacity, int expectedStringLength) {
        this(null, capacity, expectedStringLength);
    }

    public StringBlockBuilder(DataType dataType, int capacity, int expectedStringLength) {
        super(capacity);
        this.dataType = dataType;
        this.offsets = new MemoryCountableIntArrayList(capacity);
        this.data = new MemoryCountableCharArrayList(capacity * expectedStringLength);
        this.initialStringLength = expectedStringLength;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(offsets)
            + FastMemoryCounter.sizeOf(data)
            + FastMemoryCounter.sizeOf(valueIsNull);
    }

    @Override
    public void writeString(String value) {
        for (int i = 0; i < value.length(); i++) {
            data.add(value.charAt(i));
        }
        valueIsNull.add(false);
        offsets.add(data.size());
    }

    @Override
    public String getString(int position) {
        checkReadablePosition(position);
        int beginOffset = beginOffset(position);
        int endOffset = endOffset(position);
        return new String(data.elements(), beginOffset, endOffset - beginOffset);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getString(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof String);
        writeString((String) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        offsets.ensureCapacity(capacity);
        data.ensureCapacity(capacity * initialStringLength);
    }

    @Override
    public Block build() {
        char[] validData = Arrays.copyOf(data.elements(), data.size());
        return new StringBlock(dataType, 0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            offsets.elements(),
            validData);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        offsets.add(data.size());
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new StringBlockBuilder(dataType, getCapacity(), initialStringLength);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        int beginOffset = position > 0 ? offsets.getInt(position - 1) : 0;
        int endOffset = offsets.getInt(position);

        return ChunkUtil.hashCodeIgnoreCase(data.elements(), beginOffset, endOffset);
    }

    int beginOffset(int position) {
        return position > 0 ? offsets.getInt(position - 1) : 0;
    }

    int endOffset(int position) {
        return offsets.getInt(position);
    }
}
