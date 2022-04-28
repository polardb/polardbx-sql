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

import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.Map;

/**
 * Enum Block Builder
 *
 * @author xiaoying
 */
public class EnumBlockBuilder extends AbstractBlockBuilder {

    private final int initialStringLength;
    final IntArrayList offsets; // records where the bytes end at
    final CharArrayList data;
    final Map<String, Integer> enumValues;
    private final EnumType enumType;

    public EnumBlockBuilder(int capacity, int expectedStringLength, Map<String, Integer> enumValues) {
        super(capacity);
        this.offsets = new IntArrayList(capacity);
        this.data = new CharArrayList(capacity * expectedStringLength);
        this.initialStringLength = expectedStringLength;
        this.enumValues = enumValues;
        this.enumType = new EnumType(enumValues);
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
        try {
            int beginOffset = beginOffset(position);
            int endOffset = endOffset(position);
            return new String(data.elements(), beginOffset, endOffset - beginOffset);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : new EnumValue(enumType, getString(position));
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof String || value instanceof EnumValue);
        writeString(value instanceof String ? (String) value : ((EnumValue) value).getValue());
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
        return new EnumBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null, offsets.elements(),
            validData, this.enumValues);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        offsets.add(data.size());
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new EnumBlockBuilder(getCapacity(), initialStringLength, enumValues);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        int beginOffset = position > 0 ? offsets.getInt(position - 1) : 0;
        int endOffset = offsets.getInt(position);

        return ChunkUtil.hashCode(data.elements(), beginOffset, endOffset, true);
    }

    int beginOffset(int position) {
        return position > 0 ? offsets.getInt(position - 1) : 0;
    }

    int endOffset(int position) {
        return offsets.getInt(position);
    }
}
