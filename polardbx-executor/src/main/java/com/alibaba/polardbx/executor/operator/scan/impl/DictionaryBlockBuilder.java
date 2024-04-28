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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.chunk.AbstractBlockBuilder;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.optimizer.core.datatype.BlobType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Block builder for slice block based on dictionary.
 */
public class DictionaryBlockBuilder extends AbstractBlockBuilder {
    private BlockDictionary blockDictionary;
    private IntArrayList values;
    private DataType dataType;
    private boolean isCompatible;

    public DictionaryBlockBuilder(boolean isCompatible, DataType dataType, int initialCapacity) {
        super(initialCapacity);
        Preconditions.checkArgument(dataType instanceof SliceType || dataType instanceof BlobType);
        this.dataType = dataType;
        this.isCompatible = isCompatible;
        this.values = new IntArrayList(initialCapacity);
    }

    public void setDictionary(BlockDictionary dictionary) {
        this.blockDictionary = dictionary;
    }

    @Override
    public void writeInt(int value) {
        values.add(value);
        valueIsNull.add(false);
    }

    public Slice getRegion(int position) {
        if (valueIsNull.get(position)) {
            return Slices.EMPTY_SLICE;
        } else {
            int dictId = values.getInt(position);
            if (dictId == -1) {
                return Slices.EMPTY_SLICE;
            }
            return blockDictionary.getValue(dictId);
        }
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getRegion(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Integer);
        writeInt((Integer) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        if (dataType instanceof SliceType) {
            return new SliceBlock((SliceType) dataType, 0, getPositionCount(),
                mayHaveNull() ? valueIsNull.elements() : null,
                blockDictionary, values.elements(), isCompatible
            );
        }
        throw new UnsupportedOperationException("Unsupported dictionary type: " + dataType);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add(-1);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new DictionaryBlockBuilder(isCompatible, dataType, getCapacity());
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        int dictId = values.getInt(position);
        if (dictId == -1) {
            return 0;
        }
        return blockDictionary.getValue(dictId).hashCode();
    }

}
