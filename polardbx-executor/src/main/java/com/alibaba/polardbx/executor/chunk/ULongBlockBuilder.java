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

import com.alibaba.polardbx.common.datatype.UInt64;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.math.BigInteger;

public class ULongBlockBuilder extends AbstractBlockBuilder {
    protected final LongArrayList values;

    public ULongBlockBuilder(int capacity) {
        super(capacity);
        this.values = new LongArrayList(capacity);
    }

    public void writeUInt64(UInt64 uint64) {
        values.add(uint64.longValue());
        valueIsNull.add(false);
    }

    public UInt64 getUInt64(int position) {
        checkReadablePosition(position);
        return UInt64.fromLong(values.getLong(position));
    }

    @Override
    public void writeBigInteger(BigInteger value) {
        writeUInt64(UInt64.fromBigInteger(value));
    }

    @Override
    public BigInteger getBigInteger(int position) {
        return getUInt64(position).toBigInteger();
    }

    @Override
    public void writeLong(long value) {
        values.add(value);
        valueIsNull.add(false);
    }

    @Override
    public long getLong(int position) {
        checkReadablePosition(position);
        return values.getLong(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getUInt64(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        if (value instanceof Long) {
            writeLong((Long) value);
            return;
        }
        Preconditions.checkArgument(value instanceof UInt64);
        writeUInt64((UInt64) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new ULongBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null, values.elements());
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add(0L);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new ULongBlockBuilder(getCapacity());
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Long.hashCode(values.getLong(position));
    }

}
