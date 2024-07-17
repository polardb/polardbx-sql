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

import java.math.BigInteger;
import java.util.Arrays;

/**
 * A fixed-length implement of BigInteger block builder
 *
 */
public class BigIntegerBlockBuilder extends AbstractBlockBuilder {

    final ByteArrayList data;

    private static final byte[] EMPTY_PACKET = new byte[BigIntegerBlock.LENGTH];

    public BigIntegerBlockBuilder(int capacity) {
        super(capacity);
        this.data = new ByteArrayList(capacity * BigIntegerBlock.LENGTH);
    }

    @Override
    public void writeBigInteger(BigInteger value) {
        final byte[] bytes = value.toByteArray();

        if (bytes.length > BigIntegerBlock.UNSCALED_LENGTH) {
            throw new AssertionError("decimal with unexpected digits number");
        }

        data.addElements(data.size(), bytes);
        for (int i = bytes.length; i < BigIntegerBlock.UNSCALED_LENGTH; i++) {
            data.add((byte) 0);
        }
        data.add((byte) bytes.length);

        valueIsNull.add(false);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        data.addElements(data.size(), EMPTY_PACKET);
    }

    @Override
    public BigInteger getBigInteger(int position) {
        checkReadablePosition(position);
        int beginOffset = position * BigIntegerBlock.LENGTH;
        int endOffset = beginOffset + BigIntegerBlock.LENGTH;
        final int length = data.getByte(endOffset - 1);
        final byte[] bytes = Arrays.copyOfRange(data.elements(), beginOffset, beginOffset + length);
        return new BigInteger(bytes);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getBigInteger(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof BigInteger);
        writeBigInteger((BigInteger) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        data.ensureCapacity(capacity * BigIntegerBlock.LENGTH);
    }

    @Override
    public Block build() {
        return new BigIntegerBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            data.elements());
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new BigIntegerBlockBuilder(getCapacity());
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }

        return getBigInteger(position).hashCode();
    }
}
