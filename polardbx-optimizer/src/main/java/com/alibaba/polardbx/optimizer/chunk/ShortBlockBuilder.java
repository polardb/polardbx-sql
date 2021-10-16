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
import it.unimi.dsi.fastutil.shorts.ShortArrayList;

/**
 * Short Block Builder
 *
 * @author hongxi.chx
 */
public class ShortBlockBuilder extends AbstractBlockBuilder {

    private final ShortArrayList values;

    public ShortBlockBuilder(int capacity) {
        super(capacity);
        this.values = new ShortArrayList(capacity);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new ShortBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null, values.elements());
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Short);
        writeShort((Short) value);
    }

    @Override
    public void writeShort(short value) {
        values.add(value);
        valueIsNull.add(false);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add((short) 0);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new ShortBlockBuilder(getCapacity());
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getShort(position);
    }

    @Override
    public short getShort(int position) {
        return values.getShort(position);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return values.getShort(position);
    }

}
