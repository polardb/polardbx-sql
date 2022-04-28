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
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;

/**
 * Boolean Block Builder
 *
 */
public class BooleanBlockBuilder extends AbstractBlockBuilder {

    private final BooleanArrayList values;

    public BooleanBlockBuilder(int capacity) {
        super(capacity);
        this.values = new BooleanArrayList(capacity);
    }

    @Override
    public void writeBoolean(boolean value) {
        valueIsNull.add(false);
        values.add(value);
    }

    @Override
    public boolean getBoolean(int position) {
        checkReadablePosition(position);
        return values.getBoolean(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getBoolean(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Boolean);
        writeBoolean((Boolean) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new BooleanBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            values.elements());
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add(false);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new BooleanBlockBuilder(getCapacity());
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Boolean.hashCode(getBoolean(position));
    }
}
