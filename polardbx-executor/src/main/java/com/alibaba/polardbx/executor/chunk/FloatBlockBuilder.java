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

import com.alibaba.polardbx.rpc.result.XResultUtil;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.floats.FloatArrayList;

public class FloatBlockBuilder extends AbstractBlockBuilder {

    private final FloatArrayList values;

    private final int scale;

    public FloatBlockBuilder(int capacity) {
        super(capacity);
        this.values = new FloatArrayList(capacity);
        this.scale = XResultUtil.DECIMAL_NOT_SPECIFIED;
    }

    public FloatBlockBuilder(int capacity, int scale) {
        super(capacity);
        this.values = new FloatArrayList(capacity);
        this.scale = scale;
    }

    @Override
    public void writeFloat(float value) {
        values.add(value);
        valueIsNull.add(false);
    }

    @Override
    public float getFloat(int position) {
        checkReadablePosition(position);
        return values.getFloat(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getFloat(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Float);
        writeFloat((Float) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new FloatBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            values.elements(), scale);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add(0f);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new FloatBlockBuilder(getCapacity(), scale);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Float.hashCode(values.getFloat(position));
    }
}
