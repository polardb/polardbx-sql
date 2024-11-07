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
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

public class DoubleBlockBuilder extends AbstractBlockBuilder {

    private final DoubleArrayList values;
    private final int scale;

    public DoubleBlockBuilder(int capacity) {
        super(capacity);
        this.values = new DoubleArrayList(capacity);
        this.scale = XResultUtil.DECIMAL_NOT_SPECIFIED;
    }

    public DoubleBlockBuilder(int capacity, int scale) {
        super(capacity);
        this.values = new DoubleArrayList(capacity);
        this.scale = scale;
    }

    @Override
    public void writeDouble(double value) {
        values.add(value);
        valueIsNull.add(false);
    }

    @Override
    public double getDouble(int position) {
        checkReadablePosition(position);
        return values.getDouble(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getDouble(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Double);
        writeDouble((Double) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public Block build() {
        return new DoubleBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            values.elements(), scale);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add(0L);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new DoubleBlockBuilder(getCapacity(), scale);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Double.hashCode(values.getDouble(position));
    }
}
