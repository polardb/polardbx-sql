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

import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.util.Objects;

public abstract class ReferenceBlockBuilder<T> extends AbstractBlockBuilder {

    public final ReferenceArrayList<T> values;

    public ReferenceBlockBuilder(int capacity) {
        super(capacity);
        this.values = new ReferenceArrayList<>(capacity);
    }

    public void writeReference(T value) {
        values.add(value);
        valueIsNull.add(false);
    }

    public T getReference(int position) {
        checkReadablePosition(position);
        return values.get(position);
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add(null);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        values.ensureCapacity(capacity);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Objects.hashCode(values.get(position));
    }
}
