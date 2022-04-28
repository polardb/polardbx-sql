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

import java.sql.Clob;

/**
 * Clob Block Builder
 *
 */
public class ClobBlockBuilder extends ReferenceBlockBuilder<Clob> {

    public ClobBlockBuilder(int capacity) {
        super(capacity);
    }

    @Override
    public Clob getClob(int position) {
        return getReference(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getClob(position);
    }

    @Override
    public Block build() {
        return new ClobBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null, values.elements());
    }

    @Override
    public void writeClob(Clob value) {
        writeReference(value);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        Preconditions.checkArgument(value instanceof Clob);
        writeClob((Clob) value);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new ClobBlockBuilder(getCapacity());
    }
}
