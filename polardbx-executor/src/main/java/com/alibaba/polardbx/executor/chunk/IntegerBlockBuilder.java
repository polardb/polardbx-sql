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

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.operator.util.DriverObjectPool;
import com.alibaba.polardbx.executor.operator.util.ObjectPools;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

public class IntegerBlockBuilder extends AbstractBlockBuilder {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntegerBlockBuilder.class).instanceSize();
    private static final int DEFAULT_CAPACITY = 64;

    protected final BatchedArrayList.BatchIntArrayList values;

    @FieldMemoryCounter(value = false)
    private DriverObjectPool<int[]> objectPool;
    private int chunkLimit;

    public IntegerBlockBuilder(int capacity) {
        super(capacity);
        this.values = new BatchedArrayList.BatchIntArrayList(capacity);
    }

    public IntegerBlockBuilder(int capacity, int chunkLimit, DriverObjectPool<int[]> objectPool) {
        super(capacity);
        this.values = new BatchedArrayList.BatchIntArrayList(capacity);
        this.objectPool = objectPool;
        this.chunkLimit = chunkLimit;
    }

    @Override
    public void writeInt(int value) {
        values.add(value);
        valueIsNull.add(false);
    }

    @Override
    public int getInt(int position) {
        checkReadablePosition(position);
        return values.getInt(position);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getInt(position);
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
        Block block = new IntegerBlock(0, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            values.elements());
        if (objectPool != null) {
            block.setRecycler(objectPool.getRecycler(chunkLimit));
        }
        return block;
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        values.add(0);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        if (objectPool != null) {
            return new IntegerBlockBuilder(getCapacity(), chunkLimit, objectPool);
        } else {
            return new IntegerBlockBuilder(getCapacity());
        }
    }

    @Override
    public BlockBuilder newBlockBuilder(ObjectPools objectPools, int chunkLimit) {
        return new IntegerBlockBuilder(getCapacity(), chunkLimit, objectPools.getIntArrayPool());
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return values.getInt(position);
    }


    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE + values.getMemoryUsage() + valueIsNull.getMemoryUsage();
    }

}
