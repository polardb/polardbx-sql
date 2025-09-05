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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.optimizer.core.datatype.Blob;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

public abstract class AbstractBlockBuilder implements BlockBuilder {

    final int initialCapacity;
    final protected BatchedArrayList.BatchBooleanArrayList valueIsNull;
    protected boolean containsNull;

    public AbstractBlockBuilder(int initialCapacity) {
        this.initialCapacity = initialCapacity;
        this.valueIsNull = new BatchedArrayList.BatchBooleanArrayList(initialCapacity);
        this.containsNull = false;
    }

    static class MemoryCountableByteArrayList extends ByteArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableByteArrayList.class).instanceSize();

        public MemoryCountableByteArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableFloatArrayList extends FloatArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableFloatArrayList.class).instanceSize();

        public MemoryCountableFloatArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableShortArrayList extends ShortArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableShortArrayList.class).instanceSize();

        public MemoryCountableShortArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableCharArrayList extends CharArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableCharArrayList.class).instanceSize();

        public MemoryCountableCharArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableLongArrayList extends LongArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableLongArrayList.class).instanceSize();

        public MemoryCountableLongArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableIntArrayList extends IntArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableIntArrayList.class).instanceSize();

        public MemoryCountableIntArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableDoubleArrayList extends DoubleArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableDoubleArrayList.class).instanceSize();

        public MemoryCountableDoubleArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableBooleanArrayList extends BooleanArrayList implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableBooleanArrayList.class).instanceSize();

        public MemoryCountableBooleanArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));
        }
    }

    static class MemoryCountableReferenceArrayList<T> extends ReferenceArrayList<T> implements MemoryCountable {
        private static final int INSTANCE_SIZE =
            ClassLayout.parseClass(MemoryCountableReferenceArrayList.class).instanceSize();
        private static final int BLOB_INSTANCE_SIZE = ClassLayout.parseClass(Blob.class).instanceSize();

        public MemoryCountableReferenceArrayList(int capacity) {
            super(capacity);
        }

        @Override
        public long getMemoryUsage() {
            long memoryUsage = INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOf(a));

            for (int i = 0; i < size; i++) {
                if (a[i] instanceof Blob) {
                    memoryUsage += FastMemoryCounter.sizeOf((java.sql.Blob) a[i], BLOB_INSTANCE_SIZE);
                }
            }

            return memoryUsage;
        }
    }

    @Override
    public int getPositionCount() {
        return valueIsNull.size();
    }

    @Override
    public boolean isNull(int position) {
        checkReadablePosition(position);
        return valueIsNull.getBoolean(position);
    }

    @Override
    public void ensureCapacity(int capacity) {
        valueIsNull.ensureCapacity(capacity);
    }

    protected void appendNullInternal() {
        valueIsNull.add(true);
        containsNull = true;
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    void setContainsNull() {
        this.containsNull = true;
    }

    @Override
    public boolean mayHaveNull() {
        return containsNull;
    }

    protected void checkReadablePosition(int position) {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public final boolean equals(int position, Block otherBlock, int otherPosition) {
        throw new UnsupportedOperationException("Please invoke from block instead of block builder");
    }

    @Override
    public final void writePositionTo(int position, BlockBuilder blockBuilder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final long estimateSize() {
        throw new UnsupportedOperationException();
    }

    protected int getCapacity() {
        return valueIsNull.elements().length;
    }

    @Override
    public final long hashCodeUseXxhash(int pos) {
        throw new UnsupportedOperationException(
            "Block builder not support hash code calculated by xxhash, you should convert it to block first");
    }
}
