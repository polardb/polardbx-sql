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

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.util.bloomfilter.TddlHasher;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigInteger;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class ULongBlock extends AbstractBlock {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ULongBlock.class).instanceSize();

    private long[] values;

    public ULongBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new long[slotLen];
    }

    public ULongBlock(DataType dataType, long[] values, boolean[] nulls, boolean hasNull, int length) {
        super(dataType, length, nulls, hasNull);
        this.values = values;
    }

    public ULongBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        sizeInBytes = (Long.BYTES + Byte.BYTES) * positionCount;
    }

    public ULongBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] values, boolean hasNull) {
        super(DataTypes.ULongType, positionCount, valueIsNull, hasNull);
        this.values = Preconditions.checkNotNull(values);
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        sizeInBytes = (Long.BYTES + Byte.BYTES) * positionCount;
    }

    @Override
    public BigInteger getBigInteger(int position) {
        return getUInt64(position).toBigInteger();
    }

    @Override
    public long getLong(int position) {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getUInt64(position);
    }

    public UInt64 getUInt64(int position) {
        return UInt64.fromLong(getLong(position));
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        if (other instanceof ULongBlock || other instanceof ULongBlockBuilder) {
            return getLong(position) == other.getLong(otherPosition);
        } else if (other instanceof IntegerBlock || other instanceof IntegerBlockBuilder) {
            return getLong(position) == other.getInt(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeLong(getLong(position));
        }
    }

    @Override
    public void addToBloomFilter(TddlHasher sink, int position) {
        if (isNull(position)) {
            sink.putLong(0L);
        } else {
            sink.putLong(getLong(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Long.hashCode(values[position + arrayOffset]);
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }
        final int n = getPositionCount();
        int[] hashes = new int[n];
        for (int position = 0; position < n; position++) {
            hashes[position] = Long.hashCode(values[position + arrayOffset]);
        }
        return hashes;
    }

    @Override
    public DataType getType() {
        return DataTypes.ULongType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof ULongBlock) {
            ULongBlock outputVectorSlot = (ULongBlock) output;
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    outputVectorSlot.values[j] = values[j];
                }
            } else {
                System.arraycopy(values, 0, outputVectorSlot.values, 0, size);
            }
        } else {
            BlockUtils.copySelectedInCommon(selectedInUse, sel, size, this, output);
        }

        super.copySelected(selectedInUse, sel, size, output);
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        if (!(another instanceof ULongBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        ULongBlock vectorSlot = (ULongBlock) another;
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return UInt64.fromLong(values[position]);
    }

    @Override
    public void setElementAt(int position, Object element) {
        super.updateElementAt(position, element, e -> values[position] = ((Number) e).longValue());
    }

    public long[] longArray() {
        return values;
    }

    @Override
    public void compact(int[] selection) {
        if (selection == null) {
            return;
        }
        int compactedSize = selection.length;
        for (int i = 0; i < compactedSize; i++) {
            int j = selection[i];
            values[i] = values[j];
            isNull[i] = isNull[j];
        }
        this.positionCount = compactedSize;

        // re-compute the size
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(values);
        sizeInBytes = (Long.BYTES + Byte.BYTES) * positionCount;
    }
}