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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Byte Block
 *
 */
public class ByteBlock extends AbstractBlock {
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ByteBlock.class).instanceSize();

    private byte[] values;

    public ByteBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new byte[slotLen];
    }

    ByteBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, byte[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        sizeInBytes = 2 * Byte.BYTES * positionCount;
    }

    @Override
    public byte getByte(int position) {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getByte(position);
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
        if (other instanceof ByteBlock || other instanceof ByteBlockBuilder) {
            return getByte(position) == other.getByte(otherPosition);
        } else if (other instanceof IntegerBlock || other instanceof IntegerBlockBuilder) {
            return getByte(position) == other.getInt(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeByte(getByte(position));
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putByte((byte) 0);
        } else {
            sink.putByte(getByte(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Byte.hashCode(values[position + arrayOffset]);
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }
        final int n = getPositionCount();
        int[] hashes = new int[n];
        for (int position = 0; position < n; position++) {
            hashes[position] = Byte.hashCode(values[position + arrayOffset]);
        }
        return hashes;
    }

    @Override
    public DataType getType() {
        return DataTypes.ByteType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof ByteBlock) {
            ByteBlock outputVectorSlot = (ByteBlock) output;
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
        if (!(another instanceof ByteBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        ByteBlock vectorSlot = (ByteBlock) another;
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        values[position] = DataTypes.ByteType.convertFrom(element);
    }

    public byte[] byteArray() {
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
