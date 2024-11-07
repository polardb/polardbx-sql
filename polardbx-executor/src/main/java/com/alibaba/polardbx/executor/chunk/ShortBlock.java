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
import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Short Block
 *
 * @author hongxi.chx
 */
public class ShortBlock extends AbstractBlock {
    private static final short NULL_VALUE = (short) 0;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(ShortBlock.class).instanceSize();

    private short[] values;

    public ShortBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new short[slotLen];
        updateSizeInfo();
    }

    public ShortBlock(DataType dataType, short[] values, boolean[] nulls, boolean hasNull, int length) {
        super(dataType, length, nulls, hasNull);
        this.values = values;
        updateSizeInfo();
    }

    public ShortBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, short[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = values;
        updateSizeInfo();
    }

    public ShortBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, short[] values, boolean hasNull) {
        super(DataTypes.ShortType, positionCount, valueIsNull, hasNull);
        this.values = values;
        updateSizeInfo();
    }

    public static ShortBlock from(ShortBlock other, int selSize, int[] selection) {
        return new ShortBlock(0,
            selSize,
            BlockUtils.copyNullArray(other.isNull, selection, selSize),
            BlockUtils.copyShortArray(other.values, selection, selSize));
    }

    @Override
    public short getShort(int position) {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    @Override
    public long getLong(int position) {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getShort(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeShort(getShort(position));
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putShort(NULL_VALUE);
        } else {
            sink.putShort(getShort(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Short.hashCode(values[position + arrayOffset]);
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        if (isNull(pos)) {
            return NULL_HASH_CODE;
        } else {
            return XxhashUtils.finalShuffle(values[pos + arrayOffset]);
        }
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }
        final int n = getPositionCount();
        int[] hashes = new int[n];
        for (int position = 0; position < n; position++) {
            hashes[position] = Short.hashCode(values[position + arrayOffset]);
        }
        return hashes;
    }

    @Override
    public void hashCodeVector(int[] results, int positionCount) {
        if (mayHaveNull()) {
            super.hashCodeVector(results, positionCount);
            return;
        }

        for (int position = 0; position < positionCount; position++) {
            results[position] = Short.hashCode(values[position + arrayOffset]);
        }
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
        if (other instanceof ShortBlock || other instanceof ShortBlockBuilder) {
            return getShort(position) == other.getShort(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public DataType getType() {
        return DataTypes.ShortType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof ShortBlock) {
            ShortBlock outputVectorSlot = output.cast(ShortBlock.class);
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
        if (!(another instanceof ShortBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        ShortBlock vectorSlot = another.cast(ShortBlock.class);
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        super.updateElementAt(position, element, e -> values[position] = (short) e);
    }

    public short[] shortArray() {
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
        updateSizeInfo();
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(values);
        elementUsedBytes = Byte.BYTES * positionCount + Short.BYTES * positionCount;
    }
}
