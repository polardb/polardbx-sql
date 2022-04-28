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
 * Integer Block
 *
 */
public class IntegerBlock extends AbstractBlock {
    private static final int NULL_VALUE = 0;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(IntegerBlock.class).instanceSize();

    private int[] values;
    private int[] selection;

    public IntegerBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new int[slotLen];
        estimatedSize = INSTANCE_SIZE + Byte.BYTES * positionCount + sizeOf(values);
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount;
    }

    public IntegerBlock(DataType dataType, int[] values, boolean[] nulls, boolean hasNull, int length, int[] selection) {
        super(dataType, length, nulls, hasNull);
        this.values = values;
        this.selection = selection;
        estimatedSize = INSTANCE_SIZE + Byte.BYTES * positionCount + sizeOf(values);
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount;
    }

    IntegerBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount;
    }

    IntegerBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] values, boolean hasNull) {
        super(DataTypes.IntegerType, positionCount, valueIsNull, hasNull);
        this.values = Preconditions.checkNotNull(values);
        estimatedSize = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        sizeInBytes = (Integer.BYTES + Byte.BYTES) * positionCount;
    }

    private int realPositionOf(int position) {
        if (selection == null) {
            return position;
        }
        return selection[position];
    }

    @Override
    public boolean isNull(int position) {
        position = realPositionOf(position);
        return isNull != null && isNull[position + arrayOffset];
    }

    @Override
    public int getInt(int position) {
        position = realPositionOf(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getInt(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeInt(getInt(position));
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putInt(NULL_VALUE);
        } else {
            sink.putInt(getInt(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return getInt(position);
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
        if (other instanceof IntegerBlock || other instanceof IntegerBlockBuilder) {
            return getInt(position) == other.getInt(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    /**
     * Designed for test purpose
     */
    public static IntegerBlock of(Integer... values) {
        final int len = values.length;
        boolean[] valueIsNull = new boolean[len];
        int[] intValues = new int[len];
        for (int i = 0; i < len; i++) {
            if (values[i] != null) {
                intValues[i] = values[i];
            } else {
                valueIsNull[i] = true;
            }
        }
        return new IntegerBlock(0, len, valueIsNull, intValues);
    }

    public static IntegerBlock wrap(int[] values) {
        return new IntegerBlock(0, values.length, null, values);
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }
        if (selection != null) {
            return super.hashCodeVector();
        }
        final int n = getPositionCount();
        int[] hashes = new int[n];
        System.arraycopy(values, arrayOffset, hashes, 0, n);
        return hashes;
    }

    @Override
    public DataType getType() {
        return DataTypes.IntegerType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        checkNoDelayMaterialization();
        if (output instanceof IntegerBlock) {
            IntegerBlock outputVectorSlot = (IntegerBlock) output;
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
        checkNoDelayMaterialization();
        if (!(another instanceof IntegerBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        IntegerBlock vectorSlot = (IntegerBlock) another;
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        checkNoDelayMaterialization();
        super.updateElementAt(position, element, e -> values[position] = (int) e);
    }

    public int[] intArray() {
        return values;
    }

    @Override
    public void compact(int[] selection) {
        checkNoDelayMaterialization();
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

    public int[] getSelection() {
        return selection;
    }

    private void checkNoDelayMaterialization() {
        if (selection != null) {
            throw new AssertionError("un-support delay materialization in this method");
        }
    }
}
