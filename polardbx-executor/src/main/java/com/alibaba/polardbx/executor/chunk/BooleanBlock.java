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
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Boolean Block
 */
public class BooleanBlock extends AbstractBlock {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(BooleanBlock.class).instanceSize();

    private boolean[] values;

    public BooleanBlock(DataType<?> dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new boolean[slotLen];
        updateSizeInfo();
    }

    public BooleanBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, boolean[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        updateSizeInfo();
    }

    public BooleanBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, boolean[] values, boolean hasNull) {
        super(DataTypes.BooleanType, positionCount, valueIsNull, hasNull);
        this.values = Preconditions.checkNotNull(values);
        updateSizeInfo();
    }

    @Override
    public boolean getBoolean(int position) {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getBoolean(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeBoolean(getBoolean(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Boolean.hashCode(values[position + arrayOffset]);
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }
        final int n = getPositionCount();
        int[] hashes = new int[n];
        for (int position = 0; position < n; position++) {
            hashes[position] = Boolean.hashCode(values[position + arrayOffset]);
        }
        return hashes;
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
        if (other instanceof BooleanBlock || other instanceof BooleanBlockBuilder) {
            return getBoolean(position) == other.getBoolean(position);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof BooleanBlock) {
            BooleanBlock outputVector = (BooleanBlock) output;
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    outputVector.values[j] = values[j];
                }
            } else {
                System.arraycopy(values, 0, outputVector.values, 0, size);
            }
        } else {
            BlockUtils.copySelectedInCommon(selectedInUse, sel, size, this, output);
        }

        super.copySelected(selectedInUse, sel, size, output);
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        if (!(another instanceof BooleanBlock)) {
            GeneralUtil.nestedException("Cannot shallow copy to " + another);
        }
        BooleanBlock vectorSlot = (BooleanBlock) another;
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        super.updateElementAt(position, element, e -> values[position] = BooleanType.isTrue((int) e));
    }

    public boolean[] booleanArray() {
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
        elementUsedBytes = Byte.BYTES * positionCount + Byte.BYTES * positionCount;
    }
}
