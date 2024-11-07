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
import com.alibaba.polardbx.rpc.result.XResultUtil;

import com.google.common.base.Preconditions;
import io.airlift.slice.XxHash64;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class DoubleBlock extends AbstractBlock {
    private static final double NULL_VALUE = 0;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DoubleBlock.class).instanceSize();

    private double[] values;
    private final int scale;

    public DoubleBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new double[slotLen];
        this.scale = null == dataType ? XResultUtil.DECIMAL_NOT_SPECIFIED : dataType.getScale();
        updateSizeInfo();
    }

    public DoubleBlock(DataType dataType, double[] values, boolean[] nulls, boolean hasNull, int length) {
        super(dataType, length, nulls, hasNull);
        this.values = values;
        this.scale = null == dataType ? XResultUtil.DECIMAL_NOT_SPECIFIED : dataType.getScale();
        updateSizeInfo();
    }

    public DoubleBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, double[] values, int scale) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        this.scale = scale;
        updateSizeInfo();
    }

    public DoubleBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, double[] values, boolean hasNull) {
        super(DataTypes.DoubleType, positionCount, valueIsNull, hasNull);
        this.values = Preconditions.checkNotNull(values);
        this.scale = XResultUtil.DECIMAL_NOT_SPECIFIED;
        updateSizeInfo();
    }

    public static DoubleBlock from(DoubleBlock other, int selSize, int[] selection) {
        return new DoubleBlock(0,
            selSize,
            BlockUtils.copyNullArray(other.isNull, selection, selSize),
            BlockUtils.copyDoubleArray(other.values, selection, selSize),
            other.scale);
    }

    @Override
    public double getDouble(int position) {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getDouble(position);
    }

    @Override
    public boolean equals(int position, Block block, int otherPosition) {
        boolean n1 = isNull(position);
        boolean n2 = block.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }
        if (block instanceof DoubleBlock || block instanceof DoubleBlockBuilder) {
            return getDouble(position) == block.getDouble(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (isNull(position)) {
            blockBuilder.appendNull();
        } else {
            blockBuilder.writeDouble(getDouble(position));
        }
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putDouble(NULL_VALUE);
        } else {
            sink.putDouble(getDouble(position));
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return Double.hashCode(values[position + arrayOffset]);
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        if (isNull(pos)) {
            return NULL_HASH_CODE;
        } else {
            return XxHash64.hash(Double.doubleToRawLongBits(values[pos + arrayOffset]));
        }
    }

    /**
     * Designed for test purpose
     */
    public static DoubleBlock of(Double... values) {
        DoubleBlockBuilder builder = new DoubleBlockBuilder(0);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                builder.appendNull();
            } else {
                builder.writeDouble(values[i]);
            }
        }
        return builder.build().cast(DoubleBlock.class);
    }

    @Override
    public int[] hashCodeVector() {
        if (mayHaveNull()) {
            return super.hashCodeVector();
        }
        final int n = getPositionCount();
        int[] hashes = new int[n];
        for (int position = 0; position < n; position++) {
            hashes[position] = Double.hashCode(values[position + arrayOffset]);
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
            results[position] = Double.hashCode(values[position + arrayOffset]);
        }
    }

    @Override
    public DataType getType() {
        return DataTypes.DoubleType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof DoubleBlock) {
            DoubleBlock outputVector = output.cast(DoubleBlock.class);
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
        if (!(another instanceof DoubleBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        DoubleBlock vectorSlot = another.cast(DoubleBlock.class);
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        super.updateElementAt(position, element, e -> values[position] = (double) e);
    }

    public double[] doubleArray() {
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
        elementUsedBytes = Byte.BYTES * positionCount + Double.BYTES * positionCount;
    }

    @Override
    public int checksumV2(int position) {
        if (isNull(position)) {
            return 0;
        }
        double roundVal;
        if (scale > 0 && scale < XResultUtil.DECIMAL_NOT_SPECIFIED) {
            BigDecimal bd = new BigDecimal(Double.toString(values[position + arrayOffset]));
            roundVal = bd.setScale(scale, RoundingMode.HALF_UP).doubleValue();
        } else {
            roundVal = values[position + arrayOffset];
        }
        return Double.hashCode(roundVal);
    }
}
