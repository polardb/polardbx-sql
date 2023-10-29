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
import com.alibaba.polardbx.executor.mpp.operator.SIMD.VectorizedPrimitives;
import com.alibaba.polardbx.executor.mpp.operator.ScatterMemoryContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Integer Block
 */
public class IntegerBlock extends AbstractBlock {
    private static final int NULL_VALUE = 0;
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(IntegerBlock.class).instanceSize();

    private int[] values;
    private int[] selection;

    public IntegerBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new int[slotLen];
        updateSizeInfo();
    }

    public IntegerBlock(DataType dataType, int[] values, boolean[] nulls, boolean hasNull, int length,
                        int[] selection) {
        super(dataType, length, nulls, hasNull);
        this.values = values;
        this.selection = selection;
        updateSizeInfo();
    }

    IntegerBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        updateSizeInfo();
    }

    IntegerBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, int[] values, boolean hasNull) {
        super(DataTypes.IntegerType, positionCount, valueIsNull, hasNull);
        this.values = Preconditions.checkNotNull(values);
        updateSizeInfo();
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
        updateSizeInfo();
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(values);
        elementUsedBytes = Byte.BYTES * positionCount + Integer.BYTES * positionCount;
    }

    public int[] getSelection() {
        return selection;
    }

    private void checkNoDelayMaterialization() {
        if (selection != null) {
            throw new AssertionError("un-support delay materialization in this method");
        }
    }

    @Override
    public void copyPositions_scatter_simd(ScatterMemoryContext scatterMemoryContext, BlockBuilder[] blockBuilders) {
        //存储数据的buffer
        int[] intScatterBufferDataBuffer = scatterMemoryContext.getIntScatterBufferDataBuffer();
        int[] intBufferDataBuffer = scatterMemoryContext.getIntBufferDataBuffer();
        int[] destScatterMapBuffer = scatterMemoryContext.getDestScatterMapBuffer(); //目的Scatter Buffer
        int batchSize = scatterMemoryContext.getBatchSize(); //Batch Size
        int srcStartPosition = scatterMemoryContext.getSrcStartPosition(); //开始的position
        int[] destLengthsPerPartitionInBatch =
            scatterMemoryContext.getDestLengthsPerPartitionInBatch(); //每一个partition中每一个batch对应的length
        //memory copy
        readBatchInts(srcStartPosition, intBufferDataBuffer, 0, batchSize);
        VectorizedPrimitives.SIMD_PRIMITIVES_HANDLER.scatter(intBufferDataBuffer, intScatterBufferDataBuffer,
            destScatterMapBuffer, 0, batchSize);
        boolean[] scatterBufferNullsBuffer = scatterMemoryContext.getScatterBufferNullsBuffer();
        boolean[] bufferNullsBuffer = scatterMemoryContext.getBufferNullsBuffer();
        if (hasNull()) {
            readBatchNulls(srcStartPosition, bufferNullsBuffer, 0, batchSize);
            VectorizedPrimitives.SIMD_PRIMITIVES_HANDLER.scatter(bufferNullsBuffer, scatterBufferNullsBuffer,
                destScatterMapBuffer, 0, batchSize);
        }
        int start = 0;
        for (int partition = 0; partition < blockBuilders.length; partition++) {
            if (destLengthsPerPartitionInBatch[partition] == 0) {
                continue;
            }
            //构建出BlockBuilder
            IntegerBlockBuilder integerBlockBuilder = (IntegerBlockBuilder) blockBuilders[partition];
            integerBlockBuilder.writeBatchInts(intScatterBufferDataBuffer, scatterBufferNullsBuffer, start,
                destLengthsPerPartitionInBatch[partition]);
            start += destLengthsPerPartitionInBatch[partition];
        }
    }

    public void readBatchInts(int basePosition, int[] target, int targetBase, int count) {
        //将values数组从basePosition开始的count个元素copy到target数组从targetBase开始的位置
        System.arraycopy(values, basePosition, target, targetBase, count);
    }

    public void readBatchNulls(int basePosition, boolean[] target, int targetBase, int count) {
        System.arraycopy(isNull, basePosition, target, targetBase, count);
    }
}
