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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * Long Block
 */
public class LongBlock extends AbstractBlock {
    public static final long NULL_VALUE = 0L;
    public static final long TRUE_VALUE = 1;
    public static final long FALSE_VALUE = 0;

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(LongBlock.class).instanceSize();

    private long[] values;

    public LongBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.values = new long[slotLen];
        updateSizeInfo();
    }

    public LongBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] values) {
        super(arrayOffset, positionCount, valueIsNull);
        this.values = Preconditions.checkNotNull(values);
        updateSizeInfo();
    }

    @Override
    public long getLong(int position) {
        checkReadablePosition(position);
        return values[position + arrayOffset];
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getLong(position);
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
        if (other instanceof LongBlock || other instanceof LongBlockBuilder) {
            return getLong(position) == other.getLong(otherPosition);
        } else if (other instanceof IntegerBlock || other instanceof IntegerBlockBuilder) {
            return getLong(position) == other.getInt(otherPosition);
        } else {
            throw new AssertionError();
        }
    }

    @VisibleForTesting
    public static LongBlock wrap(long[] values) {
        return new LongBlock(0, values.length, null, values);
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
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putLong(NULL_VALUE);
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

    /**
     * Designed for test purpose
     */
    public static LongBlock of(Long... values) {
        final int len = values.length;
        boolean[] valueIsNull = new boolean[len];
        long[] longValues = new long[len];
        for (int i = 0; i < len; i++) {
            if (values[i] != null) {
                longValues[i] = values[i];
            } else {
                valueIsNull[i] = true;
            }
        }
        return new LongBlock(0, len, valueIsNull, longValues);
    }

    @Override
    public DataType getType() {
        return DataTypes.LongType;
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof LongBlock) {
            LongBlock outputVectorSlot = (LongBlock) output;
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
        if (!(another instanceof LongBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        LongBlock vectorSlot = (LongBlock) another;
        super.shallowCopyTo(vectorSlot);
        vectorSlot.values = values;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        return values[position];
    }

    @Override
    public void setElementAt(int position, Object element) {
        super.updateElementAt(position, element, e -> values[position] = (long) e);
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
        updateSizeInfo();
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(values);
        elementUsedBytes = Byte.BYTES * positionCount + Long.BYTES * positionCount;
    }

    @Override
    public void copyPositions_scatter_simd(ScatterMemoryContext scatterMemoryContext, BlockBuilder[] blockBuilders) {
        long[] longScatterBufferDataBuffer = scatterMemoryContext.getLongScatterBufferDataBuffer();
        long[] longBufferDataBuffer = scatterMemoryContext.getLongBufferDataBuffer();
        int[] destScatterMapBuffer = scatterMemoryContext.getDestScatterMapBuffer();
        int batchSize = scatterMemoryContext.getBatchSize();
        int srcStartPosition = scatterMemoryContext.getSrcStartPosition();
        int[] destLengthsPerPartitionInBatch = scatterMemoryContext.getDestLengthsPerPartitionInBatch();
        readBatchLongs(srcStartPosition, longBufferDataBuffer, 0, batchSize);
        VectorizedPrimitives.SIMD_PRIMITIVES_HANDLER.scatter(longBufferDataBuffer, longScatterBufferDataBuffer,
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
            LongBlockBuilder longBlockBuilder = (LongBlockBuilder) blockBuilders[partition];
            longBlockBuilder.writeBatchLongs(longScatterBufferDataBuffer, scatterBufferNullsBuffer, start,
                destLengthsPerPartitionInBatch[partition]);
            start += destLengthsPerPartitionInBatch[partition];
        }
    }

    public void readBatchLongs(int basePosition, long[] target, int targetBase, int count) {
        //将values数组从basePosition开始的count个元素copy到target数组从targetBase开始的位置
        System.arraycopy(values, basePosition, target, targetBase, count);
    }

    public void readBatchNulls(int basePosition, boolean[] target, int targetBase, int count) {
        System.arraycopy(isNull, basePosition, target, targetBase, count);
    }
}
