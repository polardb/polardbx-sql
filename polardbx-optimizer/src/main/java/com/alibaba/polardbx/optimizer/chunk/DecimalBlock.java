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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.RawBytesDecimalUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.util.bloomfilter.TddlHasher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;

/**
 * An implement of Decimal block mixed with fixed and variable length
 *
 */
public class DecimalBlock extends AbstractBlock {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DecimalBlock.class).instanceSize();

    protected Slice memorySegments;

    /**
     * Allocate the memory of decimal vector
     */
    public DecimalBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.memorySegments = Slices.allocate(slotLen * DECIMAL_MEMORY_SIZE);

        estimatedSize = INSTANCE_SIZE + memorySegments.length();
        sizeInBytes = (DECIMAL_MEMORY_SIZE + Byte.BYTES) * positionCount;
    }

    public DecimalBlock(DataType dataType, Slice memorySegments, boolean[] nulls, boolean hasNull, int length) {
        super(dataType, length, nulls, hasNull);
        this.memorySegments = memorySegments;

        estimatedSize = INSTANCE_SIZE + memorySegments.length();
        sizeInBytes = (DECIMAL_MEMORY_SIZE + Byte.BYTES) * positionCount;
    }

    public DecimalBlock(int arrayOffset, int positionCount, boolean[] valueIsNull,
                        Slice memorySegments) {
        super(arrayOffset, positionCount, valueIsNull);
        this.memorySegments = memorySegments;

        estimatedSize = INSTANCE_SIZE + memorySegments.length();
        sizeInBytes = (Long.BYTES + Integer.BYTES + Integer.BYTES + Byte.BYTES + Byte.BYTES) * positionCount;
    }

    public DecimalBlock(int arrayOffset, int positionCount, boolean[] valueIsNull,
                        Slice memorySegments, boolean hasNull) {
        super(DataTypes.DecimalType, positionCount, valueIsNull, hasNull);
        this.memorySegments = memorySegments;

        estimatedSize = INSTANCE_SIZE + memorySegments.length();
        sizeInBytes = (Long.BYTES + Integer.BYTES + Integer.BYTES + Byte.BYTES + Byte.BYTES) * positionCount;
    }

    @Override
    public Decimal getDecimal(int position) {
        checkReadablePosition(position);
        Slice memorySegment = memorySegments.slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
        return new Decimal(memorySegment);
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getDecimal(position);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        if (blockBuilder instanceof DecimalBlockBuilder) {
            writePositionTo(position, (DecimalBlockBuilder) blockBuilder);
        } else {
            throw new AssertionError();
        }
    }

    private void writePositionTo(int position, DecimalBlockBuilder b) {
        if (isNull(position)) {
            b.appendNull();
        } else {
            // write to decimal memory segments
            b.sliceOutput.writeBytes(memorySegments, position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
            b.valueIsNull.add(false);
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        Slice memorySegment = memorySegments.slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
        return RawBytesDecimalUtils.hashCode(memorySegment);
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

        if (other instanceof DecimalBlock) {
            // for decimal block, compare by memory segment
            Slice memorySegment1 = this.segmentUncheckedAt(position);
            Slice memorySegment2 = ((DecimalBlock)other).segmentUncheckedAt(otherPosition);
            return RawBytesDecimalUtils.equals(memorySegment1, memorySegment2);
        } else if (other instanceof DecimalBlockBuilder) {
            // for decimal block, compare by memory segment
            Slice memorySegment1 = this.segmentUncheckedAt(position);
            Slice memorySegment2 = ((DecimalBlockBuilder)other).segmentUncheckedAt(otherPosition);
            return RawBytesDecimalUtils.equals(memorySegment1, memorySegment2);
        } else {
            throw new AssertionError();
        }
    }

    Slice segmentUncheckedAt(int position) {
        return memorySegments.slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
    }

    @Override
    public void addToBloomFilter(TddlHasher sink, int position) {
        if (isNull(position)) {
            sink.putInt(0);
        } else {
            sink.putInt(hashCode(position));
        }
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof DecimalBlock) {
            DecimalBlock outputVectorSlot = (DecimalBlock) output;
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];

                    // copy memory segment from specified position in selection array.
                    int fromIndex = j * DECIMAL_MEMORY_SIZE;
                    outputVectorSlot.memorySegments.setBytes(fromIndex, memorySegments, fromIndex, DECIMAL_MEMORY_SIZE);
                }
            } else {
                // directly copy memory.
                outputVectorSlot.memorySegments.setBytes(0, memorySegments);
            }
        } else {
            BlockUtils.copySelectedInCommon(selectedInUse, sel, size, this, output);
        }

        super.copySelected(selectedInUse, sel, size, output);
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        if (!(another instanceof DecimalBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        DecimalBlock vectorSlot = (DecimalBlock) another;
        super.shallowCopyTo(vectorSlot);
        vectorSlot.memorySegments = memorySegments;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        // slice a memory segment in 64 bytes and build a decimal value.
        int fromIndex = position * DECIMAL_MEMORY_SIZE;
        Slice decimalMemorySegment = memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE);
        return new Decimal(decimalMemorySegment);
    }

    @Override
    public void setElementAt(int position, Object element) {
        super.updateElementAt(position, element, e -> {
            Decimal decimal = (Decimal) e;
            Slice decimalMemorySegment = decimal.getMemorySegment();

            // copy memory from specified position in size of 64 bytes
            int fromIndex = position * DECIMAL_MEMORY_SIZE;
            memorySegments.setBytes(fromIndex, decimalMemorySegment);
        });
    }

    public Slice getMemorySegments() {
        return memorySegments;
    }

    @Override
    public void compact(int[] selection) {
        if (selection == null) {
            return;
        }
        int compactedSize = selection.length;
        int index = 0;
        for (int i = 0; i < compactedSize; i++) {
            int j = selection[i];

            // copy memory segment from specified position in selection array.
            int sourceIndex = j * DECIMAL_MEMORY_SIZE;
            this.memorySegments.setBytes(index, memorySegments, sourceIndex, DECIMAL_MEMORY_SIZE);
            index += DECIMAL_MEMORY_SIZE;

            isNull[i] = isNull[j];
        }
        this.positionCount = compactedSize;

        // re-compute the size
        estimatedSize = INSTANCE_SIZE + memorySegments.length();
        sizeInBytes = (Long.BYTES + Integer.BYTES + Integer.BYTES + Byte.BYTES + Byte.BYTES) * positionCount;
    }
}