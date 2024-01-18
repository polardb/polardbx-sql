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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.RawBytesDecimalUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DERIVED_FRACTIONS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.FRACTIONS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.INTEGERS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.IS_NEG_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.roundUp;
import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

/**
 * An implement of Decimal block mixed with fixed and variable length
 */
public class DecimalBlock extends AbstractBlock {
    private static final int NULL_VALUE = 0;

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DecimalBlock.class).instanceSize();

    private static final int UNSET = -1;

    /**
     * Allocate the memory of decimal vector
     */
    protected Slice memorySegments;
    protected int[] selection;

    /**
     * A Decimal Block is simple only if all non-null decimal values are in format of
     * (a2 * 10^(9*-1) + a1 * 10^(9*0) + b * 10^(9*-1)).
     * In other word, the int word and frac word is 0 or 1.
     */
    private DecimalBlockState state;

    /**
     * For Vectorized expression result vector.
     */
    public DecimalBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        this.memorySegments = Slices.allocate(slotLen * DECIMAL_MEMORY_SIZE);
        this.selection = null;

        this.state = DecimalBlockState.UNSET_STATE;
        updateSizeInfo();
    }

    /**
     * For Delay Materialization.
     */
    public DecimalBlock(DataType dataType, Slice memorySegments, boolean[] nulls, boolean hasNull, int length,
                        int[] selection, DecimalBlockState state) {
        super(dataType, length, nulls, hasNull);
        this.memorySegments = memorySegments;
        this.selection = selection;

        this.state = state;
        updateSizeInfo();
    }

    /**
     * Normal
     */
    public DecimalBlock(int positionCount, boolean[] valueIsNull,
                        Slice memorySegments, DecimalBlockState state) {
        super(0, positionCount, valueIsNull);
        this.memorySegments = memorySegments;
        this.selection = null;

        this.state = state;
        updateSizeInfo();
    }

    public int realPositionOf(int position) {
        return selection == null ? position : selection[position];
    }

    @Override
    public boolean isNull(int position) {
        position = realPositionOf(position);
        return isNull != null && isNull[position + arrayOffset];
    }

    @Override
    public Decimal getDecimal(int position) {
        position = realPositionOf(position);
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
            position = realPositionOf(position);
            // write to decimal memory segments
            b.sliceOutput.writeBytes(memorySegments, position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
            b.valueIsNull.add(false);

            // update decimal info
            DecimalBlockState elementState = DecimalBlockState.stateOf(memorySegments, position);
            b.state = b.state.merge(elementState);
        }
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        position = realPositionOf(position);
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
            Slice memorySegment2 = ((DecimalBlock) other).segmentUncheckedAt(otherPosition);
            return RawBytesDecimalUtils.equals(memorySegment1, memorySegment2);
        } else if (other instanceof DecimalBlockBuilder) {
            // for decimal block, compare by memory segment
            Slice memorySegment1 = this.segmentUncheckedAt(position);
            Slice memorySegment2 = ((DecimalBlockBuilder) other).segmentUncheckedAt(otherPosition);
            return RawBytesDecimalUtils.equals(memorySegment1, memorySegment2);
        } else {
            throw new AssertionError();
        }
    }

    Slice segmentUncheckedAt(int position) {
        position = realPositionOf(position);
        return memorySegments.slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        if (isNull(position)) {
            sink.putInt(NULL_VALUE);
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
        position = realPositionOf(position);
        // slice a memory segment in 64 bytes and build a decimal value.
        int fromIndex = position * DECIMAL_MEMORY_SIZE;
        Slice decimalMemorySegment = memorySegments.slice(fromIndex, DECIMAL_MEMORY_SIZE);
        return new Decimal(decimalMemorySegment);
    }

    @Override
    public void setElementAt(int position, Object element) {
        final int realPos = realPositionOf(position);
        super.updateElementAt(position, element, e -> {
            Decimal decimal = (Decimal) e;
            Slice decimalMemorySegment = decimal.getMemorySegment();

            // copy memory from specified position in size of 64 bytes
            int fromIndex = realPos * DECIMAL_MEMORY_SIZE;
            memorySegments.setBytes(fromIndex, decimalMemorySegment);
        });
    }

    public void encoding(SliceOutput sliceOutput) {
        sliceOutput.writeInt(positionCount * DECIMAL_MEMORY_SIZE);
        if (selection != null) {
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i];
                sliceOutput.appendBytes(this.memorySegments.slice(j * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE));
            }

        } else {
            sliceOutput.appendBytes(this.memorySegments);
        }
    }

    /**
     * Just allowed to be used in vectorized expression
     */
    @Deprecated
    public Slice getMemorySegments() {
        return this.memorySegments;
    }

    public Slice getRegion(int position) {
        position = realPositionOf(position);
        return memorySegments.slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
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
        updateSizeInfo();
    }

    @Override
    public void updateSizeInfo() {
        estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + memorySegments.length();
        elementUsedBytes = Byte.BYTES * positionCount + DECIMAL_MEMORY_SIZE * positionCount;
    }

    public void collectDecimalInfo() {
        // recollect decimal state info util state is not UNSET_STATE anymore.
        if (this.state != DecimalBlockState.UNSET_STATE) {
            return;
        }
        DecimalBlockState resultState = DecimalBlockState.UNSET_STATE;
        for (int position = 0; position < positionCount; position++) {
            position = realPositionOf(position);
            if (!isNull(position)) {
                // get state of block element and merge with result state
                DecimalBlockState elementState = DecimalBlockState.stateOf(memorySegments, position);
                resultState = resultState.merge(elementState);
            }
        }

        this.state = resultState;
    }

    public DecimalBlockState getState() {
        return state;
    }

    public int[] getSelection() {
        return selection;
    }

    public int fastInt1(int position) {
        return (!state.isSimple() || state.getInt1Pos() == UNSET) ? 0 :
            memorySegments.getInt(realPositionOf(position) * DECIMAL_MEMORY_SIZE + state.getInt1Pos() * 4);
    }

    public int fastInt2(int position) {
        return (!state.isSimple() || state.getInt2Pos() == UNSET) ? 0 :
            memorySegments.getInt(realPositionOf(position) * DECIMAL_MEMORY_SIZE + state.getInt2Pos() * 4);
    }

    public int fastFrac(int position) {
        return (!state.isSimple() || state.getFracPos() == UNSET) ? 0 :
            memorySegments.getInt(realPositionOf(position) * DECIMAL_MEMORY_SIZE + state.getFracPos() * 4);
    }

    public boolean isSimple() {
        return state.isSimple();
    }

    public int getInt1Pos() {
        return state.getInt1Pos();
    }

    public int getInt2Pos() {
        return state.getInt2Pos();
    }

    public int getFracPos() {
        return state.getFracPos();
    }

    // note: dangerous!
    public void setMultiResult1(int position, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index + 0, sum0);
        memorySegments.setInt(index + 4, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);
    }

    // note: dangerous!
    public void setMultiResult2(int position, int carry0, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index, carry0);
        memorySegments.setInt(index + 4, sum0);
        memorySegments.setInt(index + 8, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);
    }

    // note: dangerous!
    public void setMultiResult3(int position, int sum0, int sum9, int sum18) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index + 0, sum0);
        memorySegments.setInt(index + 4, sum9);
        memorySegments.setInt(index + 8, sum18);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);
    }

    // note: dangerous!
    public void setMultiResult4(int position, int carry0, int sum0, int sum9, int sum18) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index, carry0);
        memorySegments.setInt(index + 4, sum0);
        memorySegments.setInt(index + 8, sum9);
        memorySegments.setInt(index + 12, sum18);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);
    }

    // note: dangerous!
    public void setSubResult1(int position, int sub0, int sub9, boolean isNeg) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index, sub0);
        memorySegments.setInt(index + 4, sub9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, isNeg ? 1 : 0);
    }

    // note: dangerous!
    public void setSubResult2(int position, int carry, int sub0, int sub9, boolean isNeg) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index, carry);
        memorySegments.setInt(index + 4, sub0);
        memorySegments.setInt(index + 8, sub9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, isNeg ? 1 : 0);
    }

    // note: dangerous!
    public void setAddResult1(int position, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index, sum0);
        memorySegments.setInt(index + 4, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);
    }

    // note: dangerous!
    public void setAddResult2(int position, int carry, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        memorySegments.setInt(index, carry);
        memorySegments.setInt(index + 4, sum0);
        memorySegments.setInt(index + 8, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);
    }

    /**
     * State of decimal block
     */
    enum DecimalBlockState {
        UNSET_STATE(false, UNSET, UNSET, UNSET),

        NOT_SIMPLE(false, UNSET, UNSET, UNSET),

        // frac * 10^-9
        SIMPLE_MODE_1(true, UNSET, UNSET, 0),

        // int1 + frac * 10^-9
        SIMPLE_MODE_2(true, UNSET, 0, 1),

        // int2 * 10^9 + int1 + frac * 10^-9
        SIMPLE_MODE_3(true, 0, 1, 2);

        private final boolean isSimple;
        private final int int2Pos;
        private final int int1Pos;
        private final int fracPos;

        DecimalBlockState(boolean isSimple, int int2Pos, int int1Pos, int fracPos) {
            this.isSimple = isSimple;
            this.int2Pos = int2Pos;
            this.int1Pos = int1Pos;
            this.fracPos = fracPos;
        }

        public DecimalBlockState merge(DecimalBlockState that) {
            if (this == UNSET_STATE) {
                return that;
            }

            if (that == UNSET_STATE) {
                return this;
            }

            if (this == that && this != NOT_SIMPLE) {
                return this;
            }
            return NOT_SIMPLE;
        }

        public static DecimalBlockState stateOf(Slice memorySegments, int position) {
            int isNeg = memorySegments.getByte(position * DECIMAL_MEMORY_SIZE + IS_NEG_OFFSET) & 0xFF;
            if (isNeg == 1) {
                return NOT_SIMPLE;
            }

            int integers = memorySegments.getByte(position * DECIMAL_MEMORY_SIZE + INTEGERS_OFFSET) & 0xFF;
            int fractions = memorySegments.getByte(position * DECIMAL_MEMORY_SIZE + FRACTIONS_OFFSET) & 0xFF;

            int intWord = roundUp(integers);
            int fracWord = roundUp(fractions);

            if (intWord == 0 && fracWord == 1) {
                // frac * 10^-9
                return SIMPLE_MODE_1;
            } else if (intWord == 1 && fracWord == 1) {
                // int1 + frac * 10^-9
                return SIMPLE_MODE_2;
            } else if (intWord == 2 && fracWord == 1) {
                // int2 * 10^9 + int1 + frac * 10^-9
                return SIMPLE_MODE_3;
            }

            return NOT_SIMPLE;
        }

        public static DecimalBlockState stateOf(DecimalStructure decimalStructure) {
            if (decimalStructure == null || decimalStructure.isNeg()) {
                return NOT_SIMPLE;
            }

            int integers = decimalStructure.getIntegers();
            int fractions = decimalStructure.getFractions();

            int intWord = roundUp(integers);
            int fracWord = roundUp(fractions);

            if (intWord == 0 && fracWord == 1) {
                // frac * 10^-9
                return SIMPLE_MODE_1;
            } else if (intWord == 1 && fracWord == 1) {
                // int1 + frac * 10^-9
                return SIMPLE_MODE_2;
            } else if (intWord == 2 && fracWord == 1) {
                // int2 * 10^9 + int1 + frac * 10^-9
                return SIMPLE_MODE_3;
            }

            return NOT_SIMPLE;
        }

        public boolean isSimple() {
            return isSimple;
        }

        public int getInt2Pos() {
            return int2Pos;
        }

        public int getInt1Pos() {
            return int1Pos;
        }

        public int getFracPos() {
            return fracPos;
        }
    }
}