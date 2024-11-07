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
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.datatype.RawBytesDecimalUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.common.utils.hash.IStreamingHasher;
import com.alibaba.polardbx.executor.operator.util.BatchBlockWriter;
import com.alibaba.polardbx.executor.operator.util.DriverObjectPool;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.base.Preconditions;
import io.airlift.slice.BasicSliceOutput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DERIVED_FRACTIONS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DEC128;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DEC64;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_OVERFLOW;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_TRUNCATED;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.FRACTIONS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.INTEGERS_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.IS_NEG_OFFSET;
import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.DECIMAL_128;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.DECIMAL_64;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.FULL;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.SIMPLE_MODE_2;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.SIMPLE_MODE_3;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.UNALLOC_STATE;

/**
 * An implement of Decimal block mixed with fixed and variable length
 */
public class DecimalBlock extends AbstractBlock implements SegmentedDecimalBlock {
    private static final int NULL_VALUE = 0;

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(DecimalBlock.class).instanceSize();

    /**
     * Allocate the memory of decimal vector
     */
    protected Slice memorySegments;

    protected DecimalStructure hashCodeTmpBuffer;
    protected DecimalStructure hashCodeResultBuffer;
    protected DecimalStructure regionTmpBuffer;

    /**
     * decimal64 values in Decimal64
     * or low bits in Decimal128
     */
    protected long[] decimal64Values;
    /**
     * high bits in Decimal128
     */
    protected long[] decimal128HighValues;

    private int[] selection;
    /**
     * A Decimal Block is simple only if all non-null decimal values are in format of
     * (a2 * 10^(9*-1) + a1 * 10^(9*0) + b * 10^(9*-1)).
     * In other word, the int word and frac word is 0 or 1.
     */
    private DecimalBlockState state;

    private DriverObjectPool<long[]> objectPool = null;

    /**
     * For Vectorized expression result vector.
     */
    public DecimalBlock(DataType dataType, int slotLen) {
        super(dataType, slotLen);
        // delay memory allocation
        this.selection = null;
        this.decimal64Values = null;
        this.state = UNALLOC_STATE;
        updateSizeInfo();
    }

    private DecimalBlock(DataType dataType, int positionCount, boolean[] valueIsNull, boolean hasNull) {
        super(dataType, positionCount, valueIsNull, hasNull);
    }

    public DecimalBlock(DataType dataType, int slotLen, DriverObjectPool<long[]> objectPool, int chunkLimit) {
        super(dataType, slotLen);
        // delay memory allocation
        this.selection = null;
        this.decimal64Values = null;
        this.state = UNALLOC_STATE;
        this.objectPool = objectPool;
        this.recycler = objectPool.getRecycler(chunkLimit);
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
    public DecimalBlock(DataType dataType, int positionCount, boolean[] valueIsNull,
                        Slice memorySegments, DecimalBlockState state) {
        super(dataType, 0, positionCount, valueIsNull);
        this.memorySegments = memorySegments;
        this.selection = null;

        this.state = state;
        updateSizeInfo();
    }

    /**
     * Decimal64
     */
    public DecimalBlock(DataType dataType, int positionCount, boolean hasNull, boolean[] valueIsNull,
                        long[] decimal64Values) {
        super(dataType, positionCount, valueIsNull, hasNull);
        this.decimal64Values = decimal64Values;
        this.selection = null;

        this.state = DecimalBlockState.DECIMAL_64;
        updateSizeInfo();
    }

    /**
     * Decimal 128 with selection
     */
    public DecimalBlock(DataType dataType, int positionCount, boolean hasNull, boolean[] valueIsNull,
                        long[] decimal128Low, long[] decimal128High, int[] selection) {
        super(dataType, positionCount, valueIsNull, hasNull);
        this.decimal64Values = decimal128Low;
        this.decimal128HighValues = decimal128High;
        this.selection = selection;

        this.state = DecimalBlockState.DECIMAL_128;
        updateSizeInfo();
    }

    /**
     * Decimal 64 with selection
     */
    public DecimalBlock(DataType dataType, int positionCount, boolean hasNull, boolean[] valueIsNull,
                        long[] decimal64Values, int[] selection) {
        super(dataType, positionCount, valueIsNull, hasNull);
        this.decimal64Values = decimal64Values;
        this.selection = selection;

        this.state = DecimalBlockState.DECIMAL_64;
        updateSizeInfo();
    }

    public DecimalBlock(DataType dataType, int positionCount, boolean useDecimal64) {
        super(dataType, positionCount);
        allocateValues(useDecimal64);
        this.selection = null;
        updateSizeInfo();
    }

    public static DecimalBlock buildDecimal128Block(DataType dataType, int positionCount, boolean hasNull,
                                                    boolean[] valueIsNull,
                                                    long[] decimal128Low, long[] decimal128High) {
        DecimalBlock decimalBlock = new DecimalBlock(dataType, positionCount, valueIsNull, hasNull);
        decimalBlock.decimal64Values = decimal128Low;
        decimalBlock.decimal128HighValues = decimal128High;
        decimalBlock.selection = null;

        decimalBlock.state = DecimalBlockState.DECIMAL_128;
        decimalBlock.updateSizeInfo();
        return decimalBlock;
    }

    public static DecimalBlock from(DecimalBlock other, int selSize, int[] selection, boolean useSelection) {
        if (useSelection) {
            if (other.state == DECIMAL_64) {
                // for decimal-64 mode.
                return new DecimalBlock(other.dataType, selSize, other.hasNull,
                    other.isNull, other.decimal64Values, selection);
            } else if (other.state == DECIMAL_128) {
                // for decimal-128 mode.
                return new DecimalBlock(other.dataType, selSize, other.hasNull,
                    other.isNull, other.decimal64Values, other.decimal128HighValues, selection);
            } else {
                // for normal mode.
                return new DecimalBlock(other.dataType, other.getMemorySegments(),
                    other.isNull, other.hasNull, selSize, selection, other.state);
            }
        }
        if (other.state == DECIMAL_64) {
            // case1: for decimal-64 mode.
            if (selection == null) {
                // case 1.1: directly copy long array
                boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, null, selSize);
                return new DecimalBlock(other.dataType, selSize,
                    targetNulls != null,
                    targetNulls,
                    BlockUtils.copyLongArray(other.decimal64Values, null, selSize),
                    null);
            } else {
                // case 1.2: copy long array by selection.
                boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, selection, selSize);
                return new DecimalBlock(other.dataType, selSize,
                    targetNulls != null, targetNulls,
                    BlockUtils.copyLongArray(other.decimal64Values, selection, selSize),
                    null);
            }
        } else if (other.state == DECIMAL_128) {
            // case2: for decimal-128 mode.
            if (selection == null) {
                // case 2.1: directly copy long array
                boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, null, selSize);
                return new DecimalBlock(other.dataType, selSize,
                    targetNulls != null,
                    targetNulls,
                    BlockUtils.copyLongArray(other.decimal64Values, null, selSize),
                    BlockUtils.copyLongArray(other.decimal128HighValues, null, selSize),
                    null);
            } else {
                // case 2.2: copy long array by selection.
                boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, selection, selSize);
                return new DecimalBlock(other.dataType, selSize,
                    targetNulls != null, targetNulls,
                    BlockUtils.copyLongArray(other.decimal64Values, selection, selSize),
                    BlockUtils.copyLongArray(other.decimal128HighValues, selection, selSize),
                    null);
            }
        } else {
            // case3: for normal-decimal mode.
            if (selection == null) {
                // case 3.1: directly copy slice.
                boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, null, selSize);
                return new DecimalBlock(other.dataType,
                    Slices.copyOf(other.getMemorySegments()),
                    targetNulls, targetNulls != null,
                    selSize, null,
                    other.state);
            } else {
                // case 3.2: copy slice by selection in fixed size.
                boolean[] targetNulls = BlockUtils.copyNullArray(other.isNull, selection, selSize);
                Slice targetSlice = Slices.allocate(selSize * DECIMAL_MEMORY_SIZE);
                Slice sourceSlice = other.memorySegments;
                for (int position = 0; position < selSize; position++) {
                    targetSlice.setBytes(position * DECIMAL_MEMORY_SIZE, sourceSlice,
                        selection[position] * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
                }

                return new DecimalBlock(other.dataType, targetSlice,
                    targetNulls, targetNulls != null, selSize, null, other.state);
            }
        }
    }

    @Override
    public void recycle() {
        if (recycler != null && decimal64Values != null) {
            recycler.recycle(decimal64Values);
        }
    }

    private void allocateValues(boolean isDecimal64) {
        if (isDecimal64) {
            this.decimal64Values = new long[positionCount];
            this.state = DecimalBlockState.DECIMAL_64;
        } else {
            this.memorySegments = Slices.allocate(positionCount * DECIMAL_MEMORY_SIZE);
            this.state = DecimalBlockState.UNSET_STATE;
        }
        updateSizeInfo();
    }

    public int realPositionOf(int position) {
        return selection == null ? position : selection[position];
    }

    @Override
    public void sum(int[] groupSelected, int selSize, long[] results) {
        Preconditions.checkArgument(selSize <= positionCount);
        if (isDecimal64()) {
            boolean overflow64 = false;
            long sum = 0L;
            if (selection != null) {
                for (int i = 0; i < selSize; i++) {
                    int position = selection[groupSelected[i]];
                    long value = decimal64Values[position];
                    long oldValue = sum;
                    sum = value + oldValue;
                    overflow64 |= ((value ^ sum) & (oldValue ^ sum)) < 0;
                }
            } else {
                for (int i = 0; i < selSize; i++) {
                    int position = groupSelected[i];
                    long value = decimal64Values[position];
                    long oldValue = sum;
                    sum = value + oldValue;
                    overflow64 |= ((value ^ sum) & (oldValue ^ sum)) < 0;
                }
            }
            if (!overflow64) {
                results[0] = sum;
                results[1] = 0;
                results[2] = E_DEC_DEC64;
                return;
            }
            // try decimal128
            long sumLow = 0L, sumHigh = 0L;
            // there is no need to perform overflow detection on decimal128,
            // since the sum of a decimal64 block will never overflow decimal128
            boolean overflow128 = false;
            if (selection != null) {
                for (int i = 0; i < selSize; i++) {
                    int position = selection[groupSelected[i]];
                    long value = decimal64Values[position];
                    long valueHigh = value > 0 ? 0 : -1;
                    sumHigh += valueHigh;
                    long addResult = value + sumLow;
                    long carryOut = ((value & sumLow)
                        | ((value | sumLow) & (~addResult))) >>> 63;
                    sumHigh += carryOut;
                    sumLow = addResult;
                }
            } else {
                for (int i = 0; i < selSize; i++) {
                    int position = groupSelected[i];
                    long value = decimal64Values[position];
                    long valueHigh = value > 0 ? 0 : -1;
                    sumHigh += valueHigh;
                    long addResult = value + sumLow;
                    long carryOut = ((value & sumLow)
                        | ((value | sumLow) & (~addResult))) >>> 63;
                    sumHigh += carryOut;
                    sumLow = addResult;
                }
            }
            if (!overflow128) {
                results[0] = sumLow;
                results[1] = sumHigh;
                results[2] = E_DEC_DEC128;
                return;
            }

            // reset results and return error state
            results[0] = -1L;
            results[1] = -1L;
            results[2] = E_DEC_TRUNCATED;
        } else if (isDecimal128()) {
            long sumLow = 0L, sumHigh = 0L;
            // need to perform overflow detection in a decimal128 block
            boolean overflow128 = false;
            if (selection != null) {
                for (int i = 0; i < selSize; i++) {
                    int position = selection[groupSelected[i]];
                    long decimal128Low = decimal64Values[position];
                    long decimal128High = decimal128HighValues[position];
                    long newDecimal128High = sumHigh + decimal128High;
                    overflow128 |= ((decimal128High ^ newDecimal128High)
                        & (sumHigh ^ newDecimal128High)) < 0;
                    long newDecimal128Low = sumLow + decimal128Low;
                    long carryOut = ((sumLow & decimal128Low)
                        | ((sumLow | decimal128Low) & (~newDecimal128Low))) >>> 63;
                    newDecimal128High += carryOut;
                    sumHigh = newDecimal128High;
                    sumLow = newDecimal128Low;
                }
            } else {
                for (int i = 0; i < selSize; i++) {
                    int position = groupSelected[i];
                    long decimal128Low = decimal64Values[position];
                    long decimal128High = decimal128HighValues[position];
                    long newDecimal128High = sumHigh + decimal128High;
                    overflow128 |= ((decimal128High ^ newDecimal128High)
                        & (sumHigh ^ newDecimal128High)) < 0;
                    long newDecimal128Low = sumLow + decimal128Low;
                    long carryOut = ((sumLow & decimal128Low)
                        | ((sumLow | decimal128Low) & (~newDecimal128Low))) >>> 63;
                    newDecimal128High += carryOut;
                    sumHigh = newDecimal128High;
                    sumLow = newDecimal128Low;
                }
            }
            if (!overflow128) {
                results[0] = sumLow;
                results[1] = sumHigh;
                results[2] = E_DEC_DEC128;
                return;
            }

            // reset results and return error state
            results[0] = -1L;
            results[1] = -1L;
            results[2] = E_DEC_TRUNCATED;
        } else {
            // for normal mode, just return error state.
            results[0] = -1L;
            results[1] = -1L;
            results[2] = E_DEC_TRUNCATED;
        }
    }

    @Override
    public void sum(int startIndexIncluded, int endIndexExcluded, long[] results) {
        Preconditions.checkArgument(endIndexExcluded <= positionCount);
        if (isDecimal64()) {
            boolean overflow64 = false;
            long sum = 0L;
            if (selection != null) {
                for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
                    int position = selection[i];
                    long value = decimal64Values[position];
                    long oldValue = sum;
                    sum = value + oldValue;
                    overflow64 |= ((value ^ sum) & (oldValue ^ sum)) < 0;
                }
            } else {
                for (int position = startIndexIncluded; position < endIndexExcluded; position++) {
                    long value = decimal64Values[position];
                    long oldValue = sum;
                    sum = value + oldValue;
                    overflow64 |= ((value ^ sum) & (oldValue ^ sum)) < 0;
                }
            }
            if (!overflow64) {
                results[0] = sum;
                results[1] = 0;
                results[2] = E_DEC_DEC64;
                return;
            }

            // try decimal128
            long sumLow = 0L, sumHigh = 0L;
            // there is no need to perform overflow detection on decimal128,
            // since the sum of a decimal64 block will never overflow decimal128
            boolean overflow128 = false;
            if (selection != null) {
                for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
                    int position = selection[i];
                    long value = decimal64Values[position];
                    long valueHigh = value > 0 ? 0 : -1;
                    sumHigh += valueHigh;
                    long addResult = value + sumLow;
                    long carryOut = ((value & sumLow)
                        | ((value | sumLow) & (~addResult))) >>> 63;
                    sumHigh += carryOut;
                    sumLow = addResult;
                }
            } else {
                for (int position = startIndexIncluded; position < endIndexExcluded; position++) {
                    long value = decimal64Values[position];
                    long valueHigh = value > 0 ? 0 : -1;
                    sumHigh += valueHigh;
                    long addResult = value + sumLow;
                    long carryOut = ((value & sumLow)
                        | ((value | sumLow) & (~addResult))) >>> 63;
                    sumHigh += carryOut;
                    sumLow = addResult;
                }
            }
            if (!overflow128) {
                results[0] = sumLow;
                results[1] = sumHigh;
                results[2] = E_DEC_DEC128;
                return;
            }

            // reset results and return error state
            results[0] = -1L;
            results[1] = -1L;
            results[2] = E_DEC_TRUNCATED;
        } else if (this.state == DECIMAL_128) {
            long sumLow = 0L, sumHigh = 0L;
            // need to perform overflow detection in a decimal128 block
            boolean overflow128 = false;
            if (selection != null) {
                for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
                    int position = selection[i];
                    long decimal128Low = decimal64Values[position];
                    long decimal128High = decimal128HighValues[position];
                    long newDecimal128High = sumHigh + decimal128High;
                    overflow128 |= ((decimal128High ^ newDecimal128High)
                        & (sumHigh ^ newDecimal128High)) < 0;
                    long newDecimal128Low = sumLow + decimal128Low;
                    long carryOut = ((sumLow & decimal128Low)
                        | ((sumLow | decimal128Low) & (~newDecimal128Low))) >>> 63;
                    newDecimal128High += carryOut;
                    sumHigh = newDecimal128High;
                    sumLow = newDecimal128Low;
                }
            } else {
                for (int position = startIndexIncluded; position < endIndexExcluded; position++) {
                    long decimal128Low = decimal64Values[position];
                    long decimal128High = decimal128HighValues[position];
                    long newDecimal128High = sumHigh + decimal128High;
                    overflow128 |= ((decimal128High ^ newDecimal128High)
                        & (sumHigh ^ newDecimal128High)) < 0;
                    long newDecimal128Low = sumLow + decimal128Low;
                    long carryOut = ((sumLow & decimal128Low)
                        | ((sumLow | decimal128Low) & (~newDecimal128Low))) >>> 63;
                    newDecimal128High += carryOut;
                    sumHigh = newDecimal128High;
                    sumLow = newDecimal128Low;
                }
            }
            if (!overflow128) {
                results[0] = sumLow;
                results[1] = sumHigh;
                results[2] = E_DEC_DEC128;
                return;
            }

            // reset results and return error state
            results[0] = -1L;
            results[1] = -1L;
            results[2] = E_DEC_TRUNCATED;
        } else {
            // for non-decimal-64 mode, just return error state.
            results[0] = -1L;
            results[1] = -1L;
            results[2] = E_DEC_TRUNCATED;
        }
    }

    @Override
    public void sum(int startIndexIncluded, int endIndexExcluded, long[] sumResultArray, int[] sumStatusArray,
                    int[] normalizedGroupIds) {
        Preconditions.checkArgument(endIndexExcluded <= positionCount);
        if (isDecimal64()) {
            boolean overflow = false;
            long sum = 0L;
            if (selection != null) {
                for (int i = startIndexIncluded; i < endIndexExcluded; i++) {
                    int position = selection[i];
                    long value = decimal64Values[position];
                    int normalizedGroupId = normalizedGroupIds[position];

                    // sum
                    long oldValue = sumResultArray[normalizedGroupId];
                    sum = value + oldValue;
                    sumResultArray[normalizedGroupId] = sum;

                    // check overflow
                    overflow |= ((value ^ sum) & (oldValue ^ sum)) < 0;
                }
            } else {
                for (int position = startIndexIncluded; position < endIndexExcluded; position++) {
                    long value = decimal64Values[position];
                    int normalizedGroupId = normalizedGroupIds[position];

                    // sum
                    long oldValue = sumResultArray[normalizedGroupId];
                    sum = value + oldValue;
                    sumResultArray[normalizedGroupId] = sum;

                    // check overflow
                    overflow |= ((value ^ sum) & (oldValue ^ sum)) < 0;
                }
            }
            sumStatusArray[0] = overflow ? E_DEC_OVERFLOW : E_DEC_DEC64;
        } else {
            // for non-decimal-64 mode, just return error state.
            // decimal128 does not support put the result into a long array
            sumStatusArray[0] = E_DEC_TRUNCATED;
        }
    }

    @Override
    public boolean isNull(int position) {
        position = realPositionOf(position);
        return isNullInner(position);
    }

    @Override
    public Decimal getDecimal(int position) {
        position = realPositionOf(position);
        return getDecimalInner(position);
    }

    public void getDecimalStructure(DecimalStructure target, int position) {
        position = realPositionOf(position);
        if (isDecimal64()) {
            target.setLongWithScale(decimal64Values[position], getScale());
        } else if (isDecimal128()) {
            DecimalStructure buffer = getRegionTmpBuffer();
            FastDecimalUtils.setDecimal128WithScale(buffer, target, decimal64Values[position],
                decimal128HighValues[position], getScale());
        } else {
            memorySegments.slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE, target.getDecimalMemorySegment());
        }
    }

    @Override
    public long getLong(int position) {
        position = realPositionOf(position);
        return getLongInner(position);
    }

    @Override
    public long getDecimal128Low(int position) {
        position = realPositionOf(position);
        return getDecimal128LowInner(position);
    }

    @Override
    public long getDecimal128High(int position) {
        position = realPositionOf(position);
        return getDecimal128HighInner(position);
    }

    @Override
    public Object getObject(int position) {
        position = realPositionOf(position);
        return isNullInner(position) ? null : getDecimalInner(position);
    }

    public void writePositionTo(int position, BatchBlockWriter.BatchDecimalBlockBuilder blockWriter, Slice output) {
        position = realPositionOf(position);
        blockWriter.writeDecimal(getDecimalInner(position, output));
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder) {
        position = realPositionOf(position);

        if (blockBuilder instanceof DecimalBlockBuilder) {
            writePositionToInner(position, (DecimalBlockBuilder) blockBuilder);
        } else if (blockBuilder instanceof BatchBlockWriter.BatchDecimalBlockBuilder) {
            BatchBlockWriter.BatchDecimalBlockBuilder b = (BatchBlockWriter.BatchDecimalBlockBuilder) blockBuilder;

            if (isNullInner(position)) {
                b.appendNull();
            } else if (isDecimal64() && (b.isDecimal64() || b.isDecimal128())) {
                b.writeLong(getLongInner(position));
            } else if (isDecimal128() && (b.isDecimal64() || b.isDecimal128())) {
                b.writeDecimal128(getDecimal128LowInner(position), getDecimal128HighInner(position));
            } else {
                b.writeDecimal(getDecimalInner(position));
            }
        } else {
            throw new AssertionError(blockBuilder.getClass());
        }
    }

    public long[] allocateDecimal64() {
        if (isUnalloc()) {
            if (objectPool != null) {
                long[] pooled = objectPool.poll();
                if (pooled != null && pooled.length >= positionCount) {
                    this.decimal64Values = pooled;
                } else {
                    this.decimal64Values = new long[positionCount];
                }
            } else {
                this.decimal64Values = new long[positionCount];
            }

            this.state = DecimalBlockState.DECIMAL_64;
        } else if (this.state.isNormal()) {
            throw new IllegalStateException("Should not allocate decimal64 inside a normal decimal block");
        }
        updateSizeInfo();
        return this.decimal64Values;
    }

    public void allocateDecimal128() {
        if (isUnalloc()) {
            if (objectPool != null) {
                long[] pooled = objectPool.poll();
                if (pooled != null && pooled.length >= positionCount) {
                    this.decimal64Values = pooled;
                    pooled = objectPool.poll();
                } else {
                    this.decimal64Values = new long[positionCount];
                    pooled = null;
                }
                if (pooled != null && pooled.length >= positionCount) {
                    this.decimal128HighValues = pooled;
                } else {
                    this.decimal128HighValues = new long[positionCount];
                }
            } else {
                this.decimal64Values = new long[positionCount];
                this.decimal128HighValues = new long[positionCount];
            }
            this.state = DecimalBlockState.DECIMAL_128;
        } else if (isDecimal64()) {
            // will not clear existing decimal64Values to reduce operations,
            // caller should be aware of this behavior
            if (objectPool != null) {
                long[] pooled = objectPool.poll();
                if (pooled != null && pooled.length >= positionCount) {
                    this.decimal128HighValues = pooled;
                } else {
                    this.decimal128HighValues = new long[positionCount];
                }
            } else {
                this.decimal128HighValues = new long[positionCount];
            }
            this.state = DecimalBlockState.DECIMAL_128;
        } else if (this.state.isNormal()) {
            throw new IllegalStateException("Should not allocate decimal128 inside a normal decimal block");
        }

        updateSizeInfo();
    }

    public void deallocateDecimal64() {
        if (isDecimal64()) {
            this.decimal64Values = null;
            this.state = UNALLOC_STATE;
        }
    }

    public void deallocateDecimal128() {
        if (isDecimal128()) {
            this.decimal64Values = null;
            this.decimal128HighValues = null;
            this.state = UNALLOC_STATE;
        }
    }

    public long[] decimal64Values() {
        return this.decimal64Values;
    }

    private void writePositionToInner(int position, DecimalBlockBuilder b) {
        if (isNullInner(position)) {
            b.appendNull();
            return;
        }
        if (isDecimal64()) {
            if (b.isDecimal64() || b.isDecimal128() || b.state.isUnset()) {
                b.setScale(getScale());
                b.writeLong(getLongInner(position));
            } else {
                b.writeDecimal(getDecimalInner(position));
            }
            return;
        }

        if (isDecimal128()) {
            if (b.isDecimal64() || b.isDecimal128() || b.state.isUnset()) {
                b.writeDecimal128(getDecimal128LowInner(position), getDecimal128HighInner(position));
            } else {
                b.writeDecimal(getDecimalInner(position));
            }
            return;
        }

        b.convertToNormalDecimal();
        // normal decimal
        // write to decimal memory segments
        b.sliceOutput.writeBytes(memorySegments, position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
        b.valueIsNull.add(false);

        // update decimal info
        DecimalBlockState elementState = DecimalBlockState.stateOf(memorySegments, position);
        b.state = b.state.merge(elementState);
    }

    @Override
    public int hashCode(int position) {
        position = realPositionOf(position);
        return hashCodeInner(position);
    }

    @Override
    public long hashCodeUseXxhash(int pos) {
        int realPos = realPositionOf(pos);
        if (isNullInner(realPos)) {
            return NULL_HASH_CODE;
        } else {
            if (isDecimal64()) {
                long val = getLongInner(realPos);
                DecimalStructure bufferDec = getHashCodeTmpBuffer();
                DecimalStructure resultDec = getHashCodeResultBuffer();
                FastDecimalUtils.setLongWithScale(bufferDec, resultDec, val, getScale());

                return RawBytesDecimalUtils.hashCode(resultDec.getDecimalMemorySegment());
            } else if (isDecimal128()) {
                long decimal128Low = getDecimal128LowInner(realPos);
                long decimal128High = getDecimal128HighInner(realPos);
                DecimalStructure bufferDec = getHashCodeTmpBuffer();
                DecimalStructure resultDec = getHashCodeResultBuffer();
                FastDecimalUtils.setDecimal128WithScale(bufferDec, resultDec, decimal128Low, decimal128High,
                    getScale());

                return RawBytesDecimalUtils.hashCode(resultDec.getDecimalMemorySegment());
            } else {
                Slice memorySegment = memorySegments.slice(realPos * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
                return RawBytesDecimalUtils.hashCode(memorySegment);
            }
        }
    }

    private DecimalStructure getHashCodeTmpBuffer() {
        if (this.hashCodeTmpBuffer == null) {
            this.hashCodeTmpBuffer = new DecimalStructure();
        }
        return this.hashCodeTmpBuffer;
    }

    private DecimalStructure getHashCodeResultBuffer() {
        if (this.hashCodeResultBuffer == null) {
            this.hashCodeResultBuffer = new DecimalStructure();
        }
        return this.hashCodeResultBuffer;
    }

    private DecimalStructure getRegionTmpBuffer() {
        if (this.regionTmpBuffer == null) {
            this.regionTmpBuffer = new DecimalStructure();
        }
        return this.regionTmpBuffer;
    }

    @Override
    public boolean equals(int position, Block other, int otherPosition) {
        position = realPositionOf(position);
        return equalsInner(position, other, otherPosition);
    }

    @Override
    public Slice segmentUncheckedAt(int position) {
        position = realPositionOf(position);
        return segmentUncheckedAtInner(position);
    }

    @Override
    public void addToHasher(IStreamingHasher sink, int position) {
        position = realPositionOf(position);
        addToHasherInner(sink, position);
    }

    @Override
    public void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output) {
        if (output instanceof DecimalBlock) {
            DecimalBlock outputVectorSlot = output.cast(DecimalBlock.class);
            if (outputVectorSlot.isUnalloc()) {
                copySelectedToUnalloc(selectedInUse, sel, size, outputVectorSlot);
            } else if (outputVectorSlot.isDecimal64()) {
                copySelectedToDecimal64(selectedInUse, sel, size, outputVectorSlot);
            } else if (outputVectorSlot.isDecimal128()) {
                copySelectedToDecimal128(selectedInUse, sel, size, outputVectorSlot);
            } else {
                copySelectedToNormal(selectedInUse, sel, size, outputVectorSlot);
            }
        } else {
            BlockUtils.copySelectedInCommon(selectedInUse, sel, size, this, output);
        }

        super.copySelected(selectedInUse, sel, size, output);
    }

    private void copySelectedToUnalloc(boolean selectedInUse, int[] sel, int size, DecimalBlock outputVectorSlot) {
        // copy current dataType into output slot
        outputVectorSlot.dataType = new DecimalType(dataType.getPrecision(), dataType.getScale());
        if (isDecimal64()) {
            outputVectorSlot.allocateDecimal64();
            copySelectedToDecimal64(selectedInUse, sel, size, outputVectorSlot);
            return;
        }
        if (isDecimal128()) {
            outputVectorSlot.allocateDecimal128();
            copySelectedToDecimal128(selectedInUse, sel, size, outputVectorSlot);
            return;
        }
        if (isUnalloc()) {
            // should not reach here
            return;
        }
        copySelectedToNormal(selectedInUse, sel, size, outputVectorSlot);
    }

    private void copySelectedToDecimal64(boolean selectedInUse, int[] sel, int size, DecimalBlock outputVectorSlot) {
        if (isDecimal64()) {
            // decimal64 -> decimal64
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    outputVectorSlot.decimal64Values[j] = decimal64Values[j];
                }
            } else {
                System.arraycopy(decimal64Values, 0, outputVectorSlot.decimal64Values, 0, size);
            }
            return;
        }
        if (isDecimal128()) {
            // decimal128 -> decimal64
            // must convert target decimal64 to decimal128
            outputVectorSlot.allocToDecimal128();
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    outputVectorSlot.decimal64Values[j] = decimal64Values[j];
                    outputVectorSlot.decimal128HighValues[j] = decimal128HighValues[j];
                }
            } else {
                System.arraycopy(decimal64Values, 0, outputVectorSlot.decimal64Values, 0, size);
                System.arraycopy(decimal128HighValues, 0, outputVectorSlot.decimal128HighValues, 0, size);
            }
            return;
        }
        // normal -> decimal64
        // must convert target decimal64 to normal
        outputVectorSlot.allocToNormal();
        copySelectedToNormal(selectedInUse, sel, size, outputVectorSlot);
    }

    private void copySelectedToDecimal128(boolean selectedInUse, int[] sel, int size, DecimalBlock outputVectorSlot) {
        if (isDecimal64()) {
            // decimal64 -> decimal128
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    outputVectorSlot.decimal64Values[j] = decimal64Values[j];
                    outputVectorSlot.decimal128HighValues[j] = decimal64Values[j] < 0 ? -1 : 0;
                }
            } else {
                System.arraycopy(decimal64Values, 0, outputVectorSlot.decimal64Values, 0, size);
                for (int i = 0; i < size; i++) {
                    outputVectorSlot.decimal128HighValues[i] = decimal64Values[i] < 0 ? -1 : 0;
                }
            }
            return;
        }
        if (isDecimal128()) {
            // decimal128 -> decimal128
            // must convert target decimal64 to decimal128
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    outputVectorSlot.decimal64Values[j] = decimal64Values[j];
                    outputVectorSlot.decimal128HighValues[j] = decimal128HighValues[j];
                }
            } else {
                System.arraycopy(decimal64Values, 0, outputVectorSlot.decimal64Values, 0, size);
                System.arraycopy(decimal128HighValues, 0, outputVectorSlot.decimal128HighValues, 0, size);
            }
            return;
        }
        // normal -> decimal128
        // must convert target decimal128 to normal
        outputVectorSlot.allocToNormal();
        copySelectedToNormal(selectedInUse, sel, size, outputVectorSlot);
    }

    private void copySelectedToNormal(boolean selectedInUse, int[] sel, int size, DecimalBlock outputVectorSlot) {
        if (isDecimal64()) {
            // decimal64 -> normal
            DecimalStructure bufferDec = new DecimalStructure();
            DecimalStructure resultDec = new DecimalStructure();
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    FastDecimalUtils.setLongWithScale(bufferDec, resultDec, decimal64Values[j], getScale());
                    int index = j * DECIMAL_MEMORY_SIZE;
                    outputVectorSlot.getMemorySegments().setBytes(index, resultDec.getDecimalMemorySegment(), 0,
                        DECIMAL_MEMORY_SIZE);
                }
            } else {
                for (int i = 0; i < size; i++) {
                    FastDecimalUtils.setLongWithScale(bufferDec, resultDec, decimal64Values[i], getScale());
                    int index = i * DECIMAL_MEMORY_SIZE;
                    outputVectorSlot.getMemorySegments().setBytes(index, resultDec.getDecimalMemorySegment(), 0,
                        DECIMAL_MEMORY_SIZE);
                }
            }
            outputVectorSlot.collectDecimalInfo();
            return;
        }
        if (isDecimal128()) {
            // decimal128 -> normal
            DecimalStructure bufferDec = new DecimalStructure();
            DecimalStructure resultDec = new DecimalStructure();
            if (selectedInUse) {
                for (int i = 0; i < size; i++) {
                    int j = sel[i];
                    long decimal128Low = decimal64Values[j];
                    long decimal128High = decimal128HighValues[j];
                    FastDecimalUtils.setDecimal128WithScale(bufferDec, resultDec, decimal128Low, decimal128High,
                        getScale());
                    int index = j * DECIMAL_MEMORY_SIZE;
                    outputVectorSlot.getMemorySegments().setBytes(index, resultDec.getDecimalMemorySegment(), 0,
                        DECIMAL_MEMORY_SIZE);
                }
            } else {
                for (int i = 0; i < size; i++) {
                    long decimal128Low = decimal64Values[i];
                    long decimal128High = decimal128HighValues[i];
                    FastDecimalUtils.setDecimal128WithScale(bufferDec, resultDec, decimal128Low, decimal128High,
                        getScale());
                    int index = i * DECIMAL_MEMORY_SIZE;
                    outputVectorSlot.getMemorySegments().setBytes(index, resultDec.getDecimalMemorySegment(), 0,
                        DECIMAL_MEMORY_SIZE);
                }
            }
            outputVectorSlot.collectDecimalInfo();
            return;
        }
        // normal -> normal
        if (selectedInUse) {
            for (int i = 0; i < size; i++) {
                int j = sel[i];

                // copy memory segment from specified position in selection array.
                int fromIndex = j * DECIMAL_MEMORY_SIZE;
                outputVectorSlot.getMemorySegments()
                    .setBytes(fromIndex, getMemorySegments(), fromIndex, DECIMAL_MEMORY_SIZE);
            }
        } else {
            // directly copy memory.
            outputVectorSlot.getMemorySegments().setBytes(0, getMemorySegments());
        }
        outputVectorSlot.collectDecimalInfo();
    }

    @Override
    public void shallowCopyTo(RandomAccessBlock another) {
        if (!(another instanceof DecimalBlock)) {
            GeneralUtil.nestedException("cannot shallow copy to " + another == null ? null : another.toString());
        }
        DecimalBlock vectorSlot = another.cast(DecimalBlock.class);
        super.shallowCopyTo(vectorSlot);
        vectorSlot.memorySegments = memorySegments;
        vectorSlot.decimal64Values = decimal64Values;
        vectorSlot.decimal128HighValues = decimal128HighValues;
        vectorSlot.state = state;
    }

    @Override
    protected Object getElementAtUnchecked(int position) {
        position = realPositionOf(position);
        return getElementAtUncheckedInner(position);
    }

    @Override
    public void setElementAt(int position, Object element) {
        position = realPositionOf(position);
        setElementAtInner(position, element);
    }

    public void encoding(SliceOutput sliceOutput) {
        sliceOutput.writeInt(positionCount * DECIMAL_MEMORY_SIZE);
        if (selection != null) {
            for (int i = 0; i < positionCount; i++) {
                int j = selection[i];
                sliceOutput.appendBytes(this.getMemorySegments().slice(j * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE));
            }

        } else {
            sliceOutput.appendBytes(this.getMemorySegments());
        }
    }

    /**
     * Just allowed to be used in vectorized expression
     */
    @Deprecated
    public Slice getMemorySegments() {
        allocToNormal();
        return this.memorySegments;
    }

    /**
     * dangerous!
     * can only be invoked by intermediate DecimalBlock, like the OutputSlot of vectorized expression
     */
    private void allocToNormal() {
        if (isUnalloc()) {
            allocateNormalDecimal();
        } else if (isDecimal64() || isDecimal128()) {
            convertToNormal();
        }
    }

    /**
     * dangerous!
     * can only be invoked by intermediate DecimalBlock, like the OutputSlot of vectorized expression
     */
    private void allocToDecimal128() {
        if (isUnalloc()) {
            allocateDecimal128();
            return;
        }
        if (isDecimal64()) {
            if (this.decimal64Values != null) {
                this.decimal128HighValues = new long[positionCount];
                for (int i = 0; i < decimal64Values.length; i++) {
                    decimal128HighValues[i] = decimal64Values[i] < 0 ? -1 : 0;
                }
            }
            this.state = DECIMAL_128;
            updateSizeInfo();
            return;
        }
        if (isDecimal128()) {
            return;
        }
        throw new IllegalStateException("Can not convert " + state + "to DECIMAL_128");
    }

    void allocateNormalDecimal() {
        this.memorySegments = Slices.allocate(positionCount * DECIMAL_MEMORY_SIZE);
        this.state = DecimalBlockState.UNSET_STATE;
        updateSizeInfo();
    }

    public Slice getRegion(int position) {
        position = realPositionOf(position);
        return getRegionInner(position);
    }

    public Slice getRegion(int position, Slice output) {
        position = realPositionOf(position);
        return getRegionInner(position, output);
    }

    /**
     * convert Decimal64 to normal decimal memory segment
     */
    private void convertToNormal() {
        if ((!isDecimal64() && !isDecimal128()) || this.memorySegments != null) {
            return;
        }
        SliceOutput sliceOutput = new DynamicSliceOutput(positionCount * DECIMAL_MEMORY_SIZE);
        DecimalStructure buffer = new DecimalStructure();
        DecimalStructure result = new DecimalStructure();
        if (isDecimal64() && decimal64Values != null) {
            // handle decimal64 values
            for (int pos = 0; pos < decimal64Values.length; pos++) {
                if (!isNullInner(pos)) {
                    long decimal64 = decimal64Values[pos];
                    if (decimal64 == 0) {
                        sliceOutput.writeBytes(Decimal.ZERO.getMemorySegment());
                    } else {
                        FastDecimalUtils.setLongWithScale(buffer, result, decimal64, dataType.getScale());
                        sliceOutput.writeBytes(buffer.getDecimalMemorySegment());
                        buffer.reset();
                        result.reset();
                    }
                } else {
                    sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
                }
            }
        } else if (isDecimal128() && decimal64Values != null && decimal128HighValues != null) {
            // handle decimal128 values
            for (int pos = 0; pos < decimal64Values.length; pos++) {
                if (!isNullInner(pos)) {
                    long decimal128Low = decimal64Values[pos];
                    long decimal128High = decimal128HighValues[pos];
                    if (decimal128Low == 0 && decimal128High == 0) {
                        sliceOutput.writeBytes(Decimal.ZERO.getMemorySegment());
                    } else {
                        FastDecimalUtils.setDecimal128WithScale(result, buffer,
                            decimal128Low, decimal128High, getScale());
                        sliceOutput.writeBytes(buffer.getDecimalMemorySegment());
                        buffer.reset();
                        result.reset();
                    }
                } else {
                    sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
                }
            }
        }

        this.memorySegments = sliceOutput.slice();
        this.decimal64Values = null;
        this.decimal128HighValues = null;
        this.state = DecimalBlockState.FULL;
        updateSizeInfo();
    }

    @Override
    public void compact(int[] selection) {
        if (selection == null) {
            return;
        }
        int compactedSize = selection.length;
        int index = 0;
        allocToNormal();
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

    private boolean isNullInner(int position) {
        return isNull != null && isNull[position + arrayOffset];
    }

    private Decimal getDecimalInner(int position) {
        if (isDecimal64()) {
            return new Decimal(decimal64Values[position], dataType.getScale());
        } else if (isDecimal128()) {
            long decimal128Low = decimal64Values[position];
            long decimal128High = decimal128HighValues[position];
            DecimalStructure decimalStructure = new DecimalStructure();
            DecimalStructure tmpBuffer = getRegionTmpBuffer();
            FastDecimalUtils.setDecimal128WithScale(tmpBuffer, decimalStructure,
                decimal128Low, decimal128High, getScale());
            return new Decimal(decimalStructure);
        } else {
            Slice memorySegment = getMemorySegments().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
            return new Decimal(memorySegment);
        }
    }

    private Decimal getDecimalInner(int position, Slice output) {
        if (isDecimal64()) {
            DecimalStructure decimalStructure = new DecimalStructure(output);
            decimalStructure.setLongWithScale(decimal64Values[position], dataType.getScale());
            return new Decimal(decimalStructure);
        } else if (isDecimal128()) {
            long decimal128Low = decimal64Values[position];
            long decimal128High = decimal128HighValues[position];
            DecimalStructure decimalStructure = new DecimalStructure(output);
            DecimalStructure tmpBuffer = getRegionTmpBuffer();
            FastDecimalUtils.setDecimal128WithScale(tmpBuffer, decimalStructure,
                decimal128Low, decimal128High, getScale());
            return new Decimal(decimalStructure);
        } else {
            Slice memorySegment =
                getMemorySegments().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE, output);
            return new Decimal(memorySegment);
        }
    }

    private long getLongInner(int position) {
        if (isDecimal64()) {
            return decimal64Values[position];
        } else {
            throw new IllegalStateException("Cannot get long from DecimalBlock with state: " + state);
        }
    }

    private long getDecimal128LowInner(int position) {
        if (isDecimal128()) {
            return decimal64Values[position];
        } else {
            throw new IllegalStateException("Cannot get decimal128Low from DecimalBlock with state: " + state);
        }
    }

    private long getDecimal128HighInner(int position) {
        if (isDecimal128()) {
            return decimal128HighValues[position];
        } else {
            throw new IllegalStateException("Cannot get decimal128High from DecimalBlock with state: " + state);
        }
    }

    private long getDecimal128LowUncheckInner(int position) {
        return decimal64Values[position];
    }

    private long getDecimal128HighUncheckInner(int position) {
        return decimal128HighValues[position];
    }

    private boolean equalsInner(int realPosition, Block other, int otherPosition) {
        boolean n1 = isNullInner(realPosition);
        boolean n2 = other.isNull(otherPosition);
        if (n1 && n2) {
            return true;
        } else if (n1 != n2) {
            return false;
        }

        if (!(other instanceof SegmentedDecimalBlock)) {
            throw new AssertionError("Failed to compare with " + other.getClass().getName());
        }
        SegmentedDecimalBlock otherBlock = (SegmentedDecimalBlock) other;
        if (isDecimal64()) {
            return decimal64EqualsInner(realPosition, otherBlock, otherPosition);
        }
        if (isDecimal128()) {
            return decimal128EqualsInner(realPosition, otherBlock, otherPosition);
        }

        // normal decimal compare
        Slice memorySegment1 = this.segmentUncheckedAtInner(realPosition);
        Slice memorySegment2 = otherBlock.segmentUncheckedAt(otherPosition);
        return RawBytesDecimalUtils.equals(memorySegment1, memorySegment2);
    }

    private boolean decimal64EqualsInner(int realPosition, SegmentedDecimalBlock otherBlock, int otherPosition) {
        if (otherBlock.isDecimal64()) {
            long val = getLongInner(realPosition);
            long otherVal = otherBlock.getLong(otherPosition);
            int scale = getScale();
            int otherScale = otherBlock.getScale();
            int scaleDiff = scale - otherScale;
            if (scaleDiff == 0) {
                return val == otherVal;
            }
            if ((val >= 0 && otherVal < 0) || (val < 0 && otherVal >= 0)) {
                return false;
            }
            int scaleDiffAbs = Math.abs(scaleDiff);
            if (scaleDiffAbs < DecimalTypeBase.POW_10.length) {
                long power = DecimalTypeBase.POW_10[scaleDiffAbs];
                // it does not matter if this overflows
                if (scaleDiff > 0) {
                    otherVal *= power;
                } else {
                    val *= power;
                }
                return val == otherVal;
            }
            // rare case for large scaleDiff, just fallback to normal compare
        } else if (otherBlock.isDecimal128()) {
            if (getScale() == otherBlock.getScale()) {
                long thisDecimal64 = getLongInner(realPosition);
                long thatLow = otherBlock.getDecimal128Low(otherPosition);
                long thatHigh = otherBlock.getDecimal128High(otherPosition);
                if (thisDecimal64 != thatLow) {
                    return false;
                }
                return thisDecimal64 >= 0 ? (thatHigh == 0) : (thatHigh == -1);
            }
        }

        // fallback to normal decimal compare
        Slice memorySegment1 = this.segmentUncheckedAtInner(realPosition);
        Slice memorySegment2 = otherBlock.segmentUncheckedAt(otherPosition);
        return RawBytesDecimalUtils.equals(memorySegment1, memorySegment2);
    }

    private boolean decimal128EqualsInner(int realPosition, SegmentedDecimalBlock otherBlock, int otherPosition) {
        if (otherBlock.isDecimal64()) {
            if (getScale() == otherBlock.getScale()) {
                long thisLow = decimal64Values[realPosition];
                long thisHigh = decimal128HighValues[realPosition];
                long thatDecimal64 = otherBlock.getLong(otherPosition);
                if (thisLow != thatDecimal64) {
                    return false;
                }
                return thatDecimal64 >= 0 ? (thisHigh == 0) : (thisHigh == -1);
            }
        } else if (otherBlock.isDecimal128()) {
            if (getScale() == otherBlock.getScale()) {
                return decimal64Values[realPosition] == otherBlock.getDecimal128Low(otherPosition) &&
                    decimal128HighValues[realPosition] == otherBlock.getDecimal128High(otherPosition);
            }
        }

        // fallback to normal decimal compare
        Slice memorySegment1 = this.segmentUncheckedAtInner(realPosition);
        Slice memorySegment2 = otherBlock.segmentUncheckedAt(otherPosition);
        return RawBytesDecimalUtils.equals(memorySegment1, memorySegment2);
    }

    private int hashCodeInner(int position) {
        if (isNullInner(position)) {
            return 0;
        }
        if (isDecimal64()) {
            long val = getLongInner(position);
            int hashCode = RawBytesDecimalUtils.hashCode(val, getScale());
            if (hashCode != 0) {
                return hashCode;
            }
            // fallback
            DecimalStructure bufferDec = getHashCodeTmpBuffer();
            DecimalStructure resultDec = getHashCodeResultBuffer();
            FastDecimalUtils.setLongWithScale(bufferDec, resultDec, val, getScale());
            return RawBytesDecimalUtils.hashCode(resultDec.getDecimalMemorySegment());
        } else if (isDecimal128()) {
            long decimal128Low = getDecimal128LowInner(position);
            long decimal128High = getDecimal128HighInner(position);
            DecimalStructure bufferDec = getHashCodeTmpBuffer();
            DecimalStructure resultDec = getHashCodeResultBuffer();
            FastDecimalUtils.setDecimal128WithScale(bufferDec, resultDec, decimal128Low, decimal128High, getScale());
            return RawBytesDecimalUtils.hashCode(resultDec.getDecimalMemorySegment());
        } else {
            Slice memorySegment = memorySegments.slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
            return RawBytesDecimalUtils.hashCode(memorySegment);
        }
    }

    private Slice segmentUncheckedAtInner(int position) {
        if (isDecimal64()) {
            long decimal64 = decimal64Values[position];
            return new Decimal(decimal64, getScale()).getMemorySegment();
        }
        if (isDecimal128()) {
            long decimal128Low = decimal64Values[position];
            long decimal128High = decimal128HighValues[position];
            DecimalStructure bufferDec = getRegionTmpBuffer();
            DecimalStructure resultDec = new DecimalStructure();
            FastDecimalUtils.setDecimal128WithScale(bufferDec, resultDec, decimal128Low, decimal128High, getScale());
            return resultDec.getDecimalMemorySegment();
        }
        return getMemorySegments().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
    }

    private void addToHasherInner(IStreamingHasher sink, int position) {
        if (isNullInner(position)) {
            sink.putInt(NULL_VALUE);
        } else {
            sink.putInt(hashCodeInner(position));
        }
    }

    private Object getElementAtUncheckedInner(int position) {
        if (isDecimal64()) {
            long decimal64 = decimal64Values[position];
            return new Decimal(decimal64, getScale());
        }
        if (isDecimal128()) {
            long decimal128Low = decimal64Values[position];
            long decimal128High = decimal128HighValues[position];
            DecimalStructure bufferDec = getRegionTmpBuffer();
            DecimalStructure resultDec = new DecimalStructure();
            FastDecimalUtils.setDecimal128WithScale(bufferDec, resultDec, decimal128Low, decimal128High, getScale());
            return new Decimal(resultDec);
        }
        // slice a memory segment in 64 bytes and build a decimal value.
        int fromIndex = position * DECIMAL_MEMORY_SIZE;
        Slice decimalMemorySegment = getMemorySegments().slice(fromIndex, DECIMAL_MEMORY_SIZE);
        return new Decimal(decimalMemorySegment);
    }

    /**
     * considered as a low-frequency path in VectorizedExpression
     */
    private void setElementAtInner(final int position, Object element) {
        if (element == null) {
            isNull[position] = true;
            hasNull = true;
            return;
        }
        Decimal decimal = (Decimal) element;
        boolean elementIsDec64 = DecimalConverter.isDecimal64(decimal);
        if (isUnalloc()) {
            allocateValues(elementIsDec64);
        }
        isNull[position] = false;
        if (isDecimal64() && elementIsDec64) {
            // set a decimal64 inside a Decimal64 Block
            // make sure that the scales are the same
            if (decimal.scale() == getScale()) {
                Decimal tmpDecimal = new Decimal();
                FastDecimalUtils.shift(decimal.getDecimalStructure(), tmpDecimal.getDecimalStructure(),
                    decimal.scale());
                long decimal64 = tmpDecimal.longValue();
                decimal64Values[position] = decimal64;
                return;
            }
        }
        if (isDecimal128() && elementIsDec64) {
            // set a decimal64 inside a Decimal128 Block
            // make sure that the scales are the same
            if (decimal.scale() == getScale()) {
                Decimal tmpDecimal = new Decimal();
                FastDecimalUtils.shift(decimal.getDecimalStructure(), tmpDecimal.getDecimalStructure(),
                    decimal.scale());
                long decimal64 = tmpDecimal.longValue();
                decimal64Values[position] = decimal64;
                decimal128HighValues[position] = decimal64 < 0 ? -1 : 0;
                return;
            }
        }
        // set a normal decimal inside a DecimalBlock
        Slice decimalMemorySegment = decimal.getMemorySegment();

        // copy memory from specified position in size of 64 bytes
        int fromIndex = position * DECIMAL_MEMORY_SIZE;
        getMemorySegments().setBytes(fromIndex, decimalMemorySegment);
    }

    private Slice getRegionInner(int position) {
        if (isDecimal64()) {
            long val = decimal64Values[position];
            DecimalStructure decimalStructure = new DecimalStructure();
            DecimalStructure tmpBuffer = getRegionTmpBuffer();
            FastDecimalUtils.setLongWithScale(tmpBuffer, decimalStructure, val, getScale());
            return decimalStructure.getDecimalMemorySegment();
        }
        if (isDecimal128()) {
            long decimal128Low = decimal64Values[position];
            long decimal128High = decimal128HighValues[position];
            DecimalStructure decimalStructure = new DecimalStructure();
            DecimalStructure tmpBuffer = getRegionTmpBuffer();
            FastDecimalUtils.setDecimal128WithScale(tmpBuffer, decimalStructure,
                decimal128Low, decimal128High, getScale());
            return decimalStructure.getDecimalMemorySegment();
        }
        return getMemorySegments().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
    }

    private Slice getRegionInner(int position, Slice output) {
        if (isDecimal64()) {
            long val = decimal64Values[position];
            DecimalStructure decimalStructure = new DecimalStructure(output);
            DecimalStructure tmpBuffer = getRegionTmpBuffer();
            FastDecimalUtils.setLongWithScale(tmpBuffer, decimalStructure, val, getScale());
            return decimalStructure.getDecimalMemorySegment();
        }
        if (isDecimal128()) {
            long decimal128Low = decimal64Values[position];
            long decimal128High = decimal128HighValues[position];
            DecimalStructure decimalStructure = new DecimalStructure(output);
            DecimalStructure tmpBuffer = getRegionTmpBuffer();
            FastDecimalUtils.setDecimal128WithScale(tmpBuffer, decimalStructure,
                decimal128Low, decimal128High, getScale());
            return decimalStructure.getDecimalMemorySegment();
        }
        return getMemorySegments().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE, output);
    }

    @Override
    public void updateSizeInfo() {
        if (isUnalloc()) {
            // maybe not allocated yet, or all nulls
            estimatedSize = INSTANCE_SIZE + sizeOf(isNull);
            elementUsedBytes = Byte.BYTES * positionCount;
            return;
        }
        if (isDecimal64()) {
            estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(decimal64Values);
            elementUsedBytes = Byte.BYTES * positionCount + Long.BYTES * positionCount;
        } else if (isDecimal128()) {
            estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + sizeOf(decimal64Values) + sizeOf(decimal128HighValues);
            elementUsedBytes = Byte.BYTES * positionCount + Long.BYTES * positionCount * 2;
        } else {
            estimatedSize = INSTANCE_SIZE + sizeOf(isNull) + memorySegments.length();
            elementUsedBytes = Byte.BYTES * positionCount + DECIMAL_MEMORY_SIZE * positionCount;
        }
    }

    public void collectDecimalInfo() {
        // recollect decimal state info util state is not UNSET_STATE anymore.
        if (this.state != DecimalBlockState.UNSET_STATE) {
            return;
        }
        DecimalBlockState resultState = DecimalBlockState.UNSET_STATE;
        for (int position = 0; position < positionCount; position++) {
            position = realPositionOf(position);
            if (!isNullInner(position)) {
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
        position = realPositionOf(position);
        return (!state.isSimple() || state.getInt1Pos() == UNSET) ? 0 :
            getMemorySegments().getInt(position * DECIMAL_MEMORY_SIZE + state.getInt1Pos() * 4);
    }

    public int fastInt2(int position) {
        position = realPositionOf(position);
        return (!state.isSimple() || state.getInt2Pos() == UNSET) ? 0 :
            getMemorySegments().getInt(position * DECIMAL_MEMORY_SIZE + state.getInt2Pos() * 4);
    }

    public int fastFrac(int position) {
        position = realPositionOf(position);
        return (!state.isSimple() || state.getFracPos() == UNSET) ? 0 :
            getMemorySegments().getInt(position * DECIMAL_MEMORY_SIZE + state.getFracPos() * 4);
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

    /**
     * For performance consideration, when this is a decimal64 block,
     * the caller should obtain the long array through getDecimal64Values()
     */
    @Override
    public boolean isDecimal64() {
        return state.isDecimal64();
    }

    /**
     * For performance consideration, when this is a decimal128 block,
     * the caller should obtain the lowBits array and the highBits array
     * through getDecimal128LowValues() and getDecimal128HighValues()
     */
    @Override
    public boolean isDecimal128() {
        return state.isDecimal128();
    }

    public Slice allocCachedSlice() {
        Slice cachedSlice;
        if (isDecimal64() || isDecimal128()) {
            cachedSlice = DecimalStructure.allocateDecimalSlice();
        } else {
            cachedSlice = new Slice();
        }
        return cachedSlice;
    }

    public boolean isUnalloc() {
        return state == UNALLOC_STATE;
    }

    @Override
    public int getScale() {
        return dataType.getScale();
    }

    // note: dangerous!
    public void setMultiResult1(int position, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, sum0);
        memorySegments.setInt(index + 4, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);

        this.state = this.state.merge(SIMPLE_MODE_2);
    }

    // note: dangerous!
    public void setMultiResult2(int position, int carry0, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, carry0);
        memorySegments.setInt(index + 4, sum0);
        memorySegments.setInt(index + 8, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);

        this.state = this.state.merge(SIMPLE_MODE_3);
    }

    // note: dangerous!
    public void setMultiResult3(int position, int sum0, int sum9, int sum18) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, sum0);
        memorySegments.setInt(index + 4, sum9);
        memorySegments.setInt(index + 8, sum18);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);

        this.state = this.state.merge(FULL);
    }

    // note: dangerous!
    public void setMultiResult4(int position, int carry0, int sum0, int sum9, int sum18) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, carry0);
        memorySegments.setInt(index + 4, sum0);
        memorySegments.setInt(index + 8, sum9);
        memorySegments.setInt(index + 12, sum18);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 18);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);

        this.state = this.state.merge(FULL);
    }

    // note: dangerous!
    public void setSubResult1(int position, int sub0, int sub9, boolean isNeg) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, sub0);
        memorySegments.setInt(index + 4, sub9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, isNeg ? 1 : 0);

        this.state = SIMPLE_MODE_2;
    }

    // note: dangerous!
    public void setSubResult2(int position, int carry, int sub0, int sub9, boolean isNeg) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, carry);
        memorySegments.setInt(index + 4, sub0);
        memorySegments.setInt(index + 8, sub9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, isNeg ? 1 : 0);

        this.state = this.state.merge(SIMPLE_MODE_3);
    }

    // note: dangerous!
    public void setAddResult1(int position, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, sum0);
        memorySegments.setInt(index + 4, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 9);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);

        this.state = this.state.merge(SIMPLE_MODE_2);
    }

    // note: dangerous!
    public void setAddResult2(int position, int carry, int sum0, int sum9) {
        int index = realPositionOf(position) * DECIMAL_MEMORY_SIZE;
        Slice memorySegments = getMemorySegments();
        memorySegments.setInt(index, carry);
        memorySegments.setInt(index + 4, sum0);
        memorySegments.setInt(index + 8, sum9);
        memorySegments.setByte(index + INTEGERS_OFFSET, 18);
        memorySegments.setByte(index + FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + DERIVED_FRACTIONS_OFFSET, 9);
        memorySegments.setByte(index + IS_NEG_OFFSET, 0);

        this.state = this.state.merge(SIMPLE_MODE_3);
    }

    // note: dangerous!
    public void setFullState() {
        this.state = FULL;
    }

    // note: dangerous!
    public long[] getDecimal64Values() {
        return decimal64Values;
    }

    // note: dangerous!
    public long[] getDecimal128LowValues() {
        return decimal64Values;
    }

    // note: dangerous!
    public long[] getDecimal128HighValues() {
        return decimal128HighValues;
    }
}
