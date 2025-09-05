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
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.DECIMAL_128;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.DECIMAL_64;
import static com.alibaba.polardbx.executor.chunk.SegmentedDecimalBlock.DecimalBlockState.UNSET_STATE;

/**
 * Decimal block builder
 */
public class DecimalBlockBuilder extends AbstractBlockBuilder implements SegmentedDecimalBlock {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalBlockBuilder.class).instanceSize();

    SliceOutput sliceOutput;

    MemoryCountableLongArrayList decimal64List;
    MemoryCountableLongArrayList decimal128HighList;

    @FieldMemoryCounter(value = false)
    DecimalType decimalType;

    /**
     * UNSET / DECIMAL_64 / DECIMAL_128 /
     */
    DecimalBlockState state;

    private int scale;

    private DecimalStructure decimalBuffer;
    private DecimalStructure decimalResult;

    public DecimalBlockBuilder(int capacity, DataType type) {
        super(capacity);
        this.decimalType = (DecimalType) type;
        this.scale = decimalType.getScale();

        if (decimalType.isDecimal64()) {
            initDecimal64List();
        } else {
            initSliceOutput();
        }
        this.state = UNSET_STATE;
    }

    public DecimalBlockBuilder(int capacity) {
        this(capacity, DataTypes.DecimalType);
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(valueIsNull)
            + FastMemoryCounter.sizeOf(sliceOutput)
            + FastMemoryCounter.sizeOf(decimal64List)
            + FastMemoryCounter.sizeOf(decimal128HighList)
            + (state == null ? 0 : state.memorySize())
            + FastMemoryCounter.sizeOf(decimalBuffer)
            + FastMemoryCounter.sizeOf(decimalResult);
    }

    public void initSliceOutput() {
        if (this.sliceOutput == null) {
            this.sliceOutput = new DynamicSliceOutput(initialCapacity * DECIMAL_MEMORY_SIZE);
        }
    }

    private void initDecimal64List() {
        if (this.decimal64List == null) {
            this.decimal64List = new MemoryCountableLongArrayList(initialCapacity);
        }
    }

    private void initDecimal128List() {
        if (this.decimal64List == null) {
            this.decimal64List = new MemoryCountableLongArrayList(initialCapacity);
        }
        if (this.decimal128HighList == null) {
            this.decimal128HighList = new MemoryCountableLongArrayList(initialCapacity);
        }
    }

    @Override
    public void writeDecimal(Decimal value) {
        convertToNormalDecimal();

        valueIsNull.add(false);
        sliceOutput.writeBytes(value.getMemorySegment());

        updateDecimalInfo(value.getDecimalStructure());
    }

    @Override
    public void writeLong(long value) {
        if (state.isUnset()) {
            initDecimal64List();
            state = DECIMAL_64;
        } else if (!state.isDecimal64Or128()) {
            writeDecimal(new Decimal(value, decimalType.getScale()));
            return;
        }

        valueIsNull.add(false);
        decimal64List.add(value);
        if (state.isDecimal128()) {
            decimal128HighList.add(value < 0 ? -1 : 0);
        }
    }

    public void writeDecimal128(long low, long high) {
        if (state.isUnset()) {
            initDecimal128List();
            for (int i = 0; i < valueIsNull.size(); i++) {
                decimal128HighList.add(0);
            }
            state = DECIMAL_128;
        } else if (state.isDecimal64()) {
            // convert decimal64 to decimal128
            initDecimal128List();
            state = DECIMAL_128;
            for (int i = 0; i < valueIsNull.size(); i++) {
                if (decimal64List != null && decimal64List.getLong(i) < 0) {
                    decimal128HighList.add(-1);
                } else {
                    decimal128HighList.add(0);
                }
            }
        } else if (!state.isDecimal128()) {
            // normal decimal
            DecimalStructure buffer = getDecimalBuffer();
            DecimalStructure result = getDecimalResult();
            FastDecimalUtils.setDecimal128WithScale(buffer, result, low, high, scale);
            valueIsNull.add(false);
            sliceOutput.writeBytes(result.getDecimalMemorySegment());
            updateDecimalInfo(result);
            return;
        }

        valueIsNull.add(false);
        decimal64List.add(low);
        decimal128HighList.add(high);
    }

    public void writeDecimalBin(byte[] bytes) {
        convertToNormalDecimal();
        // binary -> decimal
        DecimalStructure d2 = new DecimalStructure();
        DecimalConverter.binToDecimal(bytes, d2, decimalType.getPrecision(), decimalType.getScale());

        valueIsNull.add(false);
        sliceOutput.writeBytes(d2.getDecimalMemorySegment());

        updateDecimalInfo(d2);
    }

    @Override
    public void writeByteArray(byte[] value) {
        writeByteArray(value, 0, value.length);
    }

    @Override
    public void writeByteArray(byte[] value, int offset, int length) {
        DecimalStructure d = new DecimalStructure();
        DecimalConverter.parseString(value, offset, length, d, false);
        writeDecimal(new Decimal(d));
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        if (isDecimal64() || isUnset()) {
            initDecimal64List();
            decimal64List.add(0L);
        } else if (isDecimal128()) {
            decimal64List.add(0L);
            decimal128HighList.add(0L);
        } else {
            // normal decimal
            // If null value, just skip 64-bytes
            sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
        }
    }

    @Override
    public Decimal getDecimal(int position) {
        checkReadablePosition(position);
        if (state.isDecimal64()) {
            return new Decimal(getLong(position), scale);
        }
        if (state.isDecimal128()) {
            DecimalStructure buffer = getDecimalBuffer();
            DecimalStructure result = getDecimalResult();
            FastDecimalUtils.setDecimal128WithScale(buffer, result,
                decimal64List.getLong(position), decimal128HighList.getLong(position), scale);
            return new Decimal(result);
        }
        Slice segment = sliceOutput.slice().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
        return new Decimal(segment);
    }

    @Override
    public long getLong(int position) {
        checkDecimal64StoreType();
        checkReadablePosition(position);
        return decimal64List.getLong(position);
    }

    @Override
    public long getDecimal128Low(int position) {
        checkDecimal128StoreType();
        checkReadablePosition(position);
        return decimal64List.getLong(position);
    }

    @Override
    public long getDecimal128High(int position) {
        checkDecimal128StoreType();
        checkReadablePosition(position);
        return decimal128HighList.getLong(position);
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public Object getObject(int position) {
        return isNull(position) ? null : getDecimal(position);
    }

    @Override
    public void writeObject(Object value) {
        if (value == null) {
            appendNull();
            return;
        }
        checkNormalDecimalType();
        Preconditions.checkArgument(value instanceof Decimal);
        writeDecimal((Decimal) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        if (isDecimal64()) {
            decimal64List.ensureCapacity(capacity);
        } else if (isDecimal128()) {
            decimal64List.ensureCapacity(capacity);
            decimal128HighList.ensureCapacity(capacity);
        } else {
            // Ignore bytes stored.
            sliceOutput.ensureCapacity(capacity * DECIMAL_MEMORY_SIZE);
        }
    }

    @Override
    public Block build() {
        if (isDecimal64()) {
            return new DecimalBlock(decimalType, getPositionCount(), mayHaveNull(),
                mayHaveNull() ? valueIsNull.elements() : null,
                decimal64List.elements());
        }
        if (isDecimal128()) {
            return DecimalBlock.buildDecimal128Block(decimalType, getPositionCount(), mayHaveNull(),
                mayHaveNull() ? valueIsNull.elements() : null,
                decimal64List.elements(), decimal128HighList.elements());
        }
        convertToNormalDecimal();
        return new DecimalBlock(decimalType, getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            sliceOutput.slice(), state);
    }

    @Override
    public BlockBuilder newBlockBuilder() {
        return new DecimalBlockBuilder(getCapacity(), decimalType);
    }

    @Override
    public int hashCode(int position) {
        if (isNull(position)) {
            return 0;
        }
        return getDecimal(position).hashCode();
    }

    @Override
    public Slice segmentUncheckedAt(int position) {
        return sliceOutput.slice().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
    }

    private void updateDecimalInfo(DecimalStructure d) {
        DecimalBlock.DecimalBlockState elementState = DecimalBlock.DecimalBlockState.stateOf(d);
        this.state = this.state.merge(elementState);
    }

    public DecimalBlock.DecimalBlockState getState() {
        return this.state;
    }

    public DataType getDecimalType() {
        return decimalType;
    }

    @Override
    public boolean isDecimal64() {
        return state.isDecimal64() || (state.isUnset() && decimalType.isDecimal64());
    }

    @Override
    public boolean isDecimal128() {
        return state.isDecimal128();
    }

    public boolean isNormal() {
        return state.isNormal();
    }

    public void convertToNormalDecimal() {
        initSliceOutput();

        if (isNormal()) {
            return;
        }

        // unset 或 decimal64/decimal128 状态
        if (decimal64List != null && !decimal64List.isEmpty()) {
            if (state.isUnset()) {
                for (int pos = 0; pos < decimal64List.size(); pos++) {
                    if (!isNull(pos)) {
                        // UNSET state expect all values are null
                        throw new IllegalStateException("Incorrect DecimalBlockBuilder state");
                    } else {
                        sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
                    }
                }
                decimal64List.clear();
            } else if (state.isDecimal64()) {
                state = UNSET_STATE;
                DecimalStructure tmpBuffer = getDecimalBuffer();
                DecimalStructure resultBuffer = getDecimalResult();
                // 可能已经有 DECIMAL64值
                for (int pos = 0; pos < decimal64List.size(); pos++) {
                    if (!isNull(pos)) {
                        long decimal64 = decimal64List.getLong(pos);
                        FastDecimalUtils.setLongWithScale(tmpBuffer, resultBuffer, decimal64, scale);
                        sliceOutput.writeBytes(resultBuffer.getDecimalMemorySegment());
                        updateDecimalInfo(resultBuffer);
                    } else {
                        sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
                    }
                }

                decimal64List.clear();
            } else if (state.isDecimal128()) {
                Preconditions.checkArgument(decimal64List.size() == decimal128HighList.size(),
                    "Decimal128 lowBits count does not match highBits count");
                state = UNSET_STATE;
                DecimalStructure tmpBuffer = getDecimalBuffer();
                DecimalStructure resultBuffer = getDecimalResult();
                for (int pos = 0; pos < decimal64List.size(); pos++) {
                    if (!isNull(pos)) {
                        long lowBits = decimal64List.getLong(pos);
                        long highBits = decimal128HighList.getLong(pos);
                        FastDecimalUtils.setDecimal128WithScale(tmpBuffer, resultBuffer, lowBits, highBits, scale);
                        sliceOutput.writeBytes(resultBuffer.getDecimalMemorySegment());
                        updateDecimalInfo(resultBuffer);
                    } else {
                        sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
                    }
                }

                decimal64List.clear();
                decimal128HighList.clear();
            }
        }
    }

    private void checkNormalDecimalType() {
        if (state.isDecimal64Or128()) {
            throw new AssertionError("DECIMAL_64 store type is inconsistent when writing a Decimal");
        }
    }

    private void checkDecimal64StoreType() {
        if (state.isUnset()) {
            state = DECIMAL_64;
        } else if (state != DECIMAL_64) {
            throw new AssertionError("Unmatched DECIMAL_64 type: " + state);
        }
    }

    private void checkDecimal128StoreType() {
        if (state != DECIMAL_128) {
            throw new AssertionError("Unmatched DECIMAL_128 type: " + state);
        }
    }

    public boolean canWriteDecimal64() {
        return state.isUnset() || state.isDecimal64();
    }

    public boolean isUnset() {
        return state.isUnset();
    }

    public boolean isSimple() {
        return state.isSimple();
    }

    public void setContainsNull(boolean containsNull) {
        this.containsNull = containsNull;
    }

    public void setScale(int scale) {
        if (this.scale == scale) {
            return;
        }
        if (state == DECIMAL_64 || state == DECIMAL_128) {
            throw new IllegalStateException("Cannot change scale after decimal64/128 is written");
        }
        this.scale = scale;
        this.decimalType = new DecimalType(this.decimalType.getPrecision(), scale);
    }

    public SliceOutput getSliceOutput() {
        return sliceOutput;
    }

    public LongArrayList getDecimal64List() {
        return decimal64List;
    }

    public LongArrayList getDecimal128LowList() {
        return decimal64List;
    }

    public LongArrayList getDecimal128HighList() {
        return decimal128HighList;
    }

    protected DecimalStructure getDecimalBuffer() {
        if (decimalBuffer == null) {
            this.decimalBuffer = new DecimalStructure();
        }
        return decimalBuffer;
    }

    protected DecimalStructure getDecimalResult() {
        if (decimalResult == null) {
            this.decimalResult = new DecimalStructure();
        }
        return decimalResult;
    }
}

