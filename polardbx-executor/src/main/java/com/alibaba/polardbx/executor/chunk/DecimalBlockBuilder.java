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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;

/**
 * Decimal block builder
 */
public class DecimalBlockBuilder extends AbstractBlockBuilder {
    SliceOutput sliceOutput;

    DataType decimalType;
    private boolean isUnset;
    private boolean isSimple;
    private int intWord;
    private int fracWord;

    public DecimalBlockBuilder(int capacity, DataType decimalType) {
        super(capacity);
        this.sliceOutput = new DynamicSliceOutput(capacity * DECIMAL_MEMORY_SIZE);
        this.decimalType = decimalType;
        this.isUnset = true;
        this.isSimple = false;
        this.intWord = -1;
        this.fracWord = -1;
    }

    public DecimalBlockBuilder(int capacity) {
        this(capacity, DataTypes.DecimalType);
    }

    @Override
    public void writeDecimal(Decimal value) {
        valueIsNull.add(false);
        sliceOutput.writeBytes(value.getMemorySegment());
        updateDecimalInfo(value.getDecimalStructure());
    }

    public void writeDecimalBin(byte[] bytes, DataType dataType) {
        // binary -> decimal
        DecimalStructure d2 = new DecimalStructure();
        DecimalConverter.binToDecimal(bytes, d2, dataType.getPrecision(), dataType.getScale());
        valueIsNull.add(false);
        sliceOutput.writeBytes(d2.getDecimalMemorySegment());
        updateDecimalInfo(d2);
    }

    public void writeDecimalBin(byte[] bytes) {
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
        return;
    }

    @Override
    public void appendNull() {
        appendNullInternal();
        // If null value, just skip 64-bytes
        sliceOutput.skipBytes(DECIMAL_MEMORY_SIZE);
    }

    @Override
    public Decimal getDecimal(int position) {
        checkReadablePosition(position);
        Slice segment = sliceOutput.slice().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
        return new Decimal(segment);
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
        Preconditions.checkArgument(value instanceof Decimal);
        writeDecimal((Decimal) value);
    }

    @Override
    public void ensureCapacity(int capacity) {
        super.ensureCapacity(capacity);
        // Ignore bytes stored.
        sliceOutput.ensureCapacity(capacity * DECIMAL_MEMORY_SIZE);
    }

    @Override
    public Block build() {
        int int1Pos = -1, int2Pos = -1, fracPos = -1;
        if (isSimple) {
            if (intWord == 0) {
                int2Pos = -1;
                int1Pos = -1;
                fracPos = 0;
            } else if (intWord == 1) {
                int2Pos = -1;
                int1Pos = 0;
                fracPos = 1;
            } else if (intWord == 2) {
                int2Pos = 0;
                int1Pos = 1;
                fracPos = 2;
            }
        } else {
            isSimple = false;
            int1Pos = -1;
            int2Pos = -1;
            fracPos = -1;
        }
        return new DecimalBlock(getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
            sliceOutput.slice(), isSimple, int1Pos, int2Pos, fracPos);
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

    Slice segmentUncheckedAt(int position) {
        return sliceOutput.slice().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
    }

    private void updateDecimalInfo(DecimalStructure d) {
        if (!isUnset && !isSimple) {
            // decimal info are not consistent
            return;
        }
        int currentIntWord = DecimalTypeBase.roundUp(d.getIntegers());
        int currentFracWord = DecimalTypeBase.roundUp(d.getFractions());
        if (isUnset) {
            isSimple = (currentFracWord == 0 || currentIntWord == 1 || currentIntWord == 2) && currentFracWord == 1;
            intWord = currentIntWord;
            fracWord = currentFracWord;
            isUnset = false;
        } else {
            isSimple = intWord == currentIntWord && fracWord == currentFracWord;
        }
    }

    public boolean isSimple() {
        return isSimple;
    }

    public void setSimple(boolean simple) {
        isSimple = simple;
    }

    public int getIntWord() {
        return intWord;
    }

    public void setIntWord(int intWord) {
        this.intWord = intWord;
    }

    public int getFracWord() {
        return fracWord;
    }

    public void setFracWord(int fracWord) {
        this.fracWord = fracWord;
    }

    public void setDecimalType(DataType decimalType) {
        this.decimalType = decimalType;
    }

    public DataType getDecimalType() {
        return decimalType;
    }
}

