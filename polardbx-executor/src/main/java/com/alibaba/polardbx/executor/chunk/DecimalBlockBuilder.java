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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.chunk.DecimalBlock.DecimalBlockState.*;

/**
 * Decimal block builder
 */
public class DecimalBlockBuilder extends AbstractBlockBuilder {
    SliceOutput sliceOutput;
    DataType decimalType;

    // collect state of decimal values.
    DecimalBlock.DecimalBlockState state;

    public DecimalBlockBuilder(int capacity, DataType decimalType) {
        super(capacity);
        this.sliceOutput = new DynamicSliceOutput(capacity * DECIMAL_MEMORY_SIZE);
        this.decimalType = decimalType;

        this.state = UNSET_STATE;
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
        return new DecimalBlock(getPositionCount(), mayHaveNull() ? valueIsNull.elements() : null,
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

    Slice segmentUncheckedAt(int position) {
        return sliceOutput.slice().slice(position * DECIMAL_MEMORY_SIZE, DECIMAL_MEMORY_SIZE);
    }

    private void updateDecimalInfo(DecimalStructure d) {
        DecimalBlock.DecimalBlockState elementState = DecimalBlock.DecimalBlockState.stateOf(d);
        this.state = this.state.merge(elementState);
    }

    public DecimalBlock.DecimalBlockState getState() {
        return this.state;
    }

    public void setDecimalType(DataType decimalType) {
        this.decimalType = decimalType;
    }

    public DataType getDecimalType() {
        return decimalType;
    }
}

