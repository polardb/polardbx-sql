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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.executor.chunk.EncoderUtil.encodeNullsAsBits;

public class DecimalBlockEncoding implements BlockEncoding {

    private static final String NAME = "DECIMAL";

    /**
     * compatible with boolean
     */
    private static final byte NORMAL_DEC = 0;
    private static final byte DEC_64 = 1;
    private static final byte DEC_128 = 2;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        DecimalBlock b = block.cast(DecimalBlock.class);
        int positionCount = b.getPositionCount();
        sliceOutput.appendInt(positionCount);

        byte decType;
        if (b.isDecimal64()) {
            decType = DEC_64;
        } else if (b.isDecimal128()) {
            decType = DEC_128;
        } else {
            decType = NORMAL_DEC;
        }
        sliceOutput.writeByte(decType);
        if (b.isDecimal64()) {
            writeDecimal64(sliceOutput, b);
        } else if (b.isDecimal128()) {
            writeDecimal128(sliceOutput, b);
        } else {
            writeDecimal(sliceOutput, b);
        }
    }

    private void writeDecimal(SliceOutput sliceOutput, DecimalBlock b) {
        int positionCount = b.getPositionCount();
        // for fast decimal
        String stateName = b.getState() == null ? "" : b.getState().name();
        sliceOutput.writeInt(stateName.length());
        sliceOutput.writeBytes(stateName.getBytes());

        int nullsCnt = encodeNullsAsBits(sliceOutput, b);

        // Mark if all values are null.
        boolean existNonNull;
        sliceOutput.writeBoolean(existNonNull = positionCount > nullsCnt);
        if (existNonNull) {
            // write slice length and bytes
            b.encoding(sliceOutput);
        }
    }

    private void writeDecimal64(SliceOutput sliceOutput, DecimalBlock b) {
        sliceOutput.writeInt(b.getScale());
        int positionCount = b.getPositionCount();

        encodeNullsAsBits(sliceOutput, b);
        for (int position = 0; position < positionCount; position++) {
            if (!b.isNull(position)) {
                sliceOutput.writeLong(b.getLong(position));
            }
        }
    }

    private void writeDecimal128(SliceOutput sliceOutput, DecimalBlock b) {
        sliceOutput.writeInt(b.getScale());
        int positionCount = b.getPositionCount();

        encodeNullsAsBits(sliceOutput, b);
        for (int position = 0; position < positionCount; position++) {
            if (!b.isNull(position)) {
                sliceOutput.writeLong(b.getDecimal128Low(position));
                sliceOutput.writeLong(b.getDecimal128High(position));
            }
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        byte decType = sliceInput.readByte();
        switch (decType) {
        case DEC_64:
            return readDecimal64(sliceInput, positionCount);
        case DEC_128:
            return readDecimal128(sliceInput, positionCount);
        case NORMAL_DEC:
            return readDecimal(sliceInput, positionCount);
        default:
            throw new IllegalStateException("Unexpected decimal block encoding type: " + decType);
        }
    }

    private Block readDecimal(SliceInput sliceInput, int positionCount) {
        // for fast decimal
        int stateNameLen = sliceInput.readInt();
        byte[] stateName = new byte[stateNameLen];
        sliceInput.readBytes(stateName);
        DecimalBlock.DecimalBlockState state = stateName.length == 0
            ? DecimalBlock.DecimalBlockState.UNSET_STATE
            : DecimalBlock.DecimalBlockState.valueOf(new String(stateName, StandardCharsets.UTF_8));

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);
        boolean existNonNull = sliceInput.readBoolean();

        Slice slice = Slices.allocate(positionCount * DECIMAL_MEMORY_SIZE);

        if (existNonNull) {
            int length = sliceInput.readInt();
            sliceInput.readBytes(slice, 0, length);
            slice = slice.slice(0, length);
        }

        return new DecimalBlock(DataTypes.DecimalType, positionCount, valueIsNull, slice, state);
    }

    private Block readDecimal64(SliceInput sliceInput, int positionCount) {
        int scale = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);
        boolean hasNull = false;

        long[] decimal64Values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (!valueIsNull[position]) {
                decimal64Values[position] = sliceInput.readLong();
            } else {
                hasNull = true;
            }
        }
        return new DecimalBlock(DecimalType.decimal64WithScale(scale), positionCount, hasNull, valueIsNull,
            decimal64Values);
    }

    private Block readDecimal128(SliceInput sliceInput, int positionCount) {
        int scale = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);
        boolean hasNull = false;

        long[] decimal128LowValues = new long[positionCount];
        long[] decimal128HighValues = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (!valueIsNull[position]) {
                decimal128LowValues[position] = sliceInput.readLong();
                decimal128HighValues[position] = sliceInput.readLong();
            } else {
                hasNull = true;
            }
        }
        return DecimalBlock.buildDecimal128Block(DecimalType.decimal128WithScale(scale), positionCount, hasNull,
            valueIsNull, decimal128LowValues, decimal128HighValues);
    }
}
