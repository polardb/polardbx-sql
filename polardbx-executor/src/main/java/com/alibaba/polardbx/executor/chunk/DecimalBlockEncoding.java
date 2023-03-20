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

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        DecimalBlock b = (DecimalBlock) block;
        int positionCount = b.getPositionCount();
        sliceOutput.appendInt(positionCount);

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

    @Override
    public Block readBlock(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

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

        return new DecimalBlock(positionCount, valueIsNull, slice, state);
    }
}
