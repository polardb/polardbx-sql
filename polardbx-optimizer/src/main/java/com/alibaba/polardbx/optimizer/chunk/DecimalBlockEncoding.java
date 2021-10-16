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
package com.alibaba.polardbx.optimizer.chunk;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.encodeNullsAsBits;

public class DecimalBlockEncoding implements BlockEncoding {

    private static final String NAME = "DECIMAL";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int nullsCnt = encodeNullsAsBits(sliceOutput, block);

        DecimalBlock b = (DecimalBlock) block;

        // Mark if all values are null.
        boolean existNonNull;
        sliceOutput.writeBoolean(existNonNull = positionCount > nullsCnt);
        if (existNonNull) {
            // write slice length and bytes
            Slice decimalMemorySegments = b.getMemorySegments();
            sliceOutput.writeInt(decimalMemorySegments.length());
            sliceOutput.writeBytes(decimalMemorySegments);
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);
        boolean existNonNull = sliceInput.readBoolean();

        Slice slice = Slices.allocate(positionCount * DECIMAL_MEMORY_SIZE);

        if (existNonNull) {
            int length = sliceInput.readInt();
            sliceInput.readBytes(slice, 0, length);
            slice = slice.slice(0, length);
        }

        return new DecimalBlock(0, positionCount, valueIsNull, slice);
    }
}
