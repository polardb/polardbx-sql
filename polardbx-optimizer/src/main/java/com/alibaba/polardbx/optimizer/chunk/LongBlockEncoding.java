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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.encodeNullsAsBits;

public class LongBlockEncoding implements BlockEncoding {
    private static final String NAME = "LONG";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeLong(block.getLong(position));
            }
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (!valueIsNull[position]) {
                values[position] = sliceInput.readLong();
            }
        }
        return new LongBlock(0, positionCount, valueIsNull, values);
    }
}
