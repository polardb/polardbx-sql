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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public final class EncoderUtil {
    public EncoderUtil() {
    }

    /**
     * Append null values for the block as a stream of bits.
     */
    @SuppressWarnings({"NarrowingCompoundAssignment", "ImplicitNumericConversion"})
    public static int encodeNullsAsBits(SliceOutput sliceOutput, Block block) {
        int nullsCnt = 0;
        int positionCount = block.getPositionCount();
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = 0;
            if (block.isNull(position)) {
                value |= 0b1000_0000;
                nullsCnt++;
            } else {
                value |= 0;
            }
            if (block.isNull(position + 1)) {
                value |= 0b0100_0000;
                nullsCnt++;
            } else {
                value |= 0;
            }

            if (block.isNull(position + 2)) {
                value |= 0b0010_0000;
                nullsCnt++;
            } else {
                value |= 0;
            }

            if (block.isNull(position + 3)) {
                value |= 0b0001_0000;
                nullsCnt++;
            } else {
                value |= 0;
            }

            if (block.isNull(position + 4)) {
                value |= 0b0000_1000;
                nullsCnt++;
            } else {
                value |= 0;
            }

            if (block.isNull(position + 5)) {
                value |= 0b0000_0100;
                nullsCnt++;
            } else {
                value |= 0;
            }

            if (block.isNull(position + 6)) {
                value |= 0b0000_0010;
                nullsCnt++;
            } else {
                value |= 0;
            }

            if (block.isNull(position + 7)) {
                value |= 0b0000_0001;
                nullsCnt++;
            } else {
                value |= 0;
            }
            sliceOutput.appendByte(value);
        }

        // write last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                if (block.isNull(position)) {
                    value |= mask;
                    nullsCnt++;
                } else {
                    value |= 0;
                }
                mask >>>= 1;
            }
            sliceOutput.appendByte(value);
        }
        return nullsCnt;
    }

    /**
     * Decode the bit stream created by encodeNullsAsBits.
     */
    public static boolean[] decodeNullBits(SliceInput sliceInput, int positionCount) {
        // read null bits 8 at a time
        boolean[] valueIsNull = new boolean[positionCount];
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = sliceInput.readByte();
            valueIsNull[position] = ((value & 0b1000_0000) != 0);
            valueIsNull[position + 1] = ((value & 0b0100_0000) != 0);
            valueIsNull[position + 2] = ((value & 0b0010_0000) != 0);
            valueIsNull[position + 3] = ((value & 0b0001_0000) != 0);
            valueIsNull[position + 4] = ((value & 0b0000_1000) != 0);
            valueIsNull[position + 5] = ((value & 0b0000_0100) != 0);
            valueIsNull[position + 6] = ((value & 0b0000_0010) != 0);
            valueIsNull[position + 7] = ((value & 0b0000_0001) != 0);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = sliceInput.readByte();
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                valueIsNull[position] = ((value & mask) != 0);
                mask >>>= 1;
            }
        }

        return valueIsNull;
    }
}
