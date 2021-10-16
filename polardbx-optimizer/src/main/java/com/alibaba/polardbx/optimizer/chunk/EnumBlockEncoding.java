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

import java.util.Map;

import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.encodeNullsAsBits;

public class EnumBlockEncoding implements BlockEncoding {
    private static final String NAME = "ENUM";

    final Map<String, Integer> enumValues;

    public EnumBlockEncoding(Map<String, Integer> enumValues) {
        this.enumValues = enumValues;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int nullsCnt = encodeNullsAsBits(sliceOutput, block);
        sliceOutput.writeBoolean(positionCount > nullsCnt);
        if (positionCount > nullsCnt) {
            EnumBlock EnumBlock = (EnumBlock) block;
            int[] offset = EnumBlock.getOffsets();
            for (int position = 0; position < positionCount; position++) {
                sliceOutput.writeInt(offset[position]);
            }
            int maxOffset = offset[positionCount - 1];
            if (maxOffset > 0) {
                char[] data = EnumBlock.getData();
                byte[] bytes = new String(data, 0, maxOffset).getBytes();
                sliceOutput.writeInt(bytes.length);
                sliceOutput.writeBytes(bytes);
            }
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);
        boolean existNonNull = sliceInput.readBoolean();

        int[] offset = new int[0];
        char[] data = new char[0];

        if (existNonNull) {
            offset = new int[positionCount];
            for (int position = 0; position < positionCount; position++) {
                offset[position] = sliceInput.readInt();
            }
            int maxOffset = offset[positionCount - 1];

            if (maxOffset > 0) {
                int length = sliceInput.readInt();
                byte[] bytes = new byte[length];
                sliceInput.readBytes(bytes);
                data = new String(bytes).toCharArray();
            }
        }
        return new EnumBlock(0, positionCount, valueIsNull, offset, data, this.enumValues);

    }
}
