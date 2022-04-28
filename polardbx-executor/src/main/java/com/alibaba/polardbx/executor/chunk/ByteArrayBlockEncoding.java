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

import static com.alibaba.polardbx.executor.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.executor.chunk.EncoderUtil.encodeNullsAsBits;

public class ByteArrayBlockEncoding
    implements BlockEncoding {
    private static final String NAME = "BYTE_ARRAY";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {

        ByteArrayBlock byteArrayBlock = (ByteArrayBlock) block;
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        int[] offsets = byteArrayBlock.getOffsets();
        byte[] datas = byteArrayBlock.getData();
        int length = offsets[positionCount - 1];
        for (int i = 0; i < positionCount; i++) {
            sliceOutput.writeInt(offsets[i]);
        }
        sliceOutput.writeBytes(datas, 0, length);
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        int[] offsets = new int[positionCount];

        for (int position = 0; position < positionCount; position++) {
            offsets[position] = sliceInput.readInt();
        }
        int datalength = offsets[positionCount - 1];
        byte[] datas = new byte[datalength];
        sliceInput.readBytes(datas);
        return new ByteArrayBlock(0, positionCount, valueIsNull, offsets, datas);
    }
}
