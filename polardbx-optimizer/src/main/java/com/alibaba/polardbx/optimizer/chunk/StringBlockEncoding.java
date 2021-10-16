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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.encodeNullsAsBits;

public class StringBlockEncoding implements BlockEncoding {
    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final String NAME = "VARCHAR";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        StringBlock stringBlock = (StringBlock) block;
        DataType dataType = stringBlock.getType();
        boolean hasStringType = dataType != null;
        sliceOutput.writeBoolean(hasStringType);
        if (hasStringType) {
            byte[] charsetBytes = dataType.getCharsetName().name().getBytes(UTF8);
            byte[] collationBytes = dataType.getCollationName().name().getBytes(UTF8);
            sliceOutput.writeInt(charsetBytes.length);
            sliceOutput.writeBytes(charsetBytes);
            sliceOutput.writeInt(collationBytes.length);
            sliceOutput.writeBytes(collationBytes);
        }

        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int nullsCnt = encodeNullsAsBits(sliceOutput, block);
        sliceOutput.writeBoolean(positionCount > nullsCnt);
        if (positionCount > nullsCnt) {
            int[] offset = stringBlock.getOffsets();
            for (int position = 0; position < positionCount; position++) {
                sliceOutput.writeInt(offset[position]);
            }
            int maxOffset = offset[positionCount - 1];
            if (maxOffset > 0) {
                char[] data = stringBlock.getData();
                byte[] bytes = new String(data, 0, maxOffset).getBytes();
                sliceOutput.writeInt(bytes.length);
                sliceOutput.writeBytes(bytes);
            }
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        DataType stringType = null;
        boolean hasStringType = sliceInput.readBoolean();
        if (hasStringType) {
            CharsetName charsetName = Optional.of(sliceInput.readInt())
                .map(i -> {
                    byte[] bs = new byte[i];
                    sliceInput.readBytes(bs);
                    String c = new String(bs, UTF8);
                    return c;
                })
                .map(CharsetName::of)
                .orElse(CharsetName.defaultCharset());
            CollationName collationName = Optional.of(sliceInput.readInt())
                .map(i -> {
                    byte[] bs = new byte[i];
                    sliceInput.readBytes(bs);
                    String c = new String(bs, UTF8);
                    return c;
                })
                .map(CollationName::of)
                .orElse(CollationName.defaultCollation());
            stringType = new StringType(charsetName, collationName);
        }

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
        return new StringBlock(stringType, 0, positionCount, valueIsNull, offset, data);

    }
}
