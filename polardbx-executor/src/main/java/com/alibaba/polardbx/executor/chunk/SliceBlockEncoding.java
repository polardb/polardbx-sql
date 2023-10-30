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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.alibaba.polardbx.executor.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.executor.chunk.EncoderUtil.encodeNullsAsBits;

public class SliceBlockEncoding implements BlockEncoding {
    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private final DataType type;

    public SliceBlockEncoding(DataType type) {
        this.type = type;
    }

    @Override
    public String getName() {
        return type.getStringSqlType();
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        // write type information.
        SliceBlock sliceBlock = (SliceBlock) block;
        SliceType dataType = (SliceType) sliceBlock.getType();
        byte[] charsetBytes = dataType.getCharsetName().name().getBytes(UTF8);
        byte[] collationBytes = dataType.getCollationName().name().getBytes(UTF8);
        sliceOutput.writeInt(charsetBytes.length);
        sliceOutput.writeBytes(charsetBytes);
        sliceOutput.writeInt(collationBytes.length);
        sliceOutput.writeBytes(collationBytes);

        sliceOutput.writeBoolean(sliceBlock.isCompatible());

        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        int nullsCnt = encodeNullsAsBits(sliceOutput, block);
        sliceOutput.writeBoolean(positionCount > nullsCnt);
        if (positionCount > nullsCnt) {
            sliceBlock.encoding(sliceOutput);
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        SliceType dataType = null;
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
        dataType = new SliceType(charsetName, collationName);

        boolean isCompatible = sliceInput.readBoolean();

        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);
        boolean existNonNull = sliceInput.readBoolean();

        int[] offset = new int[0];
        Slice data = Slices.EMPTY_SLICE;

        if (existNonNull) {
            offset = new int[positionCount];
            for (int position = 0; position < positionCount; position++) {
                offset[position] = sliceInput.readInt();
            }
            int maxOffset = offset[positionCount - 1];

            if (maxOffset > 0) {
                int length = sliceInput.readInt();
                Slice subRegion = sliceInput.readSlice(length);
                data = Slices.copyOf(subRegion);
            }
        }
        return new SliceBlock(dataType, 0, positionCount, valueIsNull, offset, data, isCompatible);
    }
}
