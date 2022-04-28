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

import com.alibaba.polardbx.common.utils.convertor.ConvertorException;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.sql.Blob;
import java.sql.SQLException;

import static com.alibaba.polardbx.executor.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.executor.chunk.EncoderUtil.encodeNullsAsBits;

public class BlobBlockEncoding implements BlockEncoding {
    private static final String NAME = "BLOB";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        Blob[] values = new Blob[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (!valueIsNull[position]) {
                int length = sliceInput.readInt();
                byte[] data = new byte[length];
                sliceInput.readBytes(data);
                values[position] = new com.alibaba.polardbx.optimizer.core.datatype.Blob(data);
            }
        }
        return new BlobBlock(0, positionCount, valueIsNull, values);
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                try {
                    Blob blob = block.getBlob(position);
                    int blobLength = (int) blob.length();
                    sliceOutput.writeInt(blobLength);
                    sliceOutput.writeBytes(blob.getBytes(1, (int) blob.length()));
                } catch (SQLException e) {
                    throw new ConvertorException(e);
                }

            }
        }
    }
}
