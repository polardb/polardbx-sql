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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.TimeZone;

import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.optimizer.chunk.EncoderUtil.encodeNullsAsBits;

public class TimeBlockEncoding implements BlockEncoding {
    private static final String NAME = "TIME";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        TimeBlock timeBlock = (TimeBlock) block;

        // write scale value
        DataType dataType = timeBlock.getDataType();
        int scale = dataType.getScale();
        sliceOutput.appendInt(scale);

        // write zone id
        TimeZone zone = timeBlock.getTimezone();
        byte[] zoneIdBytes = zone.getID().getBytes();
        sliceOutput.appendInt(zoneIdBytes.length);
        sliceOutput.appendBytes(zoneIdBytes);

        // write position count & null value
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);
        encodeNullsAsBits(sliceOutput, block);

        // write packed length and long value.
        long[] packed = timeBlock.getPacked();
        int length = Math.min(positionCount, packed.length);
        sliceOutput.writeInt(length);
        for (int i = 0; i < length; i++) {
            sliceOutput.writeLong(packed[i]);
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        // read data type
        int scale = sliceInput.readInt();
        DataType dataType = new TimeType(scale);

        // read zone id
        int length = sliceInput.readInt();
        byte[] zoneIdName = new byte[length];
        sliceInput.read(zoneIdName);
        TimeZone zone = TimeZone.getTimeZone(new String(zoneIdName));

        // read position count & null value
        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        // read packed long array
        int dataLength = sliceInput.readInt();
        long[] packed = new long[dataLength];
        for (int i = 0; i < dataLength; i++) {
            packed[i] = sliceInput.readLong();
        }
        return new TimeBlock(0, positionCount, valueIsNull, packed, dataType, zone);
    }
}
