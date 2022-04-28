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

import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.TimeZone;

import static com.alibaba.polardbx.executor.chunk.EncoderUtil.decodeNullBits;
import static com.alibaba.polardbx.executor.chunk.EncoderUtil.encodeNullsAsBits;

public class DateBlockEncoding implements BlockEncoding {
    private static final String NAME = "DATE";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block) {
        // write zone id
        TimeZone zone = ((DateBlock) block).getTimezone();
        byte[] zoneIdBytes = zone.getID().getBytes();
        sliceOutput.appendInt(zoneIdBytes.length);
        sliceOutput.appendBytes(zoneIdBytes);

        DateBlock dateBlock = (DateBlock) block;
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            sliceOutput.writeLong(dateBlock.getPackedLong(position));
        }
    }

    @Override
    public Block readBlock(SliceInput sliceInput) {
        // read zone id
        int length = sliceInput.readInt();
        byte[] zoneIdName = new byte[length];
        sliceInput.read(zoneIdName);
        TimeZone zone = TimeZone.getTimeZone(new String(zoneIdName));

        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            values[position] = sliceInput.readLong();
        }
        return new DateBlock(0, positionCount, valueIsNull, values, new DateType(), zone);
    }
}
