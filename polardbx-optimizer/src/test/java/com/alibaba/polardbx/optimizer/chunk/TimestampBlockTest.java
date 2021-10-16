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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TimestampBlockTest extends BaseBlockTest {
    private final static Random R = new Random();
    private final static int TEST_SIZE = 1 << 20;
    private final static int TEST_SCALE = 4;

    @Test
    public void testTimestamp() {

        final TimestampType dataType = new TimestampType(TEST_SCALE);
        TimestampBlockBuilder timestampBlockBuilder =
            new TimestampBlockBuilder(TEST_SIZE, dataType, new ExecutionContext());

        // write
        List<String> values = IntStream.range(0, TEST_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .map(
                l -> R.nextInt() % 4 == 0 ? null : (String) l.get(0)
            )
            .map(String.class::cast)
            .collect(Collectors.toList());

        values.forEach(timestampBlockBuilder::writeString);
        Block block = timestampBlockBuilder.build();

        // serialization & deserialization
        TimestampBlockEncoding encoding = new TimestampBlockEncoding();
        SliceOutput sliceOutput = new DynamicSliceOutput(1 << 10);
        encoding.writeBlock(sliceOutput, block);

        Slice slice = sliceOutput.slice();
        Block block1 = encoding.readBlock(slice.getInput());

        IntStream.range(0, TEST_SIZE)
            .forEach(
                i -> {
                    boolean isEqual = block.equals(i, block1, i);
                    Assert.assertTrue(isEqual);
                }
            );
    }
}
