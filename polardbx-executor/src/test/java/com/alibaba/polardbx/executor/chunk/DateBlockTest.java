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

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.memory.MemoryUsageReport;
import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;

public class DateBlockTest extends BaseBlockTest {
    private final static Random R = new Random();
    private final static int TEST_SIZE = 1 << 10;
    private final static int TEST_SCALE = 4;

    @Test
    public void testDate() {
        ExecutionContext context = new ExecutionContext();
        final DateType dataType = new DateType(TEST_SCALE);
        DateBlockBuilder dateBlockBuilder = new DateBlockBuilder(TEST_SIZE, dataType, context);

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

        values.forEach(dateBlockBuilder::writeString);

        MemoryCountable.checkDeviation(dateBlockBuilder, .05d, true);
        Block block = dateBlockBuilder.build();

        MemoryUsageReport report = FastMemoryCounter.parseInstance(block, 16, true, true, true);
        System.out.println("memory usage = " + report.getTotalSize());
        System.out.println("memory verbose = " + report.getFieldSizeMap());
        System.out.println("memory usage tree = \n" + report.getMemoryUsageTree());

        MemoryCountable.checkDeviation(block, .05d, true);

        // serialization & deserialization
        DateBlockEncoding encoding = new DateBlockEncoding();
        SliceOutput sliceOutput = new DynamicSliceOutput(1 << 10);
        encoding.writeBlock(sliceOutput, block);

        Slice slice = sliceOutput.slice();
        Block block1 = encoding.readBlock(slice.getInput());

        IntStream.range(0, TEST_SIZE)
            .forEach(
                i -> {
                    Assert.assertTrue(block.equals(i, block1, i));
                    assertTrue(block.equals(i, dateBlockBuilder, i));
                }
            );

        DateBlock dateBlock = (DateBlock) block;
        DateBlockBuilder builder1 = new DateBlockBuilder(TEST_SIZE / 2, dataType, context);
        for (int i = 0; i < TEST_SIZE; i++) {
            int r = i % 4;
            switch (r) {
            case 0:
                dateBlock.writePositionTo(i, builder1);
                break;
            case 1:
                if (dateBlock.isNull(i)) {
                    builder1.appendNull();
                } else {
                    builder1.writeDate(dateBlock.getDate(i));
                }
                break;
            case 2:
                if (dateBlock.isNull(i)) {
                    builder1.appendNull();
                } else {
                    builder1.writeDatetimeRawLong((Long) dateBlock.getObjectForCmp(i));
                }
                break;
            case 3:
                builder1.writeObject(dateBlock.getObject(i));
                break;
            }
        }
        MemoryCountable.checkDeviation(builder1, .05d, true);
        DateBlock newBlock = (DateBlock) builder1.build();
        MemoryCountable.checkDeviation(newBlock, .05d, true);

        for (int i = 0; i < TEST_SIZE; i++) {
            Assert.assertEquals(dateBlock.hashCode(i), newBlock.hashCode(i));
            Assert.assertEquals(dateBlock.hashCodeUseXxhash(i), newBlock.hashCodeUseXxhash(i));
            if (dateBlock.isNull(i)) {
                Assert.assertTrue(newBlock.isNull(i));
                continue;
            }
            Assert.assertEquals(dateBlock.getDate(i), newBlock.getDate(i));
            Assert.assertEquals(dateBlock.getDate(i), builder1.getDate(i));
            Assert.assertEquals(dateBlock.getObjectForCmp(i), builder1.getPackedLong(i));
            Assert.assertEquals(dateBlock.getObject(i), newBlock.getObject(i));
            Assert.assertEquals(dateBlock.getLong(i), newBlock.getLong(i));
        }

        BlockBuilder builder = builder1.newBlockBuilder();
        Assert.assertTrue(builder instanceof DateBlockBuilder);

        MemoryCountable.checkDeviation(builder, .05d, true);

        DateBlock newBlock2 = DateBlock.from(dateBlock, TEST_SIZE, null, false);
        MemoryCountable.checkDeviation(newBlock2, .05d, true);

        for (int i = 0; i < TEST_SIZE; i++) {
            if (dateBlock.isNull(i)) {
                Assert.assertTrue(newBlock2.isNull(i));
                continue;
            }
            Assert.assertEquals(dateBlock.getDate(i), newBlock2.getDate(i));
        }

        int[] sel = new int[TEST_SIZE / 2];
        for (int i = 0; i < sel.length; i++) {
            sel[i] = i * 2;
        }
        DateBlock newBlock3 = DateBlock.from(dateBlock, TEST_SIZE / 2, sel, true);
        MemoryCountable.checkDeviation(newBlock3, .05d, true);

        for (int i = 0; i < TEST_SIZE / 2; i++) {
            int j = sel[i];
            if (dateBlock.isNull(j)) {
                Assert.assertTrue(newBlock3.isNull(i));
                continue;
            }
            Assert.assertEquals(dateBlock.getDate(j), newBlock3.getDate(i));
        }

    }
}
