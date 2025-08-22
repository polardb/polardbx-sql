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

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TimestampBlockTest extends BaseBlockTest {
    private final static Random R = new Random();
    private final static int TEST_SIZE = 1 << 10;
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

        MemoryCountable.checkDeviation(timestampBlockBuilder, .05d, true);
        Block block = timestampBlockBuilder.build();
        MemoryCountable.checkDeviation(block, .05d, true);

        // serialization & deserialization
        TimestampBlockEncoding encoding = new TimestampBlockEncoding();
        SliceOutput sliceOutput = new DynamicSliceOutput(1 << 10);
        encoding.writeBlock(sliceOutput, block);

        Slice slice = sliceOutput.slice();
        Block block1 = encoding.readBlock(slice.getInput());
        MemoryCountable.checkDeviation(block1, .05d, true);

        IntStream.range(0, TEST_SIZE)
            .forEach(
                i -> {
                    Assert.assertTrue(block.equals(i, block1, i));
                    Assert.assertTrue(block.equals(i, timestampBlockBuilder, i));
                }
            );

        TimestampBlock timestampBlock = (TimestampBlock) block;
        TimestampBlockBuilder builder = new TimestampBlockBuilder(TEST_SIZE / 2, dataType, new ExecutionContext());
        for (int i = 0; i < TEST_SIZE; i++) {
            timestampBlock.writePositionTo(i, builder);
        }

        MemoryCountable.checkDeviation(builder, .05d, true);
        TimestampBlock newBlock = (TimestampBlock) builder.build();
        for (int i = 0; i < TEST_SIZE; i++) {
            Assert.assertEquals(timestampBlock.getTimestamp(i), newBlock.getTimestamp(i));
            Assert.assertEquals(timestampBlock.getObject(i), newBlock.getObject(i));
            Assert.assertEquals(timestampBlock.getObjectForCmp(i), newBlock.getObjectForCmp(i));
            Assert.assertEquals(timestampBlock.hashCode(i), newBlock.hashCode(i));
            Assert.assertEquals(timestampBlock.hashCodeUseXxhash(i), newBlock.hashCodeUseXxhash(i));
        }
        MemoryCountable.checkDeviation(newBlock, .05d, true);
    }

    @Test
    public void testFrom() {
        final TimestampType dataType = new TimestampType(TEST_SCALE);

        TimestampBlockBuilder builder = new TimestampBlockBuilder(TEST_SIZE / 2, dataType, new ExecutionContext());
        for (int i = 0; i < TEST_SIZE - 1; i++) {
            builder.writeString(RandomTimeGenerator.generateStandardTimestamp().substring(0, 19));
        }
        builder.appendNull();

        MemoryCountable.checkDeviation(builder, .05d, true);
        TimestampBlock timestampBlock = (TimestampBlock) builder.build();
        MemoryCountable.checkDeviation(timestampBlock, .05d, true);

        TimestampBlock newBlock2 = TimestampBlock.from(timestampBlock, TimeZone.getDefault());
        for (int i = 0; i < TEST_SIZE; i++) {
            Assert.assertEquals(timestampBlock.getTimestamp(i), newBlock2.getTimestamp(i));
        }
        MemoryCountable.checkDeviation(newBlock2, .05d, true);

        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        final String targetZone = "GMT+6";
        final ZoneId targetZoneId = ZoneId.of("GMT+6");

        TimestampBlock newBlock3 = TimestampBlock.from(timestampBlock, TimeZone.getTimeZone(targetZone));
        for (int i = 0; i < TEST_SIZE; i++) {
            if (timestampBlock.isNull(i)) {
                Assert.assertTrue(newBlock3.isNull(i));
                continue;
            }
            String original = timestampBlock.getTimestamp(i).toString();
            LocalDateTime localDateTime = LocalDateTime.parse(original, formatter);
            ZonedDateTime originalZoneDateTime = localDateTime.atZone(TimeZone.getDefault().toZoneId());
            ZonedDateTime targetZoneDateTime = originalZoneDateTime.withZoneSameInstant(targetZoneId);
            String expectResult = targetZoneDateTime.format(formatter);
            Assert.assertEquals(expectResult, newBlock3.getTimestamp(i).toString());
        }
        MemoryCountable.checkDeviation(newBlock3, .05d, true);

        final String targetZone2 = "GMT+10";
        final ZoneId targetZoneId2 = ZoneId.of("GMT+10");
        TimestampBlock newBlock4 =
            TimestampBlock.from(timestampBlock, TEST_SIZE, null, false, TimeZone.getTimeZone(targetZone2));
        for (int i = 0; i < TEST_SIZE; i++) {
            if (timestampBlock.isNull(i)) {
                Assert.assertTrue(newBlock4.isNull(i));
                continue;
            }
            String original = timestampBlock.getTimestamp(i).toString();
            LocalDateTime localDateTime = LocalDateTime.parse(original, formatter);
            ZonedDateTime originalZoneDateTime = localDateTime.atZone(TimeZone.getDefault().toZoneId());
            ZonedDateTime targetZoneDateTime = originalZoneDateTime.withZoneSameInstant(targetZoneId2);
            String expectResult = targetZoneDateTime.format(formatter);
            Assert.assertEquals(expectResult, newBlock4.getTimestamp(i).toString());
        }
        MemoryCountable.checkDeviation(builder, .05d, true);
        MemoryCountable.checkDeviation(newBlock4, .05d, true);
    }
}
