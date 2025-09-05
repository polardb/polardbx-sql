package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimeBlockTest extends BaseBlockTest {
    private final static Random R = new Random();
    private final static int TEST_SIZE = 1 << 20;
    private final static int TEST_SCALE = 4;

    @Test
    public void testTime() {

        final TimeType dataType = new TimeType(TEST_SCALE);
        TimeBlockBuilder timeBlockBuilder = new TimeBlockBuilder(TEST_SIZE, dataType, new ExecutionContext());

        // write
        List<String> values = IntStream.range(0, TEST_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateTimeString(1)
            )
            .map(
                l -> R.nextInt() % 4 == 0 ? null : (String) l.get(0)
            )
            .map(String.class::cast)
            .collect(Collectors.toList());

        values.forEach(timeBlockBuilder::writeString);

        MemoryCountable.checkDeviation(timeBlockBuilder, .05d, true);
        Block block = timeBlockBuilder.build();
        MemoryCountable.checkDeviation(block, .05d, true);

        // serialization & deserialization
        TimeBlockEncoding encoding = new TimeBlockEncoding();
        SliceOutput sliceOutput = new DynamicSliceOutput(1 << 10);
        encoding.writeBlock(sliceOutput, block);

        Slice slice = sliceOutput.slice();
        Block block1 = encoding.readBlock(slice.getInput());
        MemoryCountable.checkDeviation(block1, .05d, true);

        IntStream.range(0, TEST_SIZE)
            .forEach(
                i -> {
                    Assert.assertTrue(block.equals(i, block1, i));
                    Assert.assertTrue(block.equals(i, timeBlockBuilder, i));
                }
            );

        TimeBlock timeBlock = (TimeBlock) block;
        TimeBlockBuilder builder = new TimeBlockBuilder(TEST_SIZE / 2, dataType, new ExecutionContext());
        for (int i = 0; i < TEST_SIZE; i++) {
            timeBlock.writePositionTo(i, builder);
        }

        MemoryCountable.checkDeviation(builder, .05d, true);
        TimeBlock newBlock = (TimeBlock) builder.build();
        for (int i = 0; i < TEST_SIZE; i++) {
            Assert.assertEquals(timeBlock.getTime(i), newBlock.getTime(i));
            Assert.assertEquals(timeBlock.getObject(i), newBlock.getObject(i));
            Assert.assertEquals(timeBlock.getPackedLong(i), newBlock.getPackedLong(i));
            Assert.assertEquals(timeBlock.getLong(i), newBlock.getLong(i));
            Assert.assertEquals(timeBlock.hashCode(i), newBlock.hashCode(i));
            Assert.assertEquals(timeBlock.hashCodeUseXxhash(i), newBlock.hashCodeUseXxhash(i));
        }
        MemoryCountable.checkDeviation(timeBlock, .05d, true);
    }
}
