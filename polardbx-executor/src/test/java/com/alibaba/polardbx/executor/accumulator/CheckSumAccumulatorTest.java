package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.zip.CRC32;

public class CheckSumAccumulatorTest {

    private static final int COUNT = 1024;
    private CheckSumAccumulator accumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CheckSum(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType},
                COUNT, new ExecutionContext());

        this.accumulator = (CheckSumAccumulator) accumulator;
        this.random = new Random();
        Assert.assertEquals(1, accumulator.getInputTypes().length);
    }

    @Test
    public void testCheckSumWith1Block1Group() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        OrderInvariantHasher hash = new OrderInvariantHasher();

        for (int i = 0; i < COUNT - 1; i++) {
            CRC32 crc = new CRC32();
            long l = random.nextLong();
            builder.writeLong(l);
            int checksum = Long.hashCode(l);
            crc.update(new byte[] {
                (byte) (checksum >>> 24), (byte) (checksum >>> 16), (byte) (checksum >>> 8), (byte) checksum});
            crc.update(CrcAccumulator.SEPARATOR_TAG);

            hash.add(crc.getValue());
        }
        builder.appendNull();
        CRC32 crc = new CRC32();
        crc.update(CrcAccumulator.NULL_TAG);
        crc.update(CrcAccumulator.SEPARATOR_TAG);
        hash.add(crc.getValue());

        LongBlock block = (LongBlock) builder.build();
        Chunk chunk = new Chunk(block);

        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, chunk, i);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertEquals(hash.getResult().longValue(), resultBlock.getLong(0));

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }

    @Test
    public void testCheckSumWithAllNull() {
        accumulator.appendInitValue();

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertTrue(resultBlock.isNull(0));
    }
}
