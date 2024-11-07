package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlock;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.utils.ByteUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.HyperLoglog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class HLLAccumulatorTest {

    private static final int COUNT = 1024;
    private HyperLogLogAccumulator accumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new HyperLoglog(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, COUNT, new ExecutionContext());

        this.accumulator = (HyperLogLogAccumulator) accumulator;
        this.random = new Random();
        Assert.assertEquals(1, accumulator.getInputTypes().length);
    }

    @Test
    public void testHLL1Group() {
        ByteArrayBlockBuilder builder = new ByteArrayBlockBuilder(COUNT, 8);
        for (int i = 0; i < COUNT - 1; i++) {
            builder.writeByteArray(ByteUtil.toByteArray(random.nextLong()));
        }
        builder.appendNull();
        ByteArrayBlock block = (ByteArrayBlock) builder.build();
        Chunk chunk = new Chunk(block);

        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, chunk, i);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);

        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertFalse(resultBlock.isNull(0));
    }

    @Test
    public void testHLL1NullInput() {
        ByteArrayBlockBuilder builder = new ByteArrayBlockBuilder(COUNT, 8);
        for (int i = 0; i < COUNT; i++) {
            builder.appendNull();
        }
        ByteArrayBlock block = (ByteArrayBlock) builder.build();
        Chunk chunk = new Chunk(block);

        accumulator.appendInitValue();
        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, chunk, i);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        accumulator.writeResultTo(1, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(2, resultBlock.getPositionCount());
        // group 1 has no values
        Assert.assertEquals(0, resultBuilder.getLong(1));
    }

    @Test
    public void testHLL2Blocks() {
        ByteArrayBlockBuilder builder1 = new ByteArrayBlockBuilder(COUNT, 8);
        ByteArrayBlockBuilder builder2 = new ByteArrayBlockBuilder(COUNT, 8);
        for (int i = 0; i < COUNT - 1; i++) {
            builder1.writeByteArray(ByteUtil.toByteArray(random.nextLong()));
            builder2.writeByteArray(ByteUtil.toByteArray(random.nextLong()));
        }
        builder1.appendNull();
        ByteArrayBlock block1 = (ByteArrayBlock) builder1.build();
        ByteArrayBlock block2 = (ByteArrayBlock) builder1.build();
        Chunk chunk = new Chunk(block1, block2);

        accumulator.appendInitValue();
        for (int i = 0; i < chunk.getPositionCount(); i++) {
            accumulator.accumulate(0, chunk, i);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);

        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertFalse(resultBlock.isNull(0));

        Assert.assertTrue(accumulator.estimateSize() > 0);
    }
}
