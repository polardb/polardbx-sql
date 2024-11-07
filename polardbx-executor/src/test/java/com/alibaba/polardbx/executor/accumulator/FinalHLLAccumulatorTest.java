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
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FinalHyperLoglog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class FinalHLLAccumulatorTest {

    private static final int COUNT = 1024;
    private FinalHyperLogLogAccumulator accumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new FinalHyperLoglog(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, COUNT, new ExecutionContext());

        this.accumulator = (FinalHyperLogLogAccumulator) accumulator;
        this.random = new Random();
        Assert.assertEquals(1, accumulator.getInputTypes().length);
    }

    @Test
    public void testFinalHLL1Group() {
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
        Assert.assertTrue(resultBlock.getLong(0) > 0);
    }

    @Test
    public void testFinalHLL1NullInput() {
        ByteArrayBlockBuilder builder = new ByteArrayBlockBuilder(COUNT, 8);
        for (int i = 0; i < COUNT; i++) {
            builder.appendNull();
        }
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
    }

    @Test
    public void testFinalHLLWithIndex() {
        ByteArrayBlockBuilder builder = new ByteArrayBlockBuilder(COUNT, 8);
        for (int i = 0; i < COUNT - 1; i++) {
            builder.writeByteArray(ByteUtil.toByteArray(random.nextLong()));
        }
        builder.appendNull();
        ByteArrayBlock block = (ByteArrayBlock) builder.build();
        Chunk chunk = new Chunk(block);

        accumulator.appendInitValue();
        accumulator.accumulate(0, chunk, 0, COUNT);

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertTrue(resultBlock.getLong(0) > 0);

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }
}
