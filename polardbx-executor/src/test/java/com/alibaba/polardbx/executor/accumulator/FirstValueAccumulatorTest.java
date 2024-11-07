package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.InternalFirstValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FirstValueAccumulatorTest {

    private static final int COUNT = 100;
    private FirstValueAccumulator accumulator;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new InternalFirstValue(0, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, COUNT, new ExecutionContext());
        this.accumulator = (FirstValueAccumulator) accumulator;

        Assert.assertEquals(1, accumulator.getInputTypes().length);
    }

    @Test
    public void testFirstValue1Group() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT; i++) {
            builder.writeLong(i);
        }
        LongBlock block = (LongBlock) builder.build();

        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, block, i);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertEquals(0, resultBlock.getLong(0));
    }

    @Test
    public void testFirstValue2Groups() {
        final int offset = 10;
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT; i++) {
            builder.writeLong(i);
        }
        LongBlock block = (LongBlock) builder.build();

        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, block, i);
            accumulator.accumulate(1, block, (i + offset) % COUNT);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        accumulator.writeResultTo(1, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(2, resultBlock.getPositionCount());
        Assert.assertEquals(0, resultBlock.getLong(0));
        Assert.assertEquals(10, resultBlock.getLong(1));
    }

    @Test
    public void testFirstValueManyGroups() {
        final int groups = 2048;
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT; i++) {
            builder.writeLong(i);
        }
        LongBlock block = (LongBlock) builder.build();

        accumulator.appendInitValue();

        for (int i = 0; i < block.getPositionCount(); i++) {
            for (int j = 0; j < groups; j++) {
                accumulator.accumulate(j, block, (i + j) % COUNT);
            }
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < groups; i++) {
            accumulator.writeResultTo(i, resultBuilder);
        }
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(groups, resultBlock.getPositionCount());
        for (int i = 0; i < groups; i++) {
            Assert.assertFalse(resultBuilder.isNull(i));
            Assert.assertEquals("Wrong result at position: " + i, i % COUNT, resultBlock.getLong(i));
        }

        long size = accumulator.estimateSize();
        Assert.assertTrue("Estimate size should be larger than 0", size > 0);
    }

    @Test
    public void testFirstValueNullInput() {
        accumulator.appendInitValue();

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertTrue(resultBlock.isNull(0));
    }

}
