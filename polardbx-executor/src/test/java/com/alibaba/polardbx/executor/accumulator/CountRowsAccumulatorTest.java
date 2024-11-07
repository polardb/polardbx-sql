package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CountRowsAccumulatorTest {

    private static final int COUNT = 1024;
    private CountRowsAccumulator accumulator;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CountV2(new int[0], false, null, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, COUNT,
                new ExecutionContext());

        this.accumulator = (CountRowsAccumulator) accumulator;
        Assert.assertEquals(0, accumulator.getInputTypes().length);
    }

    @Test
    public void testCountRows2Group() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT - 1; i++) {
            builder.writeLong(i);
        }
        builder.appendNull();

        LongBlock block = (LongBlock) builder.build();

        accumulator.appendInitValue();
        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        accumulator.writeResultTo(1, resultBuilder);
        Block resultBlock = resultBuilder.build();

        Assert.assertEquals(2, resultBlock.getPositionCount());
        Assert.assertEquals(COUNT, resultBlock.getLong(0));
        Assert.assertEquals(0, resultBlock.getLong(1));

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }

    @Test
    public void testCountRows2GroupWithStartEnd() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT - 1; i++) {
            builder.writeLong(i);
        }
        builder.appendNull();

        int offset = COUNT / 2;

        LongBlock block = (LongBlock) builder.build();
        Chunk chunk = new Chunk(block);
        accumulator.appendInitValue();
        accumulator.appendInitValue();
        accumulator.accumulate(0, chunk, 0, offset);
        accumulator.accumulate(1, chunk, offset, COUNT);

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        accumulator.writeResultTo(1, resultBuilder);
        Block resultBlock = resultBuilder.build();

        Assert.assertEquals(2, resultBlock.getPositionCount());
        Assert.assertEquals(offset, resultBlock.getLong(0));
        Assert.assertEquals(COUNT - offset, resultBlock.getLong(1));

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }

    @Test
    public void testCountRowsWithGroupId() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT - 1; i++) {
            builder.writeLong(i);
        }
        builder.appendNull();

        int[] groupIds = new int[COUNT];
        for (int i = 0; i < COUNT; i++) {
            groupIds[i] = i % 2;
        }

        LongBlock block = (LongBlock) builder.build();
        Chunk chunk = new Chunk(block);
        accumulator.appendInitValue();
        accumulator.appendInitValue();
        accumulator.accumulate(groupIds, chunk, COUNT);

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        accumulator.writeResultTo(1, resultBuilder);
        Block resultBlock = resultBuilder.build();

        Assert.assertEquals(2, resultBlock.getPositionCount());
        Assert.assertEquals(COUNT / 2, resultBlock.getLong(0));
        Assert.assertEquals(COUNT / 2, resultBlock.getLong(1));

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }

    @Test
    public void testCountRowsWithGroupIdSelection() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT - 1; i++) {
            builder.writeLong(i);
        }
        builder.appendNull();

        int[] group0Selection = new int[COUNT / 2];
        int[] group1Selection = new int[COUNT / 2];
        for (int i = 0; i < COUNT / 2; i++) {
            group0Selection[i] = i * 2;
            group1Selection[i] = i * 2 + 1;
        }

        LongBlock block = (LongBlock) builder.build();
        Chunk chunk = new Chunk(block);
        accumulator.appendInitValue();
        accumulator.appendInitValue();
        accumulator.accumulate(0, chunk, group0Selection, COUNT / 2);
        accumulator.accumulate(1, chunk, group1Selection, COUNT / 2);

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        accumulator.writeResultTo(1, resultBuilder);
        Block resultBlock = resultBuilder.build();

        Assert.assertEquals(2, resultBlock.getPositionCount());
        Assert.assertEquals(COUNT / 2, resultBlock.getLong(0));
        Assert.assertEquals(COUNT / 2, resultBlock.getLong(1));

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }
}
