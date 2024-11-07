package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum0;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LongSum0AccumulatorTest {

    private static final int COUNT = 1024;
    private LongSum0Accumulator accumulator;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new Sum0(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, COUNT,
                new ExecutionContext());

        this.accumulator = (LongSum0Accumulator) accumulator;
        Assert.assertEquals(1, accumulator.getInputTypes().length);
    }

    @Test
    public void testLongSum1Group() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        // not use random to avoid overflow
        long result = 0;
        for (int i = 0; i < COUNT - 1; i++) {
            builder.writeLong(i);
            result += i;
        }
        builder.appendNull();

        LongBlock block = (LongBlock) builder.build();

        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, block, i);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertEquals(result, resultBlock.getLong(0));

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }

    /**
     * sum0 regards NULL as 0
     */
    @Test
    public void testLongSumWithAllNull() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        for (int i = 0; i < COUNT; i++) {
            builder.appendNull();
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
}
