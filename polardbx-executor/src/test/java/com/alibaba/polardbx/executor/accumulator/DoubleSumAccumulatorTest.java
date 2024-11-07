package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.DoubleBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class DoubleSumAccumulatorTest {

    private static final int COUNT = 1024;
    private DoubleSumAccumulator accumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new SumV2(), DataTypes.DoubleType, new DataType[] {DataTypes.DoubleType}, COUNT,
                new ExecutionContext());

        this.accumulator = (DoubleSumAccumulator) accumulator;
        Assert.assertEquals(1, accumulator.getInputTypes().length);

        this.random = new Random();
    }

    @Test
    public void testDoubleSum1Group() {
        DoubleBlockBuilder builder = new DoubleBlockBuilder(COUNT);
        double result = 0;
        for (int i = 0; i < COUNT; i++) {
            double d = random.nextDouble();
            builder.writeDouble(d);
            result += d;
        }

        DoubleBlock block = (DoubleBlock) builder.build();

        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, block, i);
        }

        DoubleBlockBuilder resultBuilder = new DoubleBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertEquals(result, resultBlock.getDouble(0), 1e-10);

        long size = accumulator.estimateSize();
        Assert.assertTrue(size > 0);
    }

    /**
     * regards NULL as NULL rather than 0
     */
    @Test
    public void testDoubleSumWithAllNull() {
        DoubleBlockBuilder builder = new DoubleBlockBuilder(COUNT);
        for (int i = 0; i < COUNT; i++) {
            builder.appendNull();
        }

        DoubleBlock block = (DoubleBlock) builder.build();

        accumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            accumulator.accumulate(0, block, i);
        }

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertTrue(resultBlock.isNull(0));
    }
}
