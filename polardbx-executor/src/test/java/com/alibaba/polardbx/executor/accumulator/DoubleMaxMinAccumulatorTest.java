package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.DoubleBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class DoubleMaxMinAccumulatorTest {

    private static final int COUNT = 1024;
    private DoubleMaxMinAccumulator minAccumulator;
    private DoubleMaxMinAccumulator maxAccumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator minAccumulator =
            AccumulatorBuilders.create(new MinV2(), DataTypes.DoubleType, new DataType[] {DataTypes.DoubleType},
                COUNT,
                new ExecutionContext());
        Accumulator maxAccumulator =
            AccumulatorBuilders.create(new MaxV2(), DataTypes.DoubleType, new DataType[] {DataTypes.DoubleType},
                COUNT,
                new ExecutionContext());

        this.minAccumulator = (DoubleMaxMinAccumulator) minAccumulator;
        this.maxAccumulator = (DoubleMaxMinAccumulator) maxAccumulator;
        this.random = new Random();

        Assert.assertEquals(1, minAccumulator.getInputTypes().length);
        Assert.assertEquals(1, maxAccumulator.getInputTypes().length);
    }

    /**
     * Group0 has values
     * Group1 has no values
     */
    @Test
    public void testMaxMin2Groups() {
        DoubleBlockBuilder builder = new DoubleBlockBuilder(COUNT);
        double d = random.nextDouble();
        builder.writeDouble(d);
        double min = d, max = d;
        for (int i = 0; i < COUNT - 1; i++) {
            d = random.nextDouble();
            builder.writeDouble(d);
            if (d < min) {
                min = d;
            }
            if (d > max) {
                max = d;
            }
        }

        DoubleBlock block = (DoubleBlock) builder.build();

        minAccumulator.appendInitValue();
        minAccumulator.appendInitValue();
        maxAccumulator.appendInitValue();
        maxAccumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            minAccumulator.accumulate(0, block, i);
            maxAccumulator.accumulate(0, block, i);
        }

        DoubleBlockBuilder minResultBuilder = new DoubleBlockBuilder(COUNT);
        DoubleBlockBuilder maxResultBuilder = new DoubleBlockBuilder(COUNT);
        minAccumulator.writeResultTo(0, minResultBuilder);
        minAccumulator.writeResultTo(1, minResultBuilder);
        maxAccumulator.writeResultTo(0, maxResultBuilder);
        maxAccumulator.writeResultTo(1, maxResultBuilder);
        Block minResultBlock = minResultBuilder.build();
        Assert.assertEquals(2, minResultBlock.getPositionCount());
        Block maxResultBlock = maxResultBuilder.build();
        Assert.assertEquals(2, maxResultBuilder.getPositionCount());

        Assert.assertEquals(min, minResultBlock.getDouble(0), 1e-10);
        Assert.assertEquals(max, maxResultBlock.getDouble(0), 1e-10);

        Assert.assertTrue(minResultBlock.isNull(1));
        Assert.assertTrue(maxResultBlock.isNull(1));

        Assert.assertTrue(minAccumulator.estimateSize() > 0);
        Assert.assertTrue(maxAccumulator.estimateSize() > 0);
    }

}
