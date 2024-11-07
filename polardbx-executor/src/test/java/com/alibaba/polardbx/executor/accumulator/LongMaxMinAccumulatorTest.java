package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class LongMaxMinAccumulatorTest {

    private static final int COUNT = 1024;
    private LongMaxMinAccumulator minAccumulator;
    private LongMaxMinAccumulator maxAccumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator minAccumulator =
            AccumulatorBuilders.create(new MinV2(), DataTypes.LongType, new DataType[] {DataTypes.LongType},
                COUNT,
                new ExecutionContext());
        Accumulator maxAccumulator =
            AccumulatorBuilders.create(new MaxV2(), DataTypes.LongType, new DataType[] {DataTypes.LongType},
                COUNT,
                new ExecutionContext());

        this.minAccumulator = (LongMaxMinAccumulator) minAccumulator;
        this.maxAccumulator = (LongMaxMinAccumulator) maxAccumulator;
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
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        long l = random.nextLong();
        builder.writeLong(l);
        long min = l, max = l;
        for (int i = 0; i < COUNT - 2; i++) {
            l = random.nextLong();
            builder.writeLong(l);
            if (l < min) {
                min = l;
            }
            if (l > max) {
                max = l;
            }
        }
        builder.appendNull();

        LongBlock block = (LongBlock) builder.build();

        minAccumulator.appendInitValue();
        minAccumulator.appendInitValue();
        maxAccumulator.appendInitValue();
        maxAccumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            minAccumulator.accumulate(0, block, i);
            maxAccumulator.accumulate(0, block, i);
        }

        LongBlockBuilder minResultBuilder = new LongBlockBuilder(COUNT);
        LongBlockBuilder maxResultBuilder = new LongBlockBuilder(COUNT);
        minAccumulator.writeResultTo(0, minResultBuilder);
        minAccumulator.writeResultTo(1, minResultBuilder);
        maxAccumulator.writeResultTo(0, maxResultBuilder);
        maxAccumulator.writeResultTo(1, maxResultBuilder);
        Block minResultBlock = minResultBuilder.build();
        Assert.assertEquals(2, minResultBlock.getPositionCount());
        Block maxResultBlock = maxResultBuilder.build();
        Assert.assertEquals(2, maxResultBuilder.getPositionCount());

        Assert.assertEquals(min, minResultBlock.getLong(0));
        Assert.assertEquals(max, maxResultBlock.getLong(0));

        Assert.assertTrue(minResultBlock.isNull(1));
        Assert.assertTrue(maxResultBlock.isNull(1));

        Assert.assertTrue(minAccumulator.estimateSize() > 0);
        Assert.assertTrue(maxAccumulator.estimateSize() > 0);
    }

}
