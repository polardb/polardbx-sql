package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumMerge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class CheckSumMergeAccumulatorTest {

    private static final int COUNT = 1024;
    private CheckSumMergeAccumulator accumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CheckSumMerge(), DataTypes.LongType, new DataType[] {DataTypes.LongType},
                COUNT, new ExecutionContext());

        this.accumulator = (CheckSumMergeAccumulator) accumulator;
        this.random = new Random();
    }

    @Test
    public void testCheckSumMerge1Group() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        OrderInvariantHasher hash = new OrderInvariantHasher();

        for (int i = 0; i < COUNT - 1; i++) {
            long l = random.nextLong();
            builder.writeLong(l);
            hash.add(l);
        }
        builder.appendNull();
        // no need to add hasher for null

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
    public void testCheckSumMergeWithAllNull() {
        accumulator.appendInitValue();

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertTrue(resultBlock.isNull(0));
    }
}
