package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumV2Merge;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class CheckSumV2MergeAccumulatorTest {

    private static final int COUNT = 1024;
    private CheckSumV2MergeAccumulator accumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CheckSumV2Merge(), DataTypes.LongType, new DataType[] {DataTypes.LongType},
                COUNT, new ExecutionContext());

        this.accumulator = (CheckSumV2MergeAccumulator) accumulator;
        this.random = new Random();
    }

    @Test
    public void testCheckSumV2Merge1Group() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();

        for (int i = 0; i < COUNT - 1; i++) {
            long l = random.nextLong();
            builder.writeLong(l);
            hash.add(l).remove(0);
        }
        builder.appendNull();
        hash.add(0).remove(0);

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
    public void testCheckSumV2MergeWithAllNull() {
        accumulator.appendInitValue();

        LongBlockBuilder resultBuilder = new LongBlockBuilder(COUNT);
        accumulator.writeResultTo(0, resultBuilder);
        Block resultBlock = resultBuilder.build();
        Assert.assertEquals(1, resultBlock.getPositionCount());
        Assert.assertTrue(resultBlock.isNull(0));
    }
}
