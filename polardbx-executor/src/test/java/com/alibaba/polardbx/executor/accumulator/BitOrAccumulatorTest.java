package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlock;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.utils.ByteUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FinalHyperLoglog;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class BitOrAccumulatorTest {

    private static final int COUNT = 1024;
    private LongBitOrAccumulator accumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new BitOr(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, COUNT,
                new ExecutionContext());

        this.accumulator = (LongBitOrAccumulator) accumulator;
        this.random = new Random();
    }

    @Test
    public void testLongBitOr1Group() {
        LongBlockBuilder builder = new LongBlockBuilder(COUNT);
        long result = 0;
        for (int i = 0; i < COUNT - 1; i++) {
            long l = random.nextLong();
            builder.writeLong(l);
            result |= l;
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
}
