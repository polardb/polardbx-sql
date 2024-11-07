package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum0;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test illegal operations on certain accumulators
 */
public class IllegalAccumulatorOpTest {

    /**
     * Some accumulators need argument, while some don't
     */
    @Test
    public void testIllegalAccumulateCall() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new Sum0(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, 100,
                new ExecutionContext());
        Assert.assertTrue(accumulator instanceof LongSum0Accumulator);
        try {
            ((LongSum0Accumulator) accumulator).accumulate(0);
            Assert.fail("Expect exception here");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not implemented"));
        }

        Accumulator accumulator2 =
            AccumulatorBuilders.create(new CountV2(new int[0], false, null, 0), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, new ExecutionContext());
        Assert.assertTrue(accumulator2 instanceof CountRowsAccumulator);
        try {
            ((CountRowsAccumulator) accumulator2).accumulate(0, LongBlock.of(1L, 2L, 3L), 0);
            Assert.fail("Expect exception here");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("not implemented"));
        }

        DecimalAvgAccumulator faultyAccumulator = Mockito.mock(DecimalAvgAccumulator.class);
        Mockito.when(faultyAccumulator.getInputTypes())
            .thenReturn(new DataType[] {DataTypes.DecimalType, DataTypes.DecimalType});
        Mockito.doCallRealMethod().when(faultyAccumulator)
            .accumulate(Mockito.anyInt(), Mockito.any(Chunk.class), Mockito.anyInt());
        try {
            Block block = LongBlock.of(1L, 2L, 3L);
            Chunk chunk = new Chunk(block);
            faultyAccumulator.accumulate(0, chunk, 0);
            Assert.fail("Expect exception here");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("has multiple arguments"));
        }
    }

}
