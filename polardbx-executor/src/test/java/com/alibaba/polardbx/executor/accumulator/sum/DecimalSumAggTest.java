package com.alibaba.polardbx.executor.accumulator.sum;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.accumulator.DecimalSumAccumulator;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DecimalSumAggTest {
    private static final int DEC_COUNT = 5;
    public static final int TEST_TIME = 1 << 10;
    private Random random = new Random();

    @Test
    public void test() {
        IntStream.range(0, TEST_TIME).forEach(i -> doTest());
    }

    private void doTest() {
        // generate 5 random decimal
        List<Decimal> decList = IntStream.range(0, DEC_COUNT).mapToObj(i -> rand()).collect(Collectors.toList());

        // get answer
        Decimal expect = decList.stream().map(Decimal::toBigDecimal).reduce(
            (bigDecimal1, bigDecimal2) -> bigDecimal1.add(bigDecimal2)
        ).map(bigDecimal -> Decimal.fromBigDecimal(bigDecimal)).get();

        // use sum agg
        Decimal[] decArray = decList.toArray(new Decimal[0]);
        for (int i = 0; i <= DEC_COUNT; i++) {
            // use any combination of decimal values
            Decimal actual = sumAgg(i, decArray);

            // assert: actual - expect == 0
            Assert.assertTrue(actual.subtract(expect).getDecimalStructure().isZero());
        }
    }

    public Decimal sumAgg(int leftSize, Decimal... decimals) {
        Preconditions.checkArgument(decimals.length >= leftSize);

        // build block from left decimals
        DecimalBlockBuilder blockBuilder1 = new DecimalBlockBuilder(4);
        for (int i = 0; i < leftSize; i++) {
            blockBuilder1.writeDecimal(decimals[i]);
        }

        // build block from right decimals
        DecimalBlockBuilder blockBuilder2 = new DecimalBlockBuilder(4);
        for (int i = leftSize; i < decimals.length; i++) {
            blockBuilder2.writeDecimal(decimals[i]);
        }
        Block block2 = blockBuilder2.build();

        // append right decimals into left decimals.
        for (int i = 0; i < block2.getPositionCount(); i++) {
            block2.writePositionTo(i, blockBuilder1);
        }

        Block block1 = blockBuilder1.build();

        return doSumAgg(block1);
    }

    private static Decimal doSumAgg(Block block) {
        DecimalSumAccumulator decimalSumAccumulator = new DecimalSumAccumulator(block.getPositionCount(),
            DataTypes.DecimalType);
        decimalSumAccumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            decimalSumAccumulator.accumulate(0, block, i);
        }

        DecimalBlockBuilder result = new DecimalBlockBuilder(block.getPositionCount());
        decimalSumAccumulator.writeResultTo(0, result);
        return result.getDecimal(0);
    }

    /**
     * Generate random decimal using shift style or string style.
     */
    private Decimal rand() {
        // random long with sign
        final long unscaled = random.nextInt(1_000_000_000);

        // 0 ~ 9
        final int scale = random.nextInt(9);

        if (random.nextInt() % 2 == 0) {
            // use shift style.
            return new Decimal(unscaled, scale);
        } else {
            // use string style.
            BigDecimal bigDecimal = BigDecimal.valueOf(unscaled, scale);
            return Decimal.fromBigDecimal(bigDecimal);
        }
    }
}
