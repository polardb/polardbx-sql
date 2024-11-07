package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class DecimalMaxMinAccumulatorTest {

    private static final int COUNT = 1024;
    private DecimalMaxMinAccumulator minAccumulator;
    private DecimalMaxMinAccumulator maxAccumulator;
    private Random random;

    @Before
    public void before() {
        Accumulator minAccumulator =
            AccumulatorBuilders.create(new MinV2(), DataTypes.DecimalType, new DataType[] {DataTypes.DecimalType},
                COUNT,
                new ExecutionContext());
        Accumulator maxAccumulator =
            AccumulatorBuilders.create(new MaxV2(), DataTypes.DecimalType, new DataType[] {DataTypes.DecimalType},
                COUNT,
                new ExecutionContext());

        this.minAccumulator = (DecimalMaxMinAccumulator) minAccumulator;
        this.maxAccumulator = (DecimalMaxMinAccumulator) maxAccumulator;
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
        final int scale = 2;
        DecimalBlockBuilder builder = new DecimalBlockBuilder(COUNT);

        Decimal decimal = new Decimal(random.nextLong(), scale);
        builder.writeDecimal(decimal);
        Decimal min = decimal, max = decimal;
        for (int i = 0; i < COUNT - 1; i++) {
            decimal = new Decimal(random.nextLong(), scale);
            builder.writeDecimal(decimal);
            int minRes = FastDecimalUtils.compare(decimal.getDecimalStructure(), min.getDecimalStructure());
            if (minRes < 0) {
                min = decimal;
            }
            int maxRes = FastDecimalUtils.compare(decimal.getDecimalStructure(), max.getDecimalStructure());
            if (maxRes > 0) {
                max = decimal;
            }
        }

        DecimalBlock block = (DecimalBlock) builder.build();

        minAccumulator.appendInitValue();
        minAccumulator.appendInitValue();
        maxAccumulator.appendInitValue();
        maxAccumulator.appendInitValue();
        for (int i = 0; i < block.getPositionCount(); i++) {
            minAccumulator.accumulate(0, block, i);
            maxAccumulator.accumulate(0, block, i);
        }

        DecimalBlockBuilder minResultBuilder = new DecimalBlockBuilder(COUNT);
        DecimalBlockBuilder maxResultBuilder = new DecimalBlockBuilder(COUNT);
        minAccumulator.writeResultTo(0, minResultBuilder);
        minAccumulator.writeResultTo(1, minResultBuilder);
        maxAccumulator.writeResultTo(0, maxResultBuilder);
        maxAccumulator.writeResultTo(1, maxResultBuilder);
        Block minResultBlock = minResultBuilder.build();
        Assert.assertEquals(2, minResultBlock.getPositionCount());
        Block maxResultBlock = maxResultBuilder.build();
        Assert.assertEquals(2, maxResultBuilder.getPositionCount());

        Assert.assertEquals(0, FastDecimalUtils.compare(minResultBlock.getDecimal(0).getDecimalStructure(),
            min.getDecimalStructure()));
        Assert.assertEquals(0, FastDecimalUtils.compare(maxResultBlock.getDecimal(0).getDecimalStructure(),
            max.getDecimalStructure()));

        Assert.assertTrue(minResultBlock.isNull(1));
        Assert.assertTrue(maxResultBlock.isNull(1));

        Assert.assertTrue(minAccumulator.estimateSize() > 0);
        Assert.assertTrue(maxAccumulator.estimateSize() > 0);
    }

}
