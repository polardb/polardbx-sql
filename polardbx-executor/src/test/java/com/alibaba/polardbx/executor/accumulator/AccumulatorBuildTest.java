package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitOr;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.BitXor;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumMerge;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumV2Merge;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FinalHyperLoglog;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.HyperLoglog;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.InternalFirstValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.PartialHyperLoglog;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Sum0;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import org.junit.Test;

public class AccumulatorBuildTest {

    @Test
    public void testCountAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CountV2(new int[] {0}, false, null, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof CountAccumulator);

        Accumulator accumulator2 =
            AccumulatorBuilders.create(new CountV2(new int[0], false, null, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator2 instanceof CountRowsAccumulator);
    }

    @Test
    public void testSumAccumulator() {
        Accumulator longAccumulator =
            AccumulatorBuilders.create(new SumV2(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, 100,
                getExecutionContext());
        Assert.assertTrue(longAccumulator instanceof LongSumAccumulator);

        Accumulator decimalAccumulator =
            AccumulatorBuilders.create(new SumV2(), DataTypes.DecimalType, new DataType[] {DataTypes.DecimalType}, 100,
                getExecutionContext());
        Assert.assertTrue(decimalAccumulator instanceof DecimalSumAccumulator);

        Accumulator doubleAccumulator =
            AccumulatorBuilders.create(new SumV2(), DataTypes.DoubleType, new DataType[] {DataTypes.DoubleType}, 100,
                getExecutionContext());
        Assert.assertTrue(doubleAccumulator instanceof DoubleSumAccumulator);
    }

    @Test
    public void testSum0Accumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new Sum0(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, 100,
                getExecutionContext());
        Assert.assertTrue(accumulator instanceof LongSum0Accumulator);
    }

    @Test
    public void testMinMaxAccumulator() {
        Accumulator longAccumulator =
            AccumulatorBuilders.create(new MinV2(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, 100,
                getExecutionContext());
        Assert.assertTrue(longAccumulator instanceof LongMaxMinAccumulator);
        Accumulator longAccumulator2 =
            AccumulatorBuilders.create(new MaxV2(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, 100,
                getExecutionContext());
        Assert.assertTrue(longAccumulator2 instanceof LongMaxMinAccumulator);

        Accumulator decimalAccumulator =
            AccumulatorBuilders.create(new MinV2(), DataTypes.DecimalType, new DataType[] {DataTypes.DecimalType}, 100,
                getExecutionContext());
        Assert.assertTrue(decimalAccumulator instanceof DecimalMaxMinAccumulator);
        Accumulator decimalAccumulator2 =
            AccumulatorBuilders.create(new MaxV2(), DataTypes.DecimalType, new DataType[] {DataTypes.DecimalType}, 100,
                getExecutionContext());
        Assert.assertTrue(decimalAccumulator2 instanceof DecimalMaxMinAccumulator);

        Accumulator doubleAccumulator =
            AccumulatorBuilders.create(new MinV2(), DataTypes.DoubleType, new DataType[] {DataTypes.DoubleType}, 100,
                getExecutionContext());
        Assert.assertTrue(doubleAccumulator instanceof DoubleMaxMinAccumulator);
        Accumulator doubleAccumulator2 =
            AccumulatorBuilders.create(new MaxV2(), DataTypes.DoubleType, new DataType[] {DataTypes.DoubleType}, 100,
                getExecutionContext());
        Assert.assertTrue(doubleAccumulator2 instanceof DoubleMaxMinAccumulator);
    }

    @Test
    public void testAvgAccumulator() {
        Accumulator decimalAccumulator =
            AccumulatorBuilders.create(new AvgV2(0, false, null, -1), DataTypes.DecimalType,
                new DataType[] {DataTypes.DecimalType}, 100, getExecutionContext());
        Assert.assertTrue(decimalAccumulator instanceof DecimalAvgAccumulator);

        Accumulator doubleAccumulator =
            AccumulatorBuilders.create(new AvgV2(0, false, null, -1), DataTypes.DoubleType,
                new DataType[] {DataTypes.DoubleType}, 100, getExecutionContext());
        Assert.assertTrue(doubleAccumulator instanceof DoubleAvgAccumulator);
    }

    @Test
    public void testBitOrAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new BitOr(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, 100,
                getExecutionContext());
        Assert.assertTrue(accumulator instanceof LongBitOrAccumulator);
    }

    @Test
    public void testBitXorAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new BitXor(), DataTypes.LongType, new DataType[] {DataTypes.LongType}, 100,
                getExecutionContext());
        Assert.assertTrue(accumulator instanceof LongBitXorAccumulator);
    }

    @Test
    public void testFirstValueAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new InternalFirstValue(0, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof FirstValueAccumulator);
    }

    @Test
    public void testHllAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new HyperLoglog(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof HyperLogLogAccumulator);
    }

    @Test
    public void testPartialHllAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new PartialHyperLoglog(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof PartialHyperLogLogAccumulator);
    }

    @Test
    public void testFinalHllAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new FinalHyperLoglog(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof FinalHyperLogLogAccumulator);
    }

    @Test
    public void testCheckSumAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CheckSum(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof CheckSumAccumulator);
    }

    @Test
    public void testCheckSumMergeAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CheckSumMerge(), DataTypes.LongType, new DataType[] {DataTypes.LongType},
                100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof CheckSumMergeAccumulator);
    }

    @Test
    public void testCheckSumV2Accumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CheckSumV2(new int[] {0}, -1), DataTypes.LongType,
                new DataType[] {DataTypes.LongType}, 100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof CheckSumV2Accumulator);
    }

    @Test
    public void testCheckSumV2MergeAccumulator() {
        Accumulator accumulator =
            AccumulatorBuilders.create(new CheckSumV2Merge(), DataTypes.LongType, new DataType[] {DataTypes.LongType},
                100, getExecutionContext());
        Assert.assertTrue(accumulator instanceof CheckSumV2MergeAccumulator);
    }

    private ExecutionContext getExecutionContext() {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setSchemaName("test_accumulator");
        return executionContext;
    }

}
