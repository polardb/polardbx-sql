package com.alibaba.polardbx.executor.vectorized.convert;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.DoubleBlockBuilder;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RunWith(Parameterized.class)
public class CastExpressionTest {

    private final int count = 1024;
    private final boolean withSelection;
    private final int[] sel;
    private final int positionCount;
    private final Random random;
    private ExecutionContext executionContext;

    public CastExpressionTest(boolean withSelection) {
        this.random = new Random();
        this.withSelection = withSelection;
        if (withSelection) {
            this.sel = new int[count / 2];
            for (int i = 0; i < sel.length; i++) {
                this.sel[i] = i * 2;
            }
            this.positionCount = sel.length;
        } else {
            this.sel = null;
            this.positionCount = count;
        }
    }

    @Parameterized.Parameters(name = "sel={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[] {false});
        list.add(new Object[] {true});

        return list;
    }

    @Before
    public void before() {
        this.executionContext = new ExecutionContext();
    }

    @Test
    public void testLongToDec64() {
        final DataType outputType = new DecimalType(16, 2);
        final long value = 1234567L;

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new LiteralVectorizedExpression(DataTypes.LongType, value, 0);
        CastLongConstToDecimalVectorizedExpression expr =
            new CastLongConstToDecimalVectorizedExpression(outputType, 1, children);
        LongBlock longBlock = new LongBlock(DataTypes.LongType, count);
        DecimalBlock outputBlock = new DecimalBlock(outputType, count);
        MutableChunk chunk = new MutableChunk(longBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        Assert.assertTrue(outputBlock.isDecimal64());
        if (!withSelection) {
            for (int i = 0; i < positionCount; i++) {
                // since scale is 2
                Assert.assertEquals(value * 100, outputBlock.getLong(i));
            }
        } else {
            for (int i = 0; i < positionCount; i++) {
                int j = sel[i];
                // since scale is 2
                Assert.assertEquals(value * 100, outputBlock.getLong(j));
            }
        }
    }

    /**
     * scale is overflow the max power of 10
     */
    @Test
    public void testLongToDecOverflow1() {
        final DataType outputType = new DecimalType(32, 11);
        final long value = 123456789101L;
        final Decimal decimal = new Decimal(value, 0);

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new LiteralVectorizedExpression(DataTypes.LongType, value, 0);
        CastLongConstToDecimalVectorizedExpression expr =
            new CastLongConstToDecimalVectorizedExpression(outputType, 1, children);
        LongBlock longBlock = new LongBlock(DataTypes.LongType, count);
        DecimalBlock outputBlock = new DecimalBlock(outputType, count);
        MutableChunk chunk = new MutableChunk(longBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        Assert.assertFalse(outputBlock.isDecimal64());
        Assert.assertTrue(outputBlock.getState().isNormal());

        if (!withSelection) {
            for (int i = 0; i < positionCount; i++) {
                Decimal actual = outputBlock.getDecimal(i);
                // since scale is 2
                Assert.assertEquals("Actual output is: " + actual, 0,
                    FastDecimalUtils.compare(decimal.getDecimalStructure(),
                        actual.getDecimalStructure()));
            }
        } else {
            for (int i = 0; i < positionCount; i++) {
                int j = sel[i];
                Decimal actual = outputBlock.getDecimal(j);
                // since scale is 2
                Assert.assertEquals("Actual output is: " + actual, 0,
                    FastDecimalUtils.compare(decimal.getDecimalStructure(),
                        actual.getDecimalStructure()));
            }
        }
    }

    @Test
    public void testDecimalToDecimal() {
        final DecimalType fromDecimalType = new DecimalType(18, 4);
        final DecimalType toDecimalType = new DecimalType(18, 2);
        DecimalBlockBuilder builder = new DecimalBlockBuilder(count, fromDecimalType);
        Decimal[] result = new Decimal[count];
        for (int i = 0; i < count; i++) {
            int val = random.nextInt();
            Decimal decimal = new Decimal(val, fromDecimalType.getScale());
            Decimal expectResult = new Decimal();
            DecimalConverter.rescale(decimal.getDecimalStructure(), expectResult.getDecimalStructure(),
                toDecimalType.getPrecision(), toDecimalType.getScale(), false);
            result[i] = expectResult;
            builder.writeDecimal(decimal);
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(fromDecimalType, 0, 0);
        CastDecimalToDecimalVectorizedExpression expr =
            new CastDecimalToDecimalVectorizedExpression(toDecimalType, 1, children);
        DecimalBlock inputBlock = (DecimalBlock) builder.build();
        DecimalBlock outputBlock = new DecimalBlock(toDecimalType, count);
        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDecimalResult(outputBlock, result);
    }

    @Test
    public void testDoubleToDecimal() {
        final DecimalType toDecimalType = new DecimalType(18, 2);
        DoubleBlockBuilder builder = new DoubleBlockBuilder(count);
        Decimal[] result = new Decimal[count];
        for (int i = 0; i < count; i++) {
            double val = random.nextDouble();
            Decimal decimalVal = Decimal.fromString(String.valueOf(val));
            Decimal expectResult = new Decimal();
            DecimalConverter.rescale(decimalVal.getDecimalStructure(), expectResult.getDecimalStructure(),
                toDecimalType.getPrecision(), toDecimalType.getScale(), false);
            result[i] = expectResult;
            builder.writeDouble(val);
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.DoubleType, 0, 0);
        CastDoubleToDecimalVectorizedExpression expr =
            new CastDoubleToDecimalVectorizedExpression(toDecimalType, 1, children);
        DoubleBlock inputBlock = (DoubleBlock) builder.build();
        DecimalBlock outputBlock = new DecimalBlock(toDecimalType, count);
        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDecimalResult(outputBlock, result);
    }

    @Test
    public void testFloatToDecimal() {
        final DecimalType toDecimalType = new DecimalType(18, 2);
        FloatBlockBuilder builder = new FloatBlockBuilder(count);
        Decimal[] result = new Decimal[count];
        for (int i = 0; i < count; i++) {
            float val = random.nextFloat();
            Decimal decimalVal = Decimal.fromString(String.valueOf(val));
            Decimal expectResult = new Decimal();
            DecimalConverter.rescale(decimalVal.getDecimalStructure(), expectResult.getDecimalStructure(),
                toDecimalType.getPrecision(), toDecimalType.getScale(), false);
            result[i] = expectResult;
            builder.writeFloat(val);
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.FloatType, 0, 0);
        CastFloatToDecimalVectorizedExpression expr =
            new CastFloatToDecimalVectorizedExpression(toDecimalType, 1, children);
        FloatBlock inputBlock = (FloatBlock) builder.build();
        DecimalBlock outputBlock = new DecimalBlock(toDecimalType, count);
        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDecimalResult(outputBlock, result);
    }

    @Test
    public void testULongToDecimal() {
        final DecimalType toDecimalType = new DecimalType(32, 2);
        ULongBlockBuilder builder = new ULongBlockBuilder(count);
        Decimal[] result = new Decimal[count];
        for (int i = 0; i < count; i++) {
            long val;
            if (i == 0) {
                // -1 作为 uint64 是 18446744073709551615
                val = -1;
                Decimal expectResult = Decimal.fromString("18446744073709551615.00");
                result[i] = expectResult;
            } else {
                val = random.nextLong();
                Decimal decimalVal = new Decimal();
                DecimalConverter.unsignedlongToDecimal(val, decimalVal.getDecimalStructure());
                Decimal expectResult = new Decimal();
                DecimalConverter.rescale(decimalVal.getDecimalStructure(), expectResult.getDecimalStructure(),
                    toDecimalType.getPrecision(), toDecimalType.getScale(), true);
                result[i] = expectResult;
            }
            builder.writeLong(val);
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.ULongType, 0, 0);
        CastULongToDecimalVectorizedExpression expr =
            new CastULongToDecimalVectorizedExpression(toDecimalType, 1, children);
        ULongBlock inputBlock = (ULongBlock) builder.build();
        DecimalBlock outputBlock = new DecimalBlock(toDecimalType, count);
        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDecimalResult(outputBlock, result);
    }

    @Test
    public void testDecimalToLong() {
        final DecimalType toDecimalType = new DecimalType(32, 2);
        DecimalBlockBuilder builder = new DecimalBlockBuilder(count);
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            double val = random.nextDouble();
            Decimal decimalVal = Decimal.fromString(String.valueOf(val));
            result[i] = Math.round(val);
            builder.writeDecimal(decimalVal);
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(toDecimalType, 0, 0);
        CastDecimalToSignedVectorizedExpression expr =
            new CastDecimalToSignedVectorizedExpression(DataTypes.LongType, 1, children);
        DecimalBlock inputBlock = (DecimalBlock) builder.build();
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToSignedResult(outputBlock, result);
    }

    @Test
    public void testDecimalToULong() {
        final DecimalType decimalType = new DecimalType(32, 2);
        DecimalBlockBuilder builder = new DecimalBlockBuilder(count);
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            double val = random.nextDouble();
            Decimal decimalVal = Decimal.fromString(String.valueOf(val));
            result[i] = Math.round(val);
            builder.writeDecimal(decimalVal);
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(decimalType, 0, 0);
        CastDecimalToUnsignedVectorizedExpression expr =
            new CastDecimalToUnsignedVectorizedExpression(DataTypes.UIntegerType, 1, children);
        DecimalBlock inputBlock = (DecimalBlock) builder.build();
        ULongBlock outputBlock = new ULongBlock(DataTypes.UIntegerType, count);
        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToUnsignedResult(outputBlock, result);
    }

    @Test
    public void testCharToSigned() {
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            long l = random.nextLong();
            result[i] = l;
            builder.writeObject(Slices.utf8Slice(Long.toString(l)));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastCharToSignedVectorizedExpression expr =
            new CastCharToSignedVectorizedExpression(DataTypes.LongType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToSignedResult(outputBlock, result);
    }

    @Test
    public void testCharToUnsigned() {
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            long l = random.nextLong();
            result[i] = l;
            builder.writeObject(Slices.utf8Slice(Long.toString(l)));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastCharToUnsignedVectorizedExpression expr =
            new CastCharToUnsignedVectorizedExpression(DataTypes.UIntegerType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        ULongBlock outputBlock = new ULongBlock(DataTypes.UIntegerType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToUnsignedResult(outputBlock, result);
    }

    @Test
    public void testCharToDouble() {
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        double[] result = new double[count];
        for (int i = 0; i < count; i++) {
            double d = random.nextDouble();
            result[i] = d;
            builder.writeObject(Slices.utf8Slice(Double.toString(d)));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastCharToDoubleVectorizedExpression expr =
            new CastCharToDoubleVectorizedExpression(DataTypes.DoubleType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        DoubleBlock outputBlock = new DoubleBlock(DataTypes.DoubleType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDoubleResult(outputBlock, result);
    }

    @Test
    public void testCharToDecimal() {
        final DecimalType decimalType = new DecimalType(18, 2);
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        Decimal[] result = new Decimal[count];
        for (int i = 0; i < count; i++) {
            int val = random.nextInt();
            Decimal decimal = new Decimal(val, decimalType.getScale());
            result[i] = decimal;
            builder.writeObject(Slices.utf8Slice(decimal.toString()));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastCharToDecimalVectorizedExpression expr =
            new CastCharToDecimalVectorizedExpression(decimalType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        DecimalBlock outputBlock = new DecimalBlock(decimalType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDecimalResult(outputBlock, result);
    }

    @Test
    public void testVarcharToSigned() {
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            long l = random.nextLong();
            result[i] = l;
            builder.writeObject(Slices.utf8Slice(Long.toString(l)));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastVarcharToSignedVectorizedExpression expr =
            new CastVarcharToSignedVectorizedExpression(DataTypes.LongType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        LongBlock outputBlock = new LongBlock(DataTypes.LongType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToSignedResult(outputBlock, result);
    }

    @Test
    public void testVarcharToUnsigned() {
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        long[] result = new long[count];
        for (int i = 0; i < count; i++) {
            long l = random.nextLong();
            result[i] = l;
            builder.writeObject(Slices.utf8Slice(Long.toString(l)));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastVarcharToUnsignedVectorizedExpression expr =
            new CastVarcharToUnsignedVectorizedExpression(DataTypes.UIntegerType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        ULongBlock outputBlock = new ULongBlock(DataTypes.UIntegerType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToUnsignedResult(outputBlock, result);
    }

    @Test
    public void testVarcharToDouble() {
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        double[] result = new double[count];
        for (int i = 0; i < count; i++) {
            double d = random.nextDouble();
            result[i] = d;
            builder.writeObject(Slices.utf8Slice(Double.toString(d)));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastVarcharToDoubleVectorizedExpression expr =
            new CastVarcharToDoubleVectorizedExpression(DataTypes.DoubleType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        DoubleBlock outputBlock = new DoubleBlock(DataTypes.DoubleType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDoubleResult(outputBlock, result);
    }

    @Test
    public void testVarcharToDecimal() {
        final DecimalType decimalType = new DecimalType(18, 2);
        ObjectBlockBuilder builder = new ObjectBlockBuilder(count);
        Decimal[] result = new Decimal[count];
        for (int i = 0; i < count; i++) {
            int val = random.nextInt();
            Decimal decimal = new Decimal(val, decimalType.getScale());
            result[i] = decimal;
            builder.writeObject(Slices.utf8Slice(decimal.toString()));
        }

        VectorizedExpression[] children = new VectorizedExpression[1];
        children[0] = new InputRefVectorizedExpression(DataTypes.CharType, 0, 0);
        CastVarcharToDecimalVectorizedExpression expr =
            new CastVarcharToDecimalVectorizedExpression(decimalType, 1, children);
        ReferenceBlock refBlock = (ReferenceBlock) builder.build();
        DecimalBlock outputBlock = new DecimalBlock(decimalType, count);
        MutableChunk chunk = new MutableChunk(refBlock, outputBlock);
        setSelection(chunk);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        validateToDecimalResult(outputBlock, result);
    }

    private void validateToSignedResult(LongBlock outputBlock, long[] result) {
        if (!withSelection) {
            for (int i = 0; i < positionCount; i++) {
                Assert.assertEquals(result[i], outputBlock.getLong(i));
            }
        } else {
            for (int i = 0; i < positionCount; i++) {
                int j = sel[i];
                Assert.assertEquals(result[j], outputBlock.getLong(j));
            }
        }
    }

    private void validateToUnsignedResult(ULongBlock outputBlock, long[] result) {
        if (!withSelection) {
            for (int i = 0; i < positionCount; i++) {
                Assert.assertEquals(result[i], outputBlock.getLong(i));
            }
        } else {
            for (int i = 0; i < positionCount; i++) {
                int j = sel[i];
                Assert.assertEquals(result[j], outputBlock.getLong(j));
            }
        }
    }

    private void validateToDoubleResult(DoubleBlock outputBlock, double[] result) {
        if (!withSelection) {
            for (int i = 0; i < positionCount; i++) {
                Assert.assertEquals(result[i], outputBlock.getDouble(i), 1e-10);
            }
        } else {
            for (int i = 0; i < positionCount; i++) {
                int j = sel[i];
                Assert.assertEquals(result[j], outputBlock.getDouble(j), 1e-10);
            }
        }
    }

    private void validateToDecimalResult(DecimalBlock outputBlock, Decimal[] result) {
        if (!withSelection) {
            for (int i = 0; i < positionCount; i++) {
                Assert.assertEquals(result[i].toString(), outputBlock.getDecimal(i).toString());
            }
        } else {
            for (int i = 0; i < positionCount; i++) {
                int j = sel[i];
                Assert.assertEquals(result[j].toString(), outputBlock.getDecimal(j).toString());
            }
        }
    }

    private void setSelection(MutableChunk chunk) {
        if (!withSelection) {
            return;
        }
        chunk.setSelection(sel);
        chunk.setBatchSize(sel.length);
        chunk.setSelectionInUse(true);
    }
}
