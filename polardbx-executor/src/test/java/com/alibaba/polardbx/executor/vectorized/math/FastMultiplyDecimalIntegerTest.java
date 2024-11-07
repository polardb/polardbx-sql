package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlockBuilder;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static com.alibaba.polardbx.executor.utils.DecimalTestUtil.gen128BitUnsignedNumStr;

@RunWith(Parameterized.class)
public class FastMultiplyDecimalIntegerTest {

    private static final int COUNT = 1024;
    private static final int PRECISION = 20;
    private static final int OUTPUT_INDEX = 2;
    private static final int BLOCK_COUNT = 3;
    private final DecimalType leftDecimalType;
    private final DecimalType targetDecimalType;
    private final boolean overflow;
    private final Random random = new Random(System.currentTimeMillis());
    private final Decimal[] targetResult = new Decimal[COUNT];
    private final boolean withSelection;
    private final int[] sel;

    private final ExecutionContext executionContext = new ExecutionContext();

    public FastMultiplyDecimalIntegerTest(int leftScale, boolean overflow,
                                          boolean withSelection) {
        if (leftScale > PRECISION) {
            throw new IllegalArgumentException("Too large scale");
        }
        this.leftDecimalType = new DecimalType(PRECISION, leftScale);
        this.targetDecimalType = new DecimalType(PRECISION * 2, leftScale);
        this.overflow = overflow;
        this.withSelection = withSelection;
        if (withSelection) {
            this.sel = new int[COUNT / 2];
            int offset = 10;
            for (int i = 0; i < sel.length; i++) {
                this.sel[i] = i + offset;
            }
        } else {
            this.sel = null;
        }
    }

    @Parameterized.Parameters(name = "leftScale={0},overflow={1},sel={2}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        final int[] scales = {0, 2, 5};
        for (int leftScale : scales) {
            list.add(new Object[] {leftScale, false, false});
            list.add(new Object[] {leftScale, true, false});
            list.add(new Object[] {leftScale, false, true});
            list.add(new Object[] {leftScale, true, true});
        }
        return list;
    }

    @Before
    public void before() {
    }

    @Test
    public void testMultiplyDecimal64() {
        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(leftDecimalType, 0, 0);
        children[1] = new InputRefVectorizedExpression(DataTypes.IntegerType, 1, 1);
        FastMultiplyDecimalColIntegerColVectorizedExpression expr =
            new FastMultiplyDecimalColIntegerColVectorizedExpression(
                OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal64Chunk(false);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        IntegerBlock rightBlock = (IntegerBlock) Objects.requireNonNull(chunk.slotIn(1));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertFalse("Expect to be allocated after evaluation", outputBlock.isUnalloc());
        if (!overflow) {
            Assert.assertTrue("Output should be decimal64 when not overflowed", outputBlock.isDecimal64());
        } else {
            Assert.assertTrue("Output should be decimal128 when overflowed", outputBlock.isDecimal128());
        }

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        validateResult(outputBlock, leftBlock, rightBlock);
    }

    @Test
    public void testMultiplyDecimal64Unsigned() {
        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(leftDecimalType, 0, 0);
        children[1] = new InputRefVectorizedExpression(DataTypes.UIntegerType, 1, 1);
        FastMultiplyDecimalColIntegerColVectorizedExpression expr =
            new FastMultiplyDecimalColIntegerColVectorizedExpression(
                OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal64Chunk(true);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        IntegerBlock rightBlock = (IntegerBlock) Objects.requireNonNull(chunk.slotIn(1));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertFalse("Expect to be allocated after evaluation", outputBlock.isUnalloc());
        if (!overflow) {
            Assert.assertTrue("Output should be decimal64/128 when not overflowed",
                outputBlock.isDecimal64() || outputBlock.isDecimal128());
        } else {
            Assert.assertTrue("Output should be decimal128 when overflowed", outputBlock.isDecimal128());
        }

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        validateResult(outputBlock, leftBlock, rightBlock);
    }

    @Test
    public void testMultiplyDecimal128() {
        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(leftDecimalType, 0, 0);
        children[1] = new InputRefVectorizedExpression(DataTypes.IntegerType, 1, 1);
        FastMultiplyDecimalColIntegerColVectorizedExpression expr =
            new FastMultiplyDecimalColIntegerColVectorizedExpression(
                OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal128Chunk(false);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        IntegerBlock rightBlock = (IntegerBlock) Objects.requireNonNull(chunk.slotIn(1));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertFalse("Expect to be allocated after evaluation", outputBlock.isUnalloc());
        if (!overflow) {
            Assert.assertTrue("Output should be decimal128 when not overflowed", outputBlock.isDecimal128());
        } else {
            Assert.assertTrue("Output should be full when overflowed", outputBlock.getState().isFull());
        }

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        validateResult(outputBlock, leftBlock, rightBlock);
    }

    @Test
    public void testMultiplyDecimal128Unsigned() {
        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(leftDecimalType, 0, 0);
        children[1] = new InputRefVectorizedExpression(DataTypes.UIntegerType, 1, 1);
        FastMultiplyDecimalColIntegerColVectorizedExpression expr =
            new FastMultiplyDecimalColIntegerColVectorizedExpression(
                OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal128Chunk(true);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        IntegerBlock rightBlock = (IntegerBlock) Objects.requireNonNull(chunk.slotIn(1));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertFalse("Expect to be allocated after evaluation", outputBlock.isUnalloc());
        if (!overflow) {
            Assert.assertTrue("Output should be decimal128 when not overflowed", outputBlock.isDecimal128());
        } else {
            Assert.assertTrue("Output should be full when overflowed", outputBlock.getState().isFull());
        }

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        validateResult(outputBlock, leftBlock, rightBlock);
    }

    @Test
    public void testMultiplyNormal() {
        if (leftDecimalType.getScale() == 0) {
            // scale=0 is not considered as a simple decimal
            return;
        }

        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(leftDecimalType, 0, 0);
        children[1] = new InputRefVectorizedExpression(DataTypes.IntegerType, 1, 1);
        FastMultiplyDecimalColIntegerColVectorizedExpression expr =
            new FastMultiplyDecimalColIntegerColVectorizedExpression(
                OUTPUT_INDEX, children);

        MutableChunk chunk = buildSimpleChunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        IntegerBlock rightBlock = (IntegerBlock) Objects.requireNonNull(chunk.slotIn(1));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertFalse("Expect to be allocated after evaluation", outputBlock.isUnalloc());

        Assert.assertTrue("Output should be full when overflowed", outputBlock.getState().isFull());

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        validateResult(outputBlock, leftBlock, rightBlock);
    }

    private void validateResult(DecimalBlock outputBlock, DecimalBlock leftBlock, IntegerBlock rightBlock) {
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(j).toString()
                        + " and " + rightBlock.getInt(j) + " at selection: " + i,
                    targetResult[j], outputBlock.getDecimal(j));
            }
        } else {
            for (int i = 0; i < COUNT; i++) {
                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(i).toString()
                        + " and " + rightBlock.getInt(i) + " at " + i,
                    targetResult[i], outputBlock.getDecimal(i));
            }
        }
    }

    private MutableChunk buildDecimal64Chunk(boolean isUnsigned) {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[2] = new DecimalBlock(targetDecimalType, COUNT);

        DecimalBlockBuilder leftBuilder = new DecimalBlockBuilder(COUNT, leftDecimalType);
        IntegerBlockBuilder rightBuilder = new IntegerBlockBuilder(COUNT);
        Decimal rightDecimal = new Decimal();

        for (int i = 0; i < COUNT; i++) {
            long left;
            int right;
            if (i == 0) {
                left = 1;
                right = 1;
            } else if (i == 1) {
                left = -1;
                right = -1;
            } else if (i == 2) {
                left = 1;
                right = genSmallInt();
            } else if (i == 3) {
                left = -1;
                right = genSmallInt();
            } else if (i == 4) {
                left = genDecimal64NotOverflowLong();
                right = -1;
            } else if (i == 5) {
                left = genDecimal64NotOverflowLong();
                right = 1;
            } else {
                if (!overflow || i % 2 == 0) {
                    left = genDecimal64NotOverflowLong();
                    // 穿插正负数
                    if (i % 3 == 0) {
                        left = -left;
                    }
                    right = genSmallInt();
                    if (i % 5 == 0) {
                        right = -right;
                    }
                } else {
                    left = genDecimal64OverflowLong();
                    right = genLargeInt();
                }
            }

            leftBuilder.writeLong(left);
            rightBuilder.writeInt(right);

            Decimal target = new Decimal();
            if (isUnsigned) {
                DecimalConverter.longToDecimal(right & 0xFFFFFFFFL, rightDecimal.getDecimalStructure());
                FastDecimalUtils.mul(
                    new Decimal(left, leftDecimalType.getScale()).getDecimalStructure(),
                    rightDecimal.getDecimalStructure(),
                    target.getDecimalStructure());
            } else {
                DecimalConverter.longToDecimal(right, rightDecimal.getDecimalStructure(), false);
                FastDecimalUtils.mul(
                    new Decimal(left, leftDecimalType.getScale()).getDecimalStructure(),
                    rightDecimal.getDecimalStructure(),
                    target.getDecimalStructure());
            }

            targetResult[i] = target;
        }
        DecimalBlock leftBlock = (DecimalBlock) leftBuilder.build();
        blocks[0] = leftBlock;
        Assert.assertTrue(leftBlock.isDecimal64());

        IntegerBlock rightBlock = (IntegerBlock) rightBuilder.build();
        blocks[1] = rightBlock;

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelectionInUse(true);
            chunk.setSelection(sel);
        }
        return chunk;
    }

    private MutableChunk buildDecimal128Chunk(boolean isUnsigned) {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[2] = new DecimalBlock(targetDecimalType, COUNT);

        int scale = leftDecimalType.getScale();
        DecimalBlockBuilder leftBuilder = new DecimalBlockBuilder(COUNT, leftDecimalType);
        IntegerBlockBuilder rightBuilder = new IntegerBlockBuilder(COUNT);
        Decimal rightDecimal = new Decimal();

        for (int i = 0; i < COUNT; i++) {
            int right;
            Decimal decimal;
            if (i == 0) {
                right = 1;
                Decimal writeDec = Decimal.fromString("1");
                FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
                writeDec.getDecimalStructure().setFractions(scale);
                long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
                leftBuilder.writeDecimal128(decimal128[0], decimal128[1]);
                decimal = writeDec;
            } else if (i == 1) {
                right = -1;
                Decimal writeDec = Decimal.fromString("-1");
                FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
                writeDec.getDecimalStructure().setFractions(scale);
                long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
                leftBuilder.writeDecimal128(decimal128[0], decimal128[1]);
                decimal = writeDec;
            } else if (i == 2) {
                right = 0;
                Decimal writeDec = Decimal.fromString("0");
                FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
                writeDec.getDecimalStructure().setFractions(scale);
                long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
                leftBuilder.writeDecimal128(decimal128[0], decimal128[1]);
                decimal = writeDec;
            } else {
                if (!overflow) {
                    String decStr = gen128BitUnsignedNumStr(random, 5, false);
                    if (i % 2 == 0) {
                        decStr = "-" + decStr;
                    }

                    Decimal writeDec = Decimal.fromString(decStr);
                    FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -scale);
                    writeDec.getDecimalStructure().setFractions(scale);
                    long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
                    leftBuilder.writeDecimal128(decimal128[0], decimal128[1]);
                    decimal = writeDec;
                    right = genSmallInt();

                } else {
                    DecimalStructure buffer = new DecimalStructure();
                    DecimalStructure result = new DecimalStructure();
                    // 低 64 位是 unsigned
                    long[] decimal128 = {-1, Long.MAX_VALUE};
                    FastDecimalUtils.setDecimal128WithScale(buffer, result, decimal128[0], decimal128[1], scale);
                    leftBuilder.writeDecimal128(decimal128[0], decimal128[1]);
                    decimal = new Decimal(result);
                    right = genLargeInt();
                }
            }

            rightBuilder.writeInt(right);
            DecimalConverter.longToDecimal(right, rightDecimal.getDecimalStructure(), false);

            Decimal target = new Decimal();
            if (isUnsigned) {
                DecimalConverter.longToDecimal(right & 0xFFFFFFFFL, rightDecimal.getDecimalStructure());
                FastDecimalUtils.mul(decimal.getDecimalStructure(),
                    rightDecimal.getDecimalStructure(),
                    target.getDecimalStructure());
            } else {
                DecimalConverter.longToDecimal(right, rightDecimal.getDecimalStructure(), false);
                FastDecimalUtils.mul(decimal.getDecimalStructure(),
                    rightDecimal.getDecimalStructure(),
                    target.getDecimalStructure());
            }
            targetResult[i] = target;
        }
        DecimalBlock leftBlock = (DecimalBlock) leftBuilder.build();
        blocks[0] = leftBlock;
        Assert.assertTrue(leftBlock.isDecimal128());

        IntegerBlock rightBlock = (IntegerBlock) rightBuilder.build();
        blocks[1] = rightBlock;

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelectionInUse(true);
            chunk.setSelection(sel);
        }
        return chunk;
    }

    private MutableChunk buildSimpleChunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[2] = new DecimalBlock(targetDecimalType, COUNT);

        DecimalBlockBuilder leftBuilder = new DecimalBlockBuilder(COUNT, leftDecimalType);
        IntegerBlockBuilder rightBuilder = new IntegerBlockBuilder(COUNT);
        Decimal rightDecimal = new Decimal();

        for (int i = 0; i < COUNT; i++) {
            long left;
            int right;
            // simple 只能是正数
            left = genDecimal64NotOverflowLong();
            right = genSmallInt();
            Decimal leftDecimal = new Decimal(left, leftDecimalType.getScale());
            DecimalConverter.longToDecimal(right, rightDecimal.getDecimalStructure(), false);
            leftBuilder.writeDecimal(leftDecimal);
            rightBuilder.writeInt(right);

            Decimal target = new Decimal();
            FastDecimalUtils.mul(leftDecimal.getDecimalStructure(), rightDecimal.getDecimalStructure(),
                target.getDecimalStructure());
            targetResult[i] = target;
        }
        DecimalBlock leftBlock = (DecimalBlock) leftBuilder.build();
        blocks[0] = leftBlock;
        Assert.assertTrue(leftBlock.isSimple());

        IntegerBlock rightBlock = (IntegerBlock) rightBuilder.build();
        blocks[1] = rightBlock;

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelectionInUse(true);
            chunk.setSelection(sel);
        }
        return chunk;
    }

    private long genDecimal64NotOverflowLong() {
        return random.nextInt(9999999) + 10_000_000;
    }

    private int genSmallInt() {
        return random.nextInt(1000);
    }

    private int genLargeInt() {
        return random.nextInt(99_999_999) + 900_000_000;
    }

    private long genDecimal64OverflowLong() {
        return random.nextInt(9999999) + 9000_000_000_000L;
    }
}
