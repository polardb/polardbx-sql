package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
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

@RunWith(Parameterized.class)
public class FastMultiplyDecimalTest {

    private final DecimalType leftDecimalType;
    private final DecimalType rightDecimalType;
    private final DecimalType targetDecimalType;
    private final boolean overflow;

    private static final int COUNT = 1024;
    /**
     * 虽然 precision 不在 decimal64 范围内
     * 但是否为 decimal64 取决于实际的值
     */
    private static final int PRECISION = 20;
    private static final int OUTPUT_INDEX = 2;
    private static final int BLOCK_COUNT = 3;

    private final Random random = new Random(System.currentTimeMillis());
    private final Decimal[] targetResult = new Decimal[COUNT];
    private final int[] sel;
    private final boolean withSelection;
    private final ExecutionContext executionContext = new ExecutionContext();

    @Parameterized.Parameters(name = "leftScale={0},rightScale={1},overflow={2},sel={3}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        final int[] scales = {0, 1, 2, 5};
        for (int leftScale : scales) {
            for (int rightScale : scales) {
                list.add(new Object[] {leftScale, rightScale, false, true});
                list.add(new Object[] {leftScale, rightScale, false, false});
                list.add(new Object[] {leftScale, rightScale, true, true});
                list.add(new Object[] {leftScale, rightScale, true, false});
            }
        }
        return list;
    }

    public FastMultiplyDecimalTest(int leftScale, int rightScale,
                                   boolean overflow, boolean withSelection) {
        if (leftScale > PRECISION || rightScale > PRECISION) {
            throw new IllegalArgumentException("Too large scale");
        }
        this.leftDecimalType = new DecimalType(PRECISION, leftScale);
        this.rightDecimalType = new DecimalType(PRECISION, rightScale);
        this.targetDecimalType = new DecimalType(PRECISION * 2, leftScale + rightScale);
        this.overflow = overflow;
        this.withSelection = withSelection;
        if (withSelection) {
            final int offset = 10;
            this.sel = new int[COUNT / 2];
            for (int i = 0; i < sel.length; i++) {
                sel[i] = i + offset;
            }
        } else {
            this.sel = null;
        }
    }

    @Before
    public void before() {
    }

    @Test
    public void testMultiplyDecimal64() {
        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(leftDecimalType, 0, 0);
        children[1] = new InputRefVectorizedExpression(rightDecimalType, 1, 1);
        FastMultiplyDecimalColVectorizedExpression expr = new FastMultiplyDecimalColVectorizedExpression(
            OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal64Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        DecimalBlock rightBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(1));

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
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(j).toString()
                        + " and " + rightBlock.getDecimal(i).toString() + " at " + i,
                    targetResult[j], outputBlock.getDecimal(j));
            }
        } else {
            for (int i = 0; i < COUNT; i++) {
                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(i).toString()
                        + " and " + rightBlock.getDecimal(i).toString() + " at " + i,
                    targetResult[i], outputBlock.getDecimal(i));
            }
        }
    }

    @Test
    public void testMultiplySimple() {
        if (leftDecimalType.getScale() == 0
            || rightDecimalType.getScale() == 0) {
            // scale=0 is not considered as a simple decimal
            return;
        }

        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(leftDecimalType, 0, 0);
        children[1] = new InputRefVectorizedExpression(rightDecimalType, 1, 1);
        FastMultiplyDecimalColVectorizedExpression expr = new FastMultiplyDecimalColVectorizedExpression(
            OUTPUT_INDEX, children);

        MutableChunk chunk = buildSimpleChunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        DecimalBlock rightBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(1));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertFalse("Expect to be allocated after evaluation", outputBlock.isUnalloc());
        if (!overflow) {
            if (leftDecimalType.getScale() + rightBlock.getScale() > 9) {
                // 超出了simple的小数范围
                Assert.assertTrue("Output should be simple when not overflowed, but got: " + outputBlock.getState(),
                    outputBlock.getState().isFull());
            } else {
                if (withSelection) {
                    Assert.assertTrue(
                        "Expect output block to simple when input is simple, got: " + outputBlock.getState(),
                        outputBlock.getState().isFull());
                } else {
                    // simple mode does not support selection
                    Assert.assertTrue("Expect output block to full when input is simple with selection, got: "
                            + outputBlock.getState(),
                        outputBlock.isSimple());
                }
            }
        } else {
            // simple 模式可能是full 可能是simple
            // 比如 simple2 和 simple3 进行merge
            Assert.assertTrue("There is no overflow in simple mode, but got: " + outputBlock.getState(),
                outputBlock.isSimple() || outputBlock.getState().isFull());
        }

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(j).toString()
                        + " and " + rightBlock.getDecimal(i).toString() + " at " + i,
                    targetResult[j], outputBlock.getDecimal(j));
            }
        } else {
            for (int i = 0; i < COUNT; i++) {
                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(i).toString()
                        + " and " + rightBlock.getDecimal(i).toString() + " at " + i,
                    targetResult[i], outputBlock.getDecimal(i));
            }
        }
    }

    private MutableChunk buildDecimal64Chunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[2] = new DecimalBlock(targetDecimalType, COUNT);

        DecimalBlockBuilder leftBuilder = new DecimalBlockBuilder(COUNT, leftDecimalType);
        DecimalBlockBuilder rightBuilder = new DecimalBlockBuilder(COUNT, rightDecimalType);
        for (int i = 0; i < COUNT; i++) {
            long left;
            long right;
            if (i == 0) {
                left = 0;
                right = genDecimal64NotOverflowLong();
            } else if (i == 1) {
                left = 1;
                right = genDecimal64NotOverflowLong();
            } else if (i == 2) {
                left = 1;
                right = genDecimal64NotOverflowLong();
            } else if (i == 3) {
                left = -1;
                right = genDecimal64NotOverflowLong();
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
                    right = genDecimal64NotOverflowLong();
                    if (i % 5 == 0) {
                        right = -right;
                    }
                } else {
                    left = genDecimal64OverflowLong();
                    right = genDecimal64OverflowLong();
                }
            }

            leftBuilder.writeLong(left);
            rightBuilder.writeLong(right);

            Decimal target = new Decimal();
            FastDecimalUtils.mul(
                new Decimal(left, leftDecimalType.getScale()).getDecimalStructure(),
                new Decimal(right, rightDecimalType.getScale()).getDecimalStructure(),
                target.getDecimalStructure());
            targetResult[i] = target;
        }
        DecimalBlock leftBlock = (DecimalBlock) leftBuilder.build();
        blocks[0] = leftBlock;
        Assert.assertTrue(leftBlock.isDecimal64());

        DecimalBlock rightBlock = (DecimalBlock) rightBuilder.build();
        blocks[1] = rightBlock;
        Assert.assertTrue(rightBlock.isDecimal64());

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setSelectionInUse(true);
            chunk.setSelection(sel);
            chunk.setBatchSize(sel.length);
        }
        return chunk;
    }

    private MutableChunk buildSimpleChunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[2] = new DecimalBlock(targetDecimalType, COUNT);

        DecimalBlockBuilder leftBuilder = new DecimalBlockBuilder(COUNT, leftDecimalType);
        DecimalBlockBuilder rightBuilder = new DecimalBlockBuilder(COUNT, rightDecimalType);
        for (int i = 0; i < COUNT; i++) {
            long left;
            long right;
            // simple 只能是正数
            if (!overflow || i % 2 == 0) {
                left = genDecimal64NotOverflowLong();
                right = genDecimal64NotOverflowLong();
            } else {
                left = genDecimal64OverflowLong();
                right = genDecimal64OverflowLong();
            }
            Decimal leftDecimal = new Decimal(left, leftDecimalType.getScale());
            Decimal rightDecimal = new Decimal(right, rightDecimalType.getScale());
            leftBuilder.writeDecimal(leftDecimal);
            rightBuilder.writeDecimal(rightDecimal);

            Decimal target = new Decimal();
            FastDecimalUtils.mul(leftDecimal.getDecimalStructure(), rightDecimal.getDecimalStructure(),
                target.getDecimalStructure());
            targetResult[i] = target;
        }
        DecimalBlock leftBlock = (DecimalBlock) leftBuilder.build();
        blocks[0] = leftBlock;
        Assert.assertTrue(leftBlock.isSimple());

        DecimalBlock rightBlock = (DecimalBlock) rightBuilder.build();
        blocks[1] = rightBlock;
        Assert.assertTrue(rightBlock.isSimple());

        MutableChunk chunk = new MutableChunk(blocks);
        if (withSelection) {
            chunk.setSelectionInUse(true);
            chunk.setSelection(sel);
            chunk.setBatchSize(sel.length);
        }
        return chunk;
    }

    private long genDecimal64NotOverflowLong() {
        return random.nextInt(9999999) + 10_000_000;
    }

    /**
     * 对于两个这种数相乘 结果会溢出long
     */
    private long genDecimal64OverflowLong() {
        return random.nextInt(9999999) + 9000_000_000L;
    }
}
