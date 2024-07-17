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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

@RunWith(Parameterized.class)
public class FastSubDecimalDecimalTest {

    private static final int DEFAULT_SCALE = 2;
    private static final int COUNT = 1024;
    private static final int OUTPUT_INDEX = 2;
    private static final int BLOCK_COUNT = 3;
    private final DecimalType decimalType1;
    private final DecimalType decimalType2;
    private final DecimalType resultDecimalType;
    private final Random random = new Random(System.currentTimeMillis());
    private final Decimal[] targetResult = new Decimal[COUNT];
    private final boolean overflow;
    private final InputState inputState;
    private final ExecutionContext executionContext = new ExecutionContext();

    public FastSubDecimalDecimalTest(boolean overflow, String inputState,
                                     int scale1, int scale2) {
        this.overflow = overflow;
        this.inputState = InputState.valueOf(inputState);

        this.decimalType1 = new DecimalType(20, scale1);
        this.decimalType2 = new DecimalType(20, scale2);
        // derive result type
        this.resultDecimalType = new DecimalType(20, Math.max(scale1, scale2));
    }

    /**
     * Mixing different scales
     */
    @Parameterized.Parameters(name = "overflow={0},inputState={1},scale1={2},scale2={3}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        for (InputState value : InputState.values()) {
            list.add(new Object[] {true, value.name(), DEFAULT_SCALE, DEFAULT_SCALE});
            list.add(new Object[] {true, value.name(), DEFAULT_SCALE + 1, DEFAULT_SCALE - 1});
            list.add(new Object[] {true, value.name(), DEFAULT_SCALE, DEFAULT_SCALE + 4});
            list.add(new Object[] {true, value.name(), DEFAULT_SCALE, DEFAULT_SCALE + 10});
            list.add(new Object[] {false, value.name(), DEFAULT_SCALE, DEFAULT_SCALE});
            list.add(new Object[] {false, value.name(), DEFAULT_SCALE + 1, DEFAULT_SCALE - 1});
            list.add(new Object[] {false, value.name(), DEFAULT_SCALE, DEFAULT_SCALE + 4});
            list.add(new Object[] {false, value.name(), DEFAULT_SCALE, DEFAULT_SCALE + 10});
        }

        return list;
    }

    private boolean isDecimal64() {
        return inputState == InputState.DECIMAL_64;
    }

    @Test
    public void testSubDecimalColDecimalCol() {

        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new InputRefVectorizedExpression(decimalType1, 0, 0);
        children[1] = new InputRefVectorizedExpression(decimalType2, 1, 1);
        FastSubDecimalColDecimalColVectorizedExpression expr = new FastSubDecimalColDecimalColVectorizedExpression(
            OUTPUT_INDEX, children);

        MutableChunk chunk = preAllocatedChunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        Assert.assertEquals("Expect left block to be decimal64: " + isDecimal64(),
            leftBlock.isDecimal64(), isDecimal64());

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertFalse("Expect to be allocated after evaluation", outputBlock.isUnalloc());

        if (!overflow) {
            switch (inputState) {
            case DECIMAL_64:
                if (Math.abs(decimalType1.getScale() - decimalType2.getScale())
                    >= FastSubDecimalColDecimalColVectorizedExpression.MAX_SCALE_DIFF) {
                    Assert.assertTrue(
                        "Expect output block to be full when exceeds MAX_SCALE_DIFF, got: " + outputBlock.getState(),
                        outputBlock.getState().isFull());
                } else {
                    Assert.assertTrue(
                        "Expect output block to be decimal64 when not overflowed, got: " + outputBlock.getState(),
                        outputBlock.isDecimal64());
                }
                break;
            case FULL:
                Assert.assertTrue("Expect output block to full when input is full got: " + outputBlock.getState(),
                    outputBlock.getState().isFull());
                break;
            }
        } else {
            switch (inputState) {
            case DECIMAL_64:
                if (decimalType1.getScale() != decimalType2.getScale()) {
                    Assert.assertTrue(
                        "Expect output block to be full when overflowed from decimal_64 with diff scales, got: "
                            + outputBlock.getState(),
                        outputBlock.getState().isFull());
                } else {
                    Assert.assertTrue(
                        "Expect output block to be decimal128 when overflowed from decimal_64, got: "
                            + outputBlock.getState(),
                        outputBlock.isDecimal128());
                }
                break;
            case FULL:
                Assert.assertTrue("Expect output block to full when input is full, got: " + outputBlock.getState(),
                    outputBlock.getState().isFull());
                break;
            }
        }

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(i).toString() + " at " + i,
                targetResult[i], outputBlock.getDecimal(i));
        }
    }

    private MutableChunk preAllocatedChunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[2] = new DecimalBlock(resultDecimalType, COUNT);

        DecimalBlockBuilder builder1 = new DecimalBlockBuilder(COUNT, decimalType1);
        DecimalBlockBuilder builder2 = new DecimalBlockBuilder(COUNT, decimalType2);
        long lastLong = 0;
        Decimal lastDecimal = Decimal.ZERO;
        for (int i = 0; i < COUNT; i++) {
            long l;
            switch (inputState) {
            case DECIMAL_64:
                if (!overflow || i % 2 == 1) {
                    l = genNotOverflowLong();
                } else {
                    // 溢出的情况一半填充MAX值保证溢出
                    l = Long.MIN_VALUE;
                }
                break;
            case FULL:
                if (!overflow || i % 2 == 1) {
                    l = -Math.abs(genLong());
                } else {
                    l = Long.MIN_VALUE;
                }
                break;
            default:
                throw new UnsupportedOperationException();
            }

            Decimal decimal = new Decimal(l, decimalType1.getScale());

            if (isDecimal64()) {
                builder1.writeLong(l);
                builder2.writeLong(lastLong);
            } else {
                builder1.writeDecimal(decimal);
                builder2.writeDecimal(lastDecimal);
            }

            Decimal target = new Decimal();
            FastDecimalUtils.sub(decimal.getDecimalStructure(), lastDecimal.getDecimalStructure(),
                target.getDecimalStructure());
            targetResult[i] = target;

            lastLong = l;
            lastDecimal = new Decimal(l, decimalType2.getScale());
        }
        DecimalBlock decimalBlock1 = (DecimalBlock) builder1.build();
        DecimalBlock decimalBlock2 = (DecimalBlock) builder2.build();
        blocks[0] = decimalBlock1;
        blocks[1] = decimalBlock2;

        switch (inputState) {
        case DECIMAL_64:
            Assert.assertTrue(decimalBlock1.isDecimal64());
            Assert.assertTrue(decimalBlock2.isDecimal64());
            break;
        case FULL:
            Assert.assertTrue(decimalBlock1.getState().isFull());
            Assert.assertTrue(decimalBlock2.getState().isFull());
            break;
        }

        return new MutableChunk(blocks);
    }

    private long genNotOverflowLong() {
        return random.nextInt(99999999) + 100000000;
    }

    private long genLong() {
        return random.nextLong();
    }

    enum InputState {
        DECIMAL_64,
        FULL
    }
}
