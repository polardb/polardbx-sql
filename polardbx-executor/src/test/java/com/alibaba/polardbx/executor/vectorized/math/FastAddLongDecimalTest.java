package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
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

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_DECIMAL_FAST_VEC;
import static com.alibaba.polardbx.executor.utils.DecimalTestUtil.gen128BitUnsignedNumStr;

@RunWith(Parameterized.class)
public class FastAddLongDecimalTest {

    private static final int SCALE = 2;
    private static final int COUNT = 1024;
    private static final int OUTPUT_INDEX = 2;
    private static final int BLOCK_COUNT = 3;
    private final DecimalType decimalType = new DecimalType(20, SCALE);
    private final Random random = new Random(System.currentTimeMillis());
    private final Decimal[] targetResult = new Decimal[COUNT];
    private final boolean overflow;
    private final InputState inputState;
    private final ExecutionContext executionContext = new ExecutionContext();
    private final boolean withSelection;
    private final int[] sel;
    private Long leftConstVal;

    public FastAddLongDecimalTest(boolean overflow, String inputState, boolean withSelection) {
        this.overflow = overflow;
        this.inputState = InputState.valueOf(inputState);
        if (this.inputState == InputState.SIMPLE) {
            executionContext.getParamManager().getProps().put(ENABLE_DECIMAL_FAST_VEC, "true");
        }
        this.withSelection = withSelection;
        if (withSelection) {
            this.sel = new int[COUNT / 2];
            final int offset = 10;
            for (int i = 0; i < sel.length; i++) {
                sel[i] = i + offset;
            }
        } else {
            this.sel = null;
        }
    }

    @Parameterized.Parameters(name = "overflow={0},inputState={1},sel={2}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        for (InputState value : InputState.values()) {
            list.add(new Object[] {true, value.name(), false});
            list.add(new Object[] {true, value.name(), true});
            list.add(new Object[] {false, value.name(), false});
            list.add(new Object[] {false, value.name(), true});
        }

        return list;
    }

    @Before
    public void before() {
        switch (inputState) {
        case DECIMAL_64:
            // 确保scale对齐后常量不会溢出
            leftConstVal = random.nextLong() / DecimalTypeBase.POW_10[SCALE + 1];
            break;
        case DECIMAL_128:
            if (!overflow) {
                leftConstVal = random.nextLong() / DecimalTypeBase.POW_10[SCALE + 1];
            } else {
                leftConstVal = Long.MAX_VALUE - random.nextInt(1000_000);
            }
            break;
        case SIMPLE:
            if (!overflow) {
                leftConstVal = 999_999_999L + random.nextInt(1000_000_000);
            } else {
                leftConstVal = Long.MAX_VALUE - random.nextInt(1000_000);
            }
            break;
        case FULL:
            leftConstVal = random.nextLong();
            break;
        case NULL:
            leftConstVal = null;
            break;
        }
        System.out.println("Left const val: " + leftConstVal);
    }

    private boolean isDecimal64() {
        return inputState == InputState.DECIMAL_64 || inputState == InputState.NULL;
    }

    private boolean isDecimal128() {
        return inputState == InputState.DECIMAL_128;
    }

    @Test
    public void testAddLongConstDecimalVar() {

        final VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new LiteralVectorizedExpression(DataTypes.LongType, leftConstVal, 1);
        children[1] = new InputRefVectorizedExpression(decimalType, 0, 0);
        FastAddLongConstDecimalColVectorizedExpression expr = new FastAddLongConstDecimalColVectorizedExpression(
            OUTPUT_INDEX, children);

        MutableChunk chunk = preAllocatedChunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        DecimalBlock leftBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));
        Assert.assertEquals("Expect left block to be decimal64: " + isDecimal64(),
            leftBlock.isDecimal64(), isDecimal64());
        Assert.assertEquals("Expect left block to be decimal128: " + isDecimal64(),
            leftBlock.isDecimal128(), isDecimal128());
        if (withSelection) {
            chunk.setBatchSize(sel.length);
            chunk.setSelection(sel);
            chunk.setSelectionInUse(true);
        }

        DecimalBlock outputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));

        Assert.assertTrue("Expect to be unallocated before evaluation", outputBlock.isUnalloc());

        expr.eval(evaluationContext);

        Assert.assertEquals("Expect to be allocated after evaluation, otherwise it is null",
            inputState == InputState.NULL, outputBlock.isUnalloc());

        if (!overflow) {
            switch (inputState) {
            case DECIMAL_64:
                Assert.assertTrue(
                    "Expect output block to be decimal64 when not overflowed, got: " + outputBlock.getState(),
                    outputBlock.isDecimal64());
                break;
            case DECIMAL_128:
                Assert.assertTrue(
                    "Expect output block to be decimal128 when not overflowed, got: " + outputBlock.getState(),
                    outputBlock.isDecimal128());
                break;
            case SIMPLE:
                Assert.assertTrue(
                    "Expect output block to simple/full when input is simple, got: " + outputBlock.getState(),
                    outputBlock.isSimple() || outputBlock.getState().isFull());
                break;
            case FULL:
                Assert.assertTrue("Expect output block to full when input is full got: " + outputBlock.getState(),
                    outputBlock.getState().isFull());
                break;
            }
        } else {
            switch (inputState) {
            case DECIMAL_64:
                Assert.assertTrue(
                    "Expect output block to be decimal_128 when overflowed from decimal_64, got: "
                        + outputBlock.getState(),
                    outputBlock.getState().isDecimal128());
                break;
            case DECIMAL_128:
                Assert.assertTrue(
                    "Expect output block to be full when overflowed from decimal_128, got: " + outputBlock.getState(),
                    outputBlock.getState().isFull());
                break;
            case SIMPLE:
                // Overflow 的 SIMPLE_3 不支持简单计算
                Assert.assertTrue(
                    "Expect output block to full when input is simple overflowed, got: " + outputBlock.getState(),
                    outputBlock.getState().isFull());
                break;
            case FULL:
                Assert.assertTrue("Expect output block to full when input is full, got: " + outputBlock.getState(),
                    outputBlock.getState().isFull());
                break;
            }
        }

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        if (withSelection) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];

                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(j).toString() + " at " + j,
                    targetResult[j], outputBlock.isNull(j) ? null : outputBlock.getDecimal(j));
            }
        } else {
            for (int i = 0; i < COUNT; i++) {
                Assert.assertEquals("Incorrect value for: " + leftBlock.getDecimal(i).toString() + " at " + i,
                    targetResult[i], outputBlock.isNull(i) ? null : outputBlock.getDecimal(i));
            }
        }

    }

    private MutableChunk preAllocatedChunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);
        blocks[2] = new DecimalBlock(decimalType, COUNT);

        DecimalBlockBuilder builder = new DecimalBlockBuilder(COUNT, decimalType);
        Decimal decimal;
        for (int i = 0; i < COUNT; i++) {
            long l;
            switch (inputState) {
            case NULL:
                l = genNotOverflowLong();
                decimal = new Decimal(l, SCALE);
                builder.writeLong(l);
                break;
            case DECIMAL_64:
                if (!overflow || i % 2 == 1) {
                    l = genNotOverflowLong();
                } else {
                    // 溢出的情况一半填充MAX值保证溢出
                    l = leftConstVal >= 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
                }
                decimal = new Decimal(l, SCALE);
                builder.writeLong(l);
                break;
            case SIMPLE:
                // SIMPLE 需要是正数
                // 而且单个 block 内需要是一致的 SIMPLE 类型
                if (!overflow) {
                    // 混合SIMPLE类型输入
                    if (i % 2 == 1) {
                        l = Math.abs(genSmallLong());
                    } else {
                        l = Math.abs(genNotOverflowLong());
                    }
                } else {
                    l = Long.MAX_VALUE;
                }
                decimal = new Decimal(l, SCALE);
                builder.writeDecimal(decimal);

                break;
            case DECIMAL_128:
                if (!overflow) {
                    String decStr = gen128BitUnsignedNumStr(random);
                    if (i % 2 == 0) {
                        decStr = "-" + decStr;
                    }

                    Decimal writeDec = Decimal.fromString(decStr);
                    FastDecimalUtils.shift(writeDec.getDecimalStructure(), writeDec.getDecimalStructure(), -SCALE);
                    writeDec.getDecimalStructure().setFractions(SCALE);
                    long[] decimal128 = FastDecimalUtils.convertToDecimal128(writeDec);
                    builder.writeDecimal128(decimal128[0], decimal128[1]);
                    decimal = writeDec;
                } else {
                    DecimalStructure buffer = new DecimalStructure();
                    DecimalStructure result = new DecimalStructure();
                    // 低 64 位是 unsigned
                    long[] decimal128 = {-1 - genSmallLong(), Long.MAX_VALUE};
                    FastDecimalUtils.setDecimal128WithScale(buffer, result, decimal128[0], decimal128[1], SCALE);
                    builder.writeDecimal128(decimal128[0], decimal128[1]);
                    decimal = new Decimal(result);
                }

                break;
            case FULL:
                if (!overflow || i % 2 == 1) {
                    l = -Math.abs(genLong());
                } else {
                    l = leftConstVal >= 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
                }
                decimal = new Decimal(l, SCALE);
                builder.writeDecimal(decimal);
                break;
            default:
                throw new UnsupportedOperationException();
            }

            if (leftConstVal != null) {
                Decimal target = new Decimal();

                FastDecimalUtils.add(Decimal.fromLong(leftConstVal).getDecimalStructure(),
                    decimal.getDecimalStructure(),
                    target.getDecimalStructure());
                targetResult[i] = target;
            }
        }
        DecimalBlock decimalBlock = (DecimalBlock) builder.build();
        blocks[0] = decimalBlock;

        switch (inputState) {
        case DECIMAL_64:
        case NULL:
            Assert.assertTrue(decimalBlock.isDecimal64());
            break;
        case DECIMAL_128:
            Assert.assertTrue(decimalBlock.isDecimal128());
            break;
        case SIMPLE:
            Assert.assertTrue(decimalBlock.isSimple());
            break;
        case FULL:
            Assert.assertTrue(decimalBlock.getState().isFull());
            break;
        }

        return new MutableChunk(blocks);
    }

    private int genSmallLong() {
        return random.nextInt(899) + 100;
    }

    private long genNotOverflowLong() {
        return random.nextInt(99999999) + 100000000;
    }

    private long genLong() {
        return random.nextLong();
    }

    enum InputState {
        DECIMAL_64,
        DECIMAL_128,
        SIMPLE,
        FULL,
        NULL
    }
}
