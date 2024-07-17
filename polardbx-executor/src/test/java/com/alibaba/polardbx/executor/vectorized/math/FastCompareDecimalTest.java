package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.utils.DecimalTestUtil;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastGEDecimalColDecimalConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastGEDecimalColLongConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastGTDecimalColDecimalConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastGTDecimalColLongConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastLEDecimalColDecimalConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastLEDecimalColLongConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastLTDecimalColDecimalConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastLTDecimalColLongConstVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
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
public class FastCompareDecimalTest {

    private static final int COUNT = 1024;
    /**
     * 虽然 precision 不在 decimal64 范围内
     * 但是否为 decimal64 取决于实际的值
     */
    private static final int PRECISION = 20;
    private static final int OUTPUT_INDEX = 1;
    private static final int BLOCK_COUNT = 2;
    private final CompareType compareType;
    private final int scale;
    private final DecimalType inputDecimalType;
    private final Long longVal;
    private final Decimal operandDec;
    private final Random random = new Random(System.currentTimeMillis());
    private final long[] targetResult = new long[COUNT];
    private final ExecutionContext executionContext = new ExecutionContext();

    public FastCompareDecimalTest(int scale, CompareType compareType, Long specialVal) {
        this.scale = scale;
        this.inputDecimalType = new DecimalType(PRECISION, scale);
        if (specialVal != null) {
            this.longVal = specialVal;
            this.operandDec = Decimal.fromLong(specialVal);
        } else {
            if (scale == -1) {
                this.longVal = null;
                this.operandDec = Decimal.fromLong(0);
            } else {
                this.longVal = new Random(System.currentTimeMillis()).nextLong();
                this.operandDec = Decimal.fromLong(longVal);
            }
        }

        this.compareType = compareType;
    }

    @Parameterized.Parameters(name = "scale={0},compareType={1},specialVal={2}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        int[] scales = {-1, 0, 1, 2, 5, 9};
        for (int scale : scales) {
            for (CompareType type : CompareType.values()) {
                list.add(new Object[] {scale, type, null});
            }
        }
        scales = new int[] {0, 1, 2, 5, 9};
        for (int scale : scales) {
            for (CompareType type : CompareType.values()) {
                list.add(new Object[] {scale, type, new Long(0)});
                list.add(new Object[] {scale, type, new Long(Long.MIN_VALUE)});
                list.add(new Object[] {scale, type, new Long(Long.MAX_VALUE)});
                list.add(new Object[] {scale, type, new Long(Integer.MAX_VALUE)});
            }
        }
        return list;
    }

    @Test
    public void testDecimal64CompareLong() {
        final VectorizedExpression[] children = new VectorizedExpression[3];
        children[0] = new InputRefVectorizedExpression(inputDecimalType, 0, 0);
        children[1] = new LiteralVectorizedExpression(DataTypes.LongType, longVal, 1);

        AbstractVectorizedExpression expr;
        switch (compareType) {
        case LE:
            expr = new FastLEDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case GE:
            expr = new FastGEDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case LT:
            expr = new FastLTDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case GT:
            expr = new FastGTDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        default:
            throw new UnsupportedOperationException("Unknown compare type: " + compareType);
        }

        MutableChunk chunk = buildDecimal64Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));

        expr.eval(evaluationContext);

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("[" + compareType + "] Incorrect result for: " + inputBlock.getDecimal(i).toString(),
                targetResult[i], outputBlock.getLong(i));
        }
    }

    @Test
    public void testDecimal128CompareLong() {
        if (scale == -1) {
            return;
        }
        final VectorizedExpression[] children = new VectorizedExpression[3];
        children[0] = new InputRefVectorizedExpression(inputDecimalType, 0, 0);
        children[1] = new LiteralVectorizedExpression(DataTypes.LongType, longVal, 1);

        AbstractVectorizedExpression expr;
        switch (compareType) {
        case LE:
            expr = new FastLEDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case GE:
            expr = new FastGEDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case LT:
            expr = new FastLTDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case GT:
            expr = new FastGTDecimalColLongConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        default:
            throw new UnsupportedOperationException("Unknown compare type: " + compareType);
        }

        MutableChunk chunk = buildDecimal128Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));

        expr.eval(evaluationContext);

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("[" + compareType + "] Incorrect result for: " + inputBlock.getDecimal(i).toString(),
                targetResult[i], outputBlock.getLong(i));
        }
    }

    @Test
    public void testDecimal64CompareSameScaleDecimal64() {
        final VectorizedExpression[] children = new VectorizedExpression[3];
        children[0] = new InputRefVectorizedExpression(inputDecimalType, 0, 0);
        children[1] = new LiteralVectorizedExpression(inputDecimalType, operandDec, 1);

        AbstractVectorizedExpression expr;
        switch (compareType) {
        case GE:
            expr = new FastGEDecimalColDecimalConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case GT:
            expr = new FastGTDecimalColDecimalConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case LE:
            expr = new FastLEDecimalColDecimalConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        case LT:
            expr = new FastLTDecimalColDecimalConstVectorizedExpression(OUTPUT_INDEX, children);
            break;
        default:
            throw new UnsupportedOperationException("Unknown compare type: " + compareType);
        }

        MutableChunk chunk = buildDecimal64Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));

        expr.eval(evaluationContext);

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("[" + compareType + "] Incorrect result for: " + inputBlock.getDecimal(i).toString(),
                targetResult[i], outputBlock.getLong(i));
        }
    }

    private MutableChunk buildDecimal64Chunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[0] = new DecimalBlock(inputDecimalType, COUNT);
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);

        DecimalBlockBuilder inputBuilder = new DecimalBlockBuilder(COUNT, inputDecimalType);
        for (int i = 0; i < COUNT; i++) {
            long input = genDecimal64NotOverflowLong();
            if (i % 2 == 0) {
                // 穿插正负数
                input = -input;
            }
            if (i == COUNT - 1 && longVal != null) {
                // 穿插等值
                input = longVal;
            }
            inputBuilder.writeLong(input);
            Decimal inputDecimal = new Decimal(input, scale);
            boolean result;
            switch (compareType) {
            case LE:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) <= 0;
                break;
            case GE:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) >= 0;
                break;
            case LT:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) < 0;
                break;
            case GT:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) > 0;
                break;
            default:
                throw new UnsupportedOperationException("Unknown compare type: " + compareType);
            }
            targetResult[i] = result ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;

        }
        DecimalBlock inputBlock = (DecimalBlock) inputBuilder.build();
        blocks[0] = inputBlock;
        Assert.assertTrue(inputBlock.isDecimal64());

        return new MutableChunk(blocks);
    }

    private MutableChunk buildDecimal128Chunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[0] = new DecimalBlock(inputDecimalType, COUNT);
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);

        DecimalBlockBuilder inputBuilder = new DecimalBlockBuilder(COUNT, inputDecimalType);
        for (int i = 0; i < COUNT; i++) {
            String decStr = DecimalTestUtil.gen128BitUnsignedNumStr(random, 1, false);
            if (i % 2 == 0) {
                // 穿插正负数
                decStr = "-" + decStr;
            }
            if (i == COUNT - 1 && longVal != null) {
                // 穿插等值
                decStr = longVal + "";
            }
            Decimal inputDecimal = Decimal.fromString(decStr);
            FastDecimalUtils.shift(inputDecimal.getDecimalStructure(), inputDecimal.getDecimalStructure(), -scale);
            inputDecimal.getDecimalStructure().setFractions(scale);
            long[] decimal128 = FastDecimalUtils.convertToDecimal128(inputDecimal);
            inputBuilder.writeDecimal128(decimal128[0], decimal128[1]);
            boolean result;
            switch (compareType) {
            case LE:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) <= 0;
                break;
            case GE:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) >= 0;
                break;
            case LT:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) < 0;
                break;
            case GT:
                result = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                    operandDec.getDecimalStructure()) > 0;
                break;
            default:
                throw new UnsupportedOperationException("Unknown compare type: " + compareType);
            }
            targetResult[i] = result ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;

        }
        DecimalBlock inputBlock = (DecimalBlock) inputBuilder.build();
        blocks[0] = inputBlock;
        Assert.assertTrue(inputBlock.isDecimal128());

        return new MutableChunk(blocks);
    }

    private long genDecimal64NotOverflowLong() {
        return random.nextInt(9999999) + 10_000_000;
    }

    private enum CompareType {
        LE, GE, LT, GT
    }
}
