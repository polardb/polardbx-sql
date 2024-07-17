package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.Decimal;
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
import com.alibaba.polardbx.executor.vectorized.compare.FastBetweenDecimalColCharConstCharConstVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastBetweenDecimalColDecimalConstDecimalConstVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * 只有当 left 和 right scale 相同时，才能走 decimal64 路径
 */
@RunWith(Parameterized.class)
public class FastBetweenDecimalTest {

    private static final int COUNT = 1024;
    /**
     * 虽然 precision 不在 decimal64 范围内
     * 但是否为 decimal64 取决于实际的值
     */
    private static final int PRECISION = 20;
    private static final int OUTPUT_INDEX = 1;
    private static final int BLOCK_COUNT = 2;
    private static final int RANGE = 1000;
    private static final Decimal RANGE_DECIMAL = Decimal.fromLong(RANGE);
    private final boolean overflow = false;
    private final int scale;
    private final DecimalType leftDecimalType;
    private final DecimalType rightDecimalType;
    private final DecimalType inputDecimalType;

    private final Decimal leftDecimal;
    private final Decimal rightDecimal;

    private final Random random = new Random(System.currentTimeMillis());
    private final long[] targetResult = new long[COUNT];

    private final ExecutionContext executionContext = new ExecutionContext();

    public FastBetweenDecimalTest(int scale) {
        this.scale = scale;
        this.leftDecimalType = new DecimalType(PRECISION, scale);
        this.rightDecimalType = new DecimalType(PRECISION, scale);
        this.inputDecimalType = new DecimalType(PRECISION, scale);
        long l = genDecimal64NotOverflowLong();
        this.leftDecimal = new Decimal(l, scale);
        this.rightDecimal = leftDecimal.add(RANGE_DECIMAL);
    }

    @Parameterized.Parameters(name = "scale={0}")
    public static List<Object[]> generateParameters() {
        List<Object[]> list = new ArrayList<>();

        final int[] scales = {0, 1, 2, 5, 9};
        for (int scale : scales) {
            list.add(new Object[] {scale});
        }
        return list;
    }

    @Test
    public void testBetweenDecimal64() {
        final VectorizedExpression[] children = new VectorizedExpression[3];
        children[0] = new InputRefVectorizedExpression(inputDecimalType, 0, 0);
        children[1] = new LiteralVectorizedExpression(leftDecimalType, leftDecimal, 1);
        children[2] = new LiteralVectorizedExpression(rightDecimalType, rightDecimal, 2);
        FastBetweenDecimalColDecimalConstDecimalConstVectorizedExpression expr =
            new FastBetweenDecimalColDecimalConstDecimalConstVectorizedExpression(
                OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal64Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));

        expr.eval(evaluationContext);

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("Incorrect between result for: " + inputBlock.getDecimal(i).toString(),
                targetResult[i], outputBlock.getLong(i));
        }
    }

    /**
     * between '0.01' and '3.21'
     */
    @Test
    public void testBetweenDecimal64CharConstCharConst() {
        final VectorizedExpression[] children = new VectorizedExpression[3];
        children[0] = new InputRefVectorizedExpression(inputDecimalType, 0, 0);
        children[1] = new LiteralVectorizedExpression(DataTypes.CharType, leftDecimal.toString(), 1);
        children[2] = new LiteralVectorizedExpression(DataTypes.CharType, rightDecimal.toString(), 2);
        FastBetweenDecimalColCharConstCharConstVectorizedExpression expr =
            new FastBetweenDecimalColCharConstCharConstVectorizedExpression(
                OUTPUT_INDEX, children);

        MutableChunk chunk = buildDecimal64Chunk();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        LongBlock outputBlock = (LongBlock) Objects.requireNonNull(chunk.slotIn(OUTPUT_INDEX));
        DecimalBlock inputBlock = (DecimalBlock) Objects.requireNonNull(chunk.slotIn(0));

        expr.eval(evaluationContext);

        // check result
        Assert.assertEquals("Incorrect output block positionCount", COUNT, outputBlock.getPositionCount());
        for (int i = 0; i < COUNT; i++) {
            Assert.assertEquals("Incorrect between result for: " + inputBlock.getDecimal(i).toString(),
                targetResult[i], outputBlock.getLong(i));
        }
    }

    private MutableChunk buildDecimal64Chunk() {
        Block[] blocks = new Block[BLOCK_COUNT];
        blocks[0] = new DecimalBlock(inputDecimalType, COUNT);
        blocks[1] = new LongBlock(DataTypes.LongType, COUNT);

        DecimalBlockBuilder inputBuilder = new DecimalBlockBuilder(COUNT, inputDecimalType);
        for (int i = 0; i < COUNT; i++) {
            long left = genDecimal64NotOverflowLong();
            // 穿插正负数
            inputBuilder.writeLong(left);
            Decimal inputDecimal = new Decimal(left, scale);
            boolean between = FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                leftDecimal.getDecimalStructure()) >= 0;
            between &= FastDecimalUtils.compare(inputDecimal.getDecimalStructure(),
                rightDecimal.getDecimalStructure()) <= 0;
            targetResult[i] = between ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
        }
        DecimalBlock inputBlock = (DecimalBlock) inputBuilder.build();
        blocks[0] = inputBlock;
        Assert.assertTrue(inputBlock.isDecimal64());

        return new MutableChunk(blocks);
    }

    private long genDecimal64NotOverflowLong() {
        return random.nextInt(9999999) + 10_000_000;
    }

    private long genDecimal64OverflowLong() {
        return random.nextInt(9999999) + 9000_000_000L;
    }
}
