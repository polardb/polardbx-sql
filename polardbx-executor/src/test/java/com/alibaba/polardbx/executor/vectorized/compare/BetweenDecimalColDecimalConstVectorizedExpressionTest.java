package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BetweenDecimalColDecimalConstVectorizedExpressionTest {

    private final int count = 10;
    private final List<Decimal> decValues;
    private final List<Long> longValues;
    private final DataType decimalType = new DecimalType(18, 2);
    private final Decimal[] decimals = new Decimal[] {
        new Decimal(100, 2),
        new Decimal(205, 2),
        new Decimal(9999, 2),
        null,
        new Decimal(20001, 2),
        new Decimal(100000, 2),
        null,
        new Decimal(10000, 2),
        new Decimal(-10000, 2)
    };

    public BetweenDecimalColDecimalConstVectorizedExpressionTest() {
        this.decValues = new ArrayList<>(count);
        this.longValues = new ArrayList<>(count);
        for (int i = 0; i < count - 1; i++) {
            long val = i * 50;
            this.decValues.add(Decimal.fromLong(val));
            this.longValues.add(val);
        }
        this.decValues.add(null);
        this.longValues.add(null);
    }

    private DecimalBlock getDecimalBlock(Decimal[] decimals) {
        DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(4, decimalType);
        for (int i = 0; i < decimals.length; i++) {
            if (decimals[i] != null) {
                blockBuilder.writeDecimal(decimals[i]);
            } else {
                blockBuilder.appendNull();
            }
        }
        return (DecimalBlock) blockBuilder.build();
    }

    @Test
    public void test() {
        DecimalBlock decimalBlock = getDecimalBlock(decimals);
        Chunk inputChunk = new Chunk(decimalBlock.getPositionCount(), decimalBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, null, 0L, 0L, null, 1L, 0L);

        doTest(inputChunk, expectBlock, new Decimal(100, 2), new Decimal(10000, 2), null);
    }

    @Test
    public void testWithSelection() {
        DecimalBlock decimalBlock = getDecimalBlock(decimals);
        Chunk inputChunk = new Chunk(decimalBlock.getPositionCount(), decimalBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, null, 0L, 0L, null, 1L, 0L);
        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, new Decimal(100, 2), new Decimal(10000, 2), sel);
    }

    @Test
    public void testNull() {
        DecimalBlock decimalBlock = getDecimalBlock(decimals);
        Chunk inputChunk = new Chunk(decimalBlock.getPositionCount(), decimalBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        doTest(inputChunk, expectBlock, null, null, null);
        doTest(inputChunk, expectBlock, new Decimal(100, 2), null, null);
        doTest(inputChunk, expectBlock, null, new Decimal(10000, 2), null);
    }

    @Test
    public void testNullWithSelection() {
        DecimalBlock decimalBlock = getDecimalBlock(decimals);
        Chunk inputChunk = new Chunk(decimalBlock.getPositionCount(), decimalBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);
        int[] sel = new int[] {0, 1, 2, 4, 5};

        doTest(inputChunk, expectBlock, null, null, sel);
        doTest(inputChunk, expectBlock, new Decimal(100, 2), null, sel);
        doTest(inputChunk, expectBlock, null, new Decimal(10000, 2), sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, Decimal left, Decimal right, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        BetweenDecimalColDecimalConstDecimalConstVectorizedExpression condition =
            new BetweenDecimalColDecimalConstDecimalConstVectorizedExpression(
                3,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(decimalType, 0, 0),
                    new LiteralVectorizedExpression(decimalType, left, 1),
                    new LiteralVectorizedExpression(decimalType, right, 2)
                });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(decimalType))
            .addEmptySlots(Arrays.asList(decimalType, decimalType, DataTypes.LongType))
            .addChunkLimit(context.getExecutorChunkLimit())
            .addOutputIndexes(new int[] {condition.getOutputIndex()})
            .build();

        preAllocatedChunk.reallocate(inputChunk.getPositionCount(), inputChunk.getBlockCount(), false);

        // Prepare selection array for evaluation.
        if (sel != null) {
            preAllocatedChunk.setBatchSize(sel.length);
            preAllocatedChunk.setSelection(sel);
            preAllocatedChunk.setSelectionInUse(true);
        } else {
            preAllocatedChunk.setBatchSize(inputChunk.getPositionCount());
            preAllocatedChunk.setSelection(null);
            preAllocatedChunk.setSelectionInUse(false);
        }

        for (int i = 0; i < inputChunk.getBlockCount(); i++) {
            Block block = inputChunk.getBlock(i);
            preAllocatedChunk.setSlotAt(block.cast(RandomAccessBlock.class), i);
        }

        // Do evaluation
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        condition.eval(evaluationContext);

        // check resultBlock
        RandomAccessBlock resultBlock = preAllocatedChunk.slotIn(condition.getOutputIndex());

        if (sel != null) {
            for (int i = 0; i < sel.length; i++) {
                int j = sel[i];
                Assert.assertEquals("Failed at pos: " + j, expectBlock.elementAt(j), resultBlock.elementAt(j));
            }
        } else {
            for (int i = 0; i < inputChunk.getPositionCount(); i++) {
                Assert.assertEquals("Failed at pos: " + i, expectBlock.elementAt(i), resultBlock.elementAt(i));
            }
        }
    }
}
