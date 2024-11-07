package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class BetweenIntegerColLongConstVectorizedExpressionTest {

    @Test
    public void test() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, null, 0L, 0L, null, 1L, 0L);

        doTest(inputChunk, expectBlock, 1, 100, null);
    }

    @Test
    public void testWithSelection() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, null, 0L, 0L, null, 1L, 0L);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, 1, 100, sel);
    }

    @Test
    public void testNull() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        doTestNull(inputChunk, expectBlock, null, null, null);
        doTestNull(inputChunk, expectBlock, 1L, null, null);
        doTestNull(inputChunk, expectBlock, null, 100L, null);
    }

    @Test
    public void testNullWithSelection() {

        IntegerBlock integerBlock = IntegerBlock.of(1, 2, 100, null, 200, 1000, null, 100, -1000);
        Chunk inputChunk = new Chunk(integerBlock.getPositionCount(), integerBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTestNull(inputChunk, expectBlock, null, null, sel);
        doTestNull(inputChunk, expectBlock, 1L, null, sel);
        doTestNull(inputChunk, expectBlock, null, 100L, sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, long left, long right, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        BetweenIntegerColLongConstLongConstVectorizedExpression condition =
            new BetweenIntegerColLongConstLongConstVectorizedExpression(
                3,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.IntegerType, 0, 0),
                    new LiteralVectorizedExpression(DataTypes.LongType, left, 1),
                    new LiteralVectorizedExpression(DataTypes.LongType, right, 2)
                });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.IntegerType))
            .addEmptySlots(Arrays.asList(DataTypes.LongType, DataTypes.LongType, DataTypes.LongType))
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

    private void doTestNull(Chunk inputChunk, RandomAccessBlock expectBlock, Long left, Long right, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        BetweenIntegerColLongConstLongConstVectorizedExpression condition =
            new BetweenIntegerColLongConstLongConstVectorizedExpression(
                3,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.IntegerType, 0, 0),
                    new LiteralVectorizedExpression(DataTypes.LongType, left, 1),
                    new LiteralVectorizedExpression(DataTypes.LongType, right, 2)
                });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.IntegerType))
            .addEmptySlots(Arrays.asList(DataTypes.LongType, DataTypes.LongType, DataTypes.LongType))
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
