package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
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

public class BetweenLongColLongConstVectorizedExpressionTest {

    @Test
    public void test() {

        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, null, 0L, 0L, null, 1L, 0L);

        doTest(inputChunk, expectBlock, 1L, 100L, null);
    }

    @Test
    public void testWithSelection() {

        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);

        LongBlock expectBlock = LongBlock.of(1L, 1L, 1L, null, 0L, 0L, null, 1L, 0L);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, 1L, 100L, sel);
    }

    @Test
    public void testNull() {

        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        doTest(inputChunk, expectBlock, null, null, null);
        doTest(inputChunk, expectBlock, 1L, null, null);
        doTest(inputChunk, expectBlock, null, 100L, null);
    }

    @Test
    public void testNullWithSelection() {

        LongBlock longBlock = LongBlock.of(1L, 2L, 100L, null, 200L, 1000L, null, 100L, -1000L);
        Chunk inputChunk = new Chunk(longBlock.getPositionCount(), longBlock);

        LongBlock expectBlock = LongBlock.of(null, null, null, null, null, null, null, null, null);

        int[] sel = new int[] {0, 1, 2, 4, 5};
        doTest(inputChunk, expectBlock, null, null, sel);
        doTest(inputChunk, expectBlock, 1L, null, sel);
        doTest(inputChunk, expectBlock, null, 100L, sel);
    }

    private void doTest(Chunk inputChunk, RandomAccessBlock expectBlock, Long left, Long right, int[] sel) {
        ExecutionContext context = new ExecutionContext();

        BetweenLongColLongConstLongConstVectorizedExpression condition =
            new BetweenLongColLongConstLongConstVectorizedExpression(
                3,
                new VectorizedExpression[] {
                    new InputRefVectorizedExpression(DataTypes.LongType, 0, 0),
                    new LiteralVectorizedExpression(DataTypes.LongType, left, 1),
                    new LiteralVectorizedExpression(DataTypes.LongType, right, 2)
                });

        // placeholder for input and output blocks
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(context.getExecutorChunkLimit())
            .addEmptySlots(Collections.singletonList(DataTypes.LongType))
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
