package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;

public class BitVecTest {
    @Test
    public void test1() {
        // Long Col & Long const
        BitAndLongColLongConstVectorizedExpression expression = new BitAndLongColLongConstVectorizedExpression(
            2, new VectorizedExpression[] {
            new InputRefVectorizedExpression(DataTypes.LongType, 0, 0),
            new LiteralVectorizedExpression(DataTypes.LongType, 15, 1)
        }
        );

        ExecutionContext executionContext = new ExecutionContext();

        final int chunkLimit = 1000;
        LongBlock longBlock = null;
        LongBlockBuilder longBlockBuilder =
            (LongBlockBuilder) BlockBuilders.create(DataTypes.LongType, executionContext);
        for (int i = 0; i < chunkLimit; i++) {
            longBlockBuilder.writeLong(i);
        }
        longBlock = (LongBlock) longBlockBuilder.build();

        ULongBlock resultBlock = (ULongBlock) BlockUtils.createBlock(DataTypes.ULongType, chunkLimit);

        MutableChunk chunk = new MutableChunk(new Block[] {
            longBlock, null, resultBlock
        });

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        expression.eval(evaluationContext);

        for (int i = 0; i < chunkLimit; i++) {
            Assert.assertEquals(i & 15, resultBlock.getLong(i));
        }
    }

    @Test
    public void test2() {
        // ULong Col & Long const
        BitAndLongColULongConstVectorizedExpression expression = new BitAndLongColULongConstVectorizedExpression(
            2, new VectorizedExpression[] {
            new InputRefVectorizedExpression(DataTypes.LongType, 0, 0),
            new LiteralVectorizedExpression(DataTypes.ULongType, 15, 1)
        }
        );

        ExecutionContext executionContext = new ExecutionContext();

        final int chunkLimit = 1000;
        LongBlock longBlock = null;
        LongBlockBuilder longBlockBuilder =
            (LongBlockBuilder) BlockBuilders.create(DataTypes.LongType, executionContext);
        for (int i = 0; i < chunkLimit; i++) {
            longBlockBuilder.writeLong(i);
        }
        longBlock = (LongBlock) longBlockBuilder.build();

        ULongBlock resultBlock = (ULongBlock) BlockUtils.createBlock(DataTypes.ULongType, chunkLimit);

        MutableChunk chunk = new MutableChunk(new Block[] {
            longBlock, null, resultBlock
        });

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        expression.eval(evaluationContext);

        for (int i = 0; i < chunkLimit; i++) {
            Assert.assertEquals(i & 15, resultBlock.getLong(i));
        }
    }
}
