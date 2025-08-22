package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlockBuilder;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;

public class NEULongColLongConstVectorizedExpressionTest {
    @Test
    public void test() {
        // ULong col <> Long const
        NEULongColLongConstVectorizedExpression expression = new NEULongColLongConstVectorizedExpression(
            2,
            new VectorizedExpression[] {
                new InputRefVectorizedExpression(DataTypes.ULongType, 0, 0),
                new LiteralVectorizedExpression(DataTypes.LongType, 1, 1)
            }
        );

        ExecutionContext executionContext = new ExecutionContext();

        final int chunkLimit = 1000;
        ULongBlock uLongBlock = null;
        ULongBlockBuilder uLongBlockBuilder =
            (ULongBlockBuilder) BlockBuilders.create(DataTypes.ULongType, executionContext);
        for (int i = 0; i < chunkLimit; i++) {
            uLongBlockBuilder.writeLong(i);
        }
        uLongBlock = (ULongBlock) uLongBlockBuilder.build();

        LongBlock longBlock = (LongBlock) BlockUtils.createBlock(DataTypes.LongType, chunkLimit);

        MutableChunk chunk = new MutableChunk(new Block[] {
            uLongBlock, null, longBlock
        });

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);

        expression.eval(evaluationContext);

        for (int i = 0; i < chunkLimit; i++) {
            Assert.assertEquals(i != 1, longBlock.getLong(i) == 1);
        }
    }

}