package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import java.util.BitSet;

public class InEmptyListVectorizedExpressionTest {
    @Test
    public void test1() {

        Block[] blocks = new Block[2];
        blocks[0] = LongBlock.of(1L, 2L, 3L, 4L, 5L);
        blocks[1] = (Block) BlockUtils.createBlock(DataTypes.LongType, 16);
        MutableChunk chunk = new MutableChunk(null, blocks, 16, new int[] {0, 1}, new BitSet());

        ExecutionContext context = new ExecutionContext();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, context);

        VectorizedExpression inputRefExpression = new InputRefVectorizedExpression(DataTypes.LongType, 0, 0);

        InEmptyListVectorizedExpression expression = new InEmptyListVectorizedExpression(
            1, new VectorizedExpression[] {inputRefExpression}
        );

        expression.eval(evaluationContext);
    }

    @Test
    public void test2() {

        Block[] blocks = new Block[2];
        blocks[0] = LongBlock.of(1L, 2L, 3L, 4L, 5L);
        blocks[1] = (Block) BlockUtils.createBlock(DataTypes.LongType, 16);
        MutableChunk chunk = new MutableChunk(null, blocks, 16, new int[] {0, 1}, new BitSet());

        ExecutionContext context = new ExecutionContext();
        EvaluationContext evaluationContext = new EvaluationContext(chunk, context);

        VectorizedExpression inputRefExpression = new InputRefVectorizedExpression(DataTypes.LongType, 0, 0);

        NotInEmptyListVectorizedExpression expression = new NotInEmptyListVectorizedExpression(
            1, new VectorizedExpression[] {inputRefExpression}
        );

        expression.eval(evaluationContext);
    }

}