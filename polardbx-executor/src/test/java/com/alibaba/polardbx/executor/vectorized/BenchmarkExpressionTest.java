package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkExpressionTest extends BaseVectorizedExpressionTest {

    private final long totalCount = 1024L;
    private final AtomicInteger counter = new AtomicInteger(0);
    private final DataType outputDataType = DataTypes.LongType;

    @Test
    public void testBenchmarkExpr() {
        BenchmarkVectorizedExpression expr = mockBenchmarkExpr();
        MutableChunk chunk = new MutableChunk(new LongBlock(DataTypes.LongType, 1));

        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals("Expect benchmark count equals", totalCount, counter.get());
    }

    @Test
    public void testBenchmarkExprWithSelection() {
        BenchmarkVectorizedExpression expr = mockBenchmarkExpr();
        int[] sel = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        MutableChunk chunk = new MutableChunk(new LongBlock(DataTypes.LongType, sel.length));
        chunk.setBatchSize(sel.length);
        chunk.setSelection(sel);
        chunk.setSelectionInUse(true);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);
        Assert.assertEquals("Expect benchmark count equals", totalCount, counter.get());
    }

    private BenchmarkVectorizedExpression mockBenchmarkExpr() {
        VectorizedExpression[] children = new VectorizedExpression[2];
        children[0] = new LiteralVectorizedExpression(DataTypes.LongType, totalCount, 0);
        children[1] = new VectorizedExpression() {
            @Override
            public void eval(EvaluationContext ctx) {
                counter.incrementAndGet();
            }

            @Override
            public VectorizedExpression[] getChildren() {
                return new VectorizedExpression[0];
            }

            @Override
            public DataType<?> getOutputDataType() {
                return DataTypes.LongType;
            }

            @Override
            public int getOutputIndex() {
                return 0;
            }
        };
        return new BenchmarkVectorizedExpression(outputDataType, 0, children);
    }
}
