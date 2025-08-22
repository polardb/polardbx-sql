package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class NotInEmptyListVectorizedExpression extends AbstractVectorizedExpression {
    public NotInEmptyListVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);

        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                res[j] = LongBlock.TRUE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                res[i] = LongBlock.TRUE_VALUE;
            }
        }
    }
}