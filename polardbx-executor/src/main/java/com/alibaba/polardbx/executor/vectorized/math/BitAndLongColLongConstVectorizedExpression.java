package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"BitAnd", "&"}, argumentTypes = {"Long", "Long"}, argumentKinds = {Variable, Const})
public class BitAndLongColLongConstVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean rightIsNull;
    private final long right;

    public BitAndLongColLongConstVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.ULongType, outputIndex, children);
        Object rightValue = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (rightValue == null) {
            rightIsNull = true;
            right = (long) 0;
        } else {
            rightIsNull = false;
            right = (long) rightValue;
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        if (rightIsNull) {
            VectorizedExpressionUtils.setNulls(chunk, outputIndex);
            return;
        }

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        long[] array1 = (leftInputVectorSlot.cast(LongBlock.class)).longArray();
        long[] res = (outputVectorSlot.cast(ULongBlock.class)).longArray();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                res[j] = (array1[j]) & (right);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                res[i] = (array1[i]) & (right);
            }
        }

    }
}


