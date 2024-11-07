package com.alibaba.polardbx.executor.vectorized.logical;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

@SuppressWarnings("unused")
@ExpressionSignatures(
    names = {"AND"},
    argumentTypes = {"Long", "Long"},
    argumentKinds = {Variable, Variable},
    priority = SPECIAL
)
public class FastAndLongColLongColVectorizedExpression extends AbstractVectorizedExpression {

    private int[] tmpSelectionBuffer = null;

    public FastAndLongColLongColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
        if (children.length != 2) {
            throw new IllegalArgumentException("AND expression should have 2 children");
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        final boolean enableShortCircuit =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_AND_FAST_VEC);

        if (!enableShortCircuit) {
            doAndEval(ctx);
            return;
        }

        doAndEvalWithShortCircuit(ctx);
    }

    private void doAndEvalWithShortCircuit(EvaluationContext ctx) {
        // evaluate left expression first
        children[0].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        int batchSize = chunk.batchSize();
        boolean outputVectorHasNull = false;
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        RandomAccessBlock inputVec1 = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        long[] leftResults = (inputVec1.cast(LongBlock.class)).longArray();
        boolean[] nulls1 = inputVec1.nulls();
        boolean input1HasNull = inputVec1.hasNull();

        // cannot reuse the origin selection inside the chunk
        int tmpSelSize = 0;
        int[] tmpSel = getSelectionBuffer(leftResults.length);
        if (isSelectionInUse) {
            // merge selection array
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                // select when left result is true
                if (leftResults[j] != LongBlock.FALSE_VALUE) {
                    tmpSel[tmpSelSize++] = j;
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                // select when left result is true
                if (leftResults[i] != LongBlock.FALSE_VALUE) {
                    tmpSel[tmpSelSize++] = i;
                }
            }
        }
        chunk.setSelection(tmpSel);
        chunk.setSelectionInUse(true);
        chunk.setBatchSize(tmpSelSize);

        // pass selection to right expression
        children[1].eval(ctx);

        // restore origin selection
        chunk.setSelection(sel);
        chunk.setSelectionInUse(isSelectionInUse);
        chunk.setBatchSize(batchSize);

        RandomAccessBlock inputVec2 = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());
        long[] rightResults = (inputVec2.cast(LongBlock.class)).longArray();
        boolean[] nulls2 = inputVec2.nulls();
        boolean input2HasNull = inputVec2.hasNull();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                boolean null1 = input1HasNull && nulls1[j];
                boolean null2 = input2HasNull && nulls2[j];

                boolean b1 = (leftResults[j] != 0);
                boolean b2 = (rightResults[j] != 0);

                boolean hasNull = null1;
                hasNull |= null2;

                boolean anyFalse = (!null1 && !b1);
                anyFalse |= (!null2 && !b2);

                outputNulls[j] = !anyFalse && hasNull;
                res[j] = !anyFalse && !hasNull ? 1 : 0;

                outputVectorHasNull |= outputNulls[j];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null1 = input1HasNull && nulls1[i];
                boolean null2 = input2HasNull && nulls2[i];

                boolean b1 = (leftResults[i] != 0);
                boolean b2 = (rightResults[i] != 0);

                boolean hasNull = null1;
                hasNull |= null2;

                boolean anyFalse = (!null1 && !b1);
                anyFalse |= (!null2 && !b2);

                outputNulls[i] = !anyFalse && hasNull;
                res[i] = !anyFalse && !hasNull ? 1 : 0;

                outputVectorHasNull |= outputNulls[i];
            }
        }

        outputVectorSlot.setHasNull(outputVectorHasNull);
    }

    private int[] getSelectionBuffer(int length) {
        if (this.tmpSelectionBuffer == null || this.tmpSelectionBuffer.length < length) {
            this.tmpSelectionBuffer = new int[length];
        }
        return this.tmpSelectionBuffer;
    }

    private void doAndEval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock inputVec1 = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock inputVec2 = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        long[] array1 = (inputVec1.cast(LongBlock.class)).longArray();
        boolean[] nulls1 = inputVec1.nulls();
        boolean input1HasNull = inputVec1.hasNull();

        long[] array2 = (inputVec2.cast(LongBlock.class)).longArray();
        boolean[] nulls2 = inputVec2.nulls();
        boolean input2HasNull = inputVec2.hasNull();

        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        boolean outputVectorHasNull = false;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                boolean null1 = input1HasNull && nulls1[j];
                boolean null2 = input2HasNull && nulls2[j];

                boolean b1 = (array1[j] != 0);
                boolean b2 = (array2[j] != 0);

                boolean hasNull = null1;
                hasNull |= null2;

                boolean anyFalse = (!null1 && !b1);
                anyFalse |= (!null2 && !b2);

                outputNulls[j] = !anyFalse && hasNull;
                res[j] = !anyFalse && !hasNull ? 1 : 0;

                outputVectorHasNull |= outputNulls[j];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null1 = input1HasNull && nulls1[i];
                boolean null2 = input2HasNull && nulls2[i];

                boolean b1 = (array1[i] != 0);
                boolean b2 = (array2[i] != 0);

                boolean hasNull = null1;
                hasNull |= null2;

                boolean anyFalse = (!null1 && !b1);
                anyFalse |= (!null2 && !b2);

                outputNulls[i] = !anyFalse && hasNull;
                res[i] = !anyFalse && !hasNull ? 1 : 0;

                outputVectorHasNull |= outputNulls[i];
            }
        }

        outputVectorSlot.setHasNull(outputVectorHasNull);
    }
}


