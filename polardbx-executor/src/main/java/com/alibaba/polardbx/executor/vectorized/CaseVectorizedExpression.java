/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

/**
 * case when cond1 then expr1
 * when cond2 then expr2
 * ...
 * else exprN
 */
public class CaseVectorizedExpression extends AbstractVectorizedExpression {
    private int[] currentSel;
    private int currentSelSize;
    private int[] remainingSel;
    private int remainingSelSize;
    private int[] originalSel;
    private int originalSelSize;

    /**
     * the children expressions are:
     * {when, then, when, then ... else}
     */
    public CaseVectorizedExpression(DataType<?> outputDataType, int outputIndex,
                                    VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        boolean selectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();
        int batchSize = chunk.batchSize();

        // initialize the current and remaining selection array only once.
        if (remainingSel == null || remainingSel.length < batchSize) {
            currentSel = new int[batchSize];
            remainingSel = new int[batchSize];
            originalSel = new int[batchSize];
        }

        RandomAccessBlock outputSlot = chunk.slotIn(outputIndex, outputDataType);

        remainingSelSize = batchSize;
        originalSelSize = batchSize;

        if (!selectionInUse) {
            for (int j = 0; j < batchSize; j++) {
                originalSel[j] = j;
            }
        } else {
            System.arraycopy(sel, 0, originalSel, 0, batchSize);
        }

        for (int i = 0; i < children.length; i++) {
            if (i != 0 && i != children.length - 1) {
                // swap original selections and remaining selections
                int[] tmp = originalSel;
                originalSel = remainingSel;
                originalSelSize = remainingSelSize;
                remainingSel = tmp;
            }

            if (i != children.length - 1) {
                // when ... then ...
                System.arraycopy(originalSel, 0, currentSel, 0, originalSelSize);
                currentSelSize = originalSelSize;
                // [when condition] evaluation to update current selection
                VectorizedExpression whenCondition = children[i];
                currentSelSize =
                    VectorizedExpressionUtils.conditionalEval(ctx, chunk, whenCondition, currentSel, currentSelSize);

                // [then expression] evaluation to yield a new vector
                VectorizedExpression thenExpression = children[++i];
                VectorizedExpressionUtils.conditionalEval(ctx, chunk, thenExpression, currentSel, currentSelSize);

                // update remaining selection array by (original - current)
                remainingSelSize = VectorizedExpressionUtils
                    .subtract(originalSel, originalSelSize, currentSel, currentSelSize, remainingSel);

                // copy the result of [then expression] to output vector, with the mask of [current selection]
                RandomAccessBlock thenVector =
                    chunk.slotIn(thenExpression.getOutputIndex(), thenExpression.getOutputDataType());
                thenVector.copySelected(true, currentSel, currentSelSize, outputSlot);
            } else {
                // else ... end
                // [else expression] evaluation to yield a new vector
                VectorizedExpression elseExpression = children[i];
                VectorizedExpressionUtils.conditionalEval(ctx, chunk, elseExpression, remainingSel, remainingSelSize);

                // copy the result of [else expression] to output vector, with the mask of [remaining selection]
                RandomAccessBlock elseVector =
                    chunk.slotIn(elseExpression.getOutputIndex(), elseExpression.getOutputDataType());
                elseVector.copySelected(true, remainingSel, remainingSelSize, outputSlot);
            }

            // if there is no remaining positions need to be computed, break the loop.
            if (remainingSelSize <= 0) {
                break;
            }
        }

    }
}
