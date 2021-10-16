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

import com.alibaba.polardbx.optimizer.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class CoalesceVectorizedExpression extends AbstractVectorizedExpression {
    private int[] currentSel;
    private int currentSelSize;
    private int[] remainingSel;
    private int remainingSelSize;
    private int[] originalSel;
    private int originalSelSize;

    public CoalesceVectorizedExpression(DataType<?> outputDataType, int outputIndex,
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
        originalSelSize = batchSize;
        if (!selectionInUse) {
            for (int j = 0; j < batchSize; j++) {
                originalSel[j] = j;
            }
        } else {
            System.arraycopy(sel, 0, originalSel, 0, batchSize);
        }

        RandomAccessBlock outputSlot = chunk.slotIn(outputIndex, outputDataType);
        boolean[] nulls = outputSlot.nulls();

        for (int childIndex = 0; childIndex < children.length; childIndex++) {
            if (childIndex != 0) {
                originalSelSize = remainingSelSize;

                // swap original selections and remaining selections
                int[] tmp = originalSel;
                originalSel = remainingSel;
                originalSelSize = remainingSelSize;
                remainingSel = tmp;
            }

            VectorizedExpression expression = children[childIndex];
            VectorizedExpressionUtils.conditionalEval(ctx, chunk, expression, originalSel, originalSelSize);

            RandomAccessBlock vector = chunk.slotIn(expression.getOutputIndex(), expression.getOutputDataType());
            if (!vector.hasNull()) {
                // if vector has no null value, copy all values with index in original selection.
                // then we can terminate the vector copy loop.
                vector.copySelected(true, originalSel, originalSelSize, outputSlot);
                break;
            } else {
                boolean[] valueIsNull = vector.nulls();
                int remainingIndex = 0;
                int currentIndex = 0;
                for (int i = 0; i < originalSelSize; i++) {
                    int j = originalSel[i];
                    if (valueIsNull[j]) {
                        // index write to remaining sel[]
                        remainingSel[remainingIndex++] = j;
                    } else {
                        // write to output
                        currentSel[currentIndex++] = j;
                    }
                }
                remainingSelSize = remainingIndex;
                currentSelSize = currentIndex;
                vector.copySelected(true, currentSel, currentSelSize, outputSlot);

                // if there are remaining positions after the last expression, handle null values.
                if (remainingSelSize > 0 && childIndex == children.length - 1) {
                    outputSlot.setHasNull(true);
                    for (int i = 0; i < remainingSelSize; i++) {
                        nulls[remainingSel[i]] = true;
                    }
                }
            }
        }
    }
}