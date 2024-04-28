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

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * if(cond, expr1(then), expr2(else))
 */
public class IfVectorizedExpression extends AbstractVectorizedExpression {
    private int[] thenSel;
    private int thenSelSize;
    private int[] elseSel;
    private int elseSelSize;

    public IfVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(children[2].getOutputDataType(), outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        thenSelSize = 0;
        elseSelSize = 0;
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int[] sel = chunk.selection();
        boolean selectionInUse = chunk.isSelectionInUse();
        int batchSize = chunk.batchSize();

        if ((thenSel == null) || (thenSel.length < batchSize)) {
            thenSel = new int[batchSize];
            elseSel = new int[batchSize];
        }

        VectorizedExpression whenOperator = children[0];
        VectorizedExpression thenOperator = children[1];
        VectorizedExpression elseOperator = children[2];

        RandomAccessBlock whenBlock = chunk.slotIn(whenOperator.getOutputIndex(), DataTypes.LongType);
        RandomAccessBlock thenVector = chunk.slotIn(thenOperator.getOutputIndex());
        RandomAccessBlock elseVector = chunk.slotIn(elseOperator.getOutputIndex());
        RandomAccessBlock outputSlot = chunk.slotIn(outputIndex, outputDataType);

        // handle when expression and yield then selections and else selections
        whenOperator.eval(ctx);
        long[] whenVector = (whenBlock.cast(LongBlock.class)).longArray();

        if (selectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                if (whenVector[j] == 1) {
                    thenSel[thenSelSize++] = j;
                } else {
                    elseSel[elseSelSize++] = j;
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                if (whenVector[i] == 1) {
                    thenSel[thenSelSize++] = i;
                } else {
                    elseSel[elseSelSize++] = i;
                }
            }
        }

        // copy then/else result to output vector
        conditionalEval(ctx, chunk, thenOperator, thenSel, thenSelSize);
        thenVector.copySelected(true, thenSel, thenSelSize, outputSlot);
        conditionalEval(ctx, chunk, elseOperator, elseSel, elseSelSize);
        elseVector.copySelected(true, elseSel, elseSelSize, outputSlot);
    }

    private void conditionalEval(EvaluationContext ctx, MutableChunk chunk, VectorizedExpression vectorizedExpression,
                                 int[] sel, int selSize) {
        // save batch state
        boolean preservedSelectedInUse = chunk.isSelectionInUse();
        int[] preservedSel = chunk.selection();
        int preservedBatchSize = chunk.batchSize();

        // prepare the context for conditional evaluation
        chunk.setSelectionInUse(true);
        chunk.setSelection(sel);
        chunk.setBatchSize(selSize);

        vectorizedExpression.eval(ctx);

        // restore batch state
        chunk.setSelectionInUse(preservedSelectedInUse);
        chunk.setSelection(preservedSel);
        chunk.setBatchSize(preservedBatchSize);
    }

}
