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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.google.common.base.Preconditions;

@SuppressWarnings("unused")
public class OrVectorizedExpression extends AbstractVectorizedExpression {
    private int[] currentSel;
    private int currentSelSize;
    private int[] remainingSel;
    private int remainingSelSize;
    private int[] originalSel;
    private int originalSelSize;
    private int[] marks;

    public OrVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(null, -1, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        boolean selectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();
        int batchSize = chunk.batchSize();

        int chunkLimitSize = ctx.getExecutionContext().getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        // initialize the current and remaining selection array only once.
        if (remainingSel == null) {
            Preconditions.checkArgument(chunkLimitSize > 0, "invalid chunk limit size");
            currentSel = new int[chunkLimitSize];
            remainingSel = new int[chunkLimitSize];
            originalSel = new int[chunkLimitSize];
            marks = new int[chunkLimitSize];
        }
        originalSelSize = batchSize;
        if (!selectionInUse) {
            for (int j = 0; j < batchSize; j++) {
                originalSel[j] = j;
            }
        } else {
            System.arraycopy(sel, 0, originalSel, 0, batchSize);
        }

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

            currentSelSize = originalSelSize;
            System.arraycopy(originalSel, 0, currentSel, 0, currentSelSize);
            currentSelSize =
                VectorizedExpressionUtils.conditionalEval(ctx, chunk, expression, currentSel, currentSelSize);
            for (int i = 0; i < currentSelSize; i++) {
                marks[currentSel[i]] = 1;
            }
            remainingSelSize = VectorizedExpressionUtils
                .subtract(originalSel, originalSelSize, currentSel, currentSelSize, remainingSel);
        }

        int selIndex = 0;
        if (!selectionInUse) {
            sel = new int[chunkLimitSize];
        }
        for (int i = 0; i < batchSize; i++) {
            if (marks[i] == 1) {
                sel[selIndex++] = i;
            }
        }
        chunk.setSelectionInUse(true);
        chunk.setBatchSize(selIndex);
    }
}