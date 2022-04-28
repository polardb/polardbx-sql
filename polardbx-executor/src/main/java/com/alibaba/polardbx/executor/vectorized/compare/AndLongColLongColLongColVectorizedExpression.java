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

package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Arrays;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"AND"}, argumentTypes = {"Long", "Long", "Long"}, argumentKinds = {Variable, Variable, Variable})
public class AndLongColLongColLongColVectorizedExpression extends AbstractVectorizedExpression {
    int[] tmpSel1 = new int[1000];
    int[] tmpSel2 = new int[1000];

    public AndLongColLongColLongColVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        if (ctx.getPreAllocatedChunk().isSelectionInUse()
            || !ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE)) {
            doNormal(ctx);
            return;
        }

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        final int batchSize = chunk.batchSize();
        final boolean isSelectionInUse = chunk.isSelectionInUse();
        final int[] sel = chunk.selection();

        VectorizedExpression child1 = children[0];
        VectorizedExpression child2 = children[1];
        VectorizedExpression child3 = children[2];

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock inputVec1 = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock inputVec2 = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());
        RandomAccessBlock inputVec3 = chunk.slotIn(children[2].getOutputIndex(), children[2].getOutputDataType());

        long[] array1 = ((LongBlock) inputVec1).longArray();
        boolean[] nulls1 = inputVec1.nulls();
        boolean input1HasNull = inputVec1.hasNull();

        long[] array2 = ((LongBlock) inputVec2).longArray();
        boolean[] nulls2 = inputVec2.nulls();
        boolean input2HasNull = inputVec2.hasNull();

        long[] array3 = ((LongBlock) inputVec3).longArray();
        boolean[] nulls3 = inputVec3.nulls();
        boolean input3HasNull = inputVec3.hasNull();

        long[] res = ((LongBlock) outputVectorSlot).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();
        outputVectorSlot.setHasNull(input1HasNull | input2HasNull | input3HasNull);

        Arrays.fill(res, 0L);

        // child1
        child1.eval(ctx);

        int tmpBatchSize1 = 0;

        for (int i = 0; i < batchSize; i++) {
            tmpSel1[tmpBatchSize1] = i;
            tmpBatchSize1 += array1[i];
            outputNulls[i] = outputNulls[i] || nulls1[i];
        }

        // child2
        ctx.getPreAllocatedChunk().setSelectionInUse(true);
        ctx.getPreAllocatedChunk().setBatchSize(tmpBatchSize1);
        ctx.getPreAllocatedChunk().setSelection(tmpSel1);
        child2.eval(ctx);

        int tmpBatchSize2 = 0;
        for (int i = 0; i < tmpBatchSize1; i++) {
            int j = tmpSel1[i];
            tmpSel2[tmpBatchSize2] = j;
            tmpBatchSize2 += array2[j];
            outputNulls[j] = outputNulls[j] || nulls2[j];
        }

        // child3
        ctx.getPreAllocatedChunk().setSelectionInUse(true);
        ctx.getPreAllocatedChunk().setBatchSize(tmpBatchSize2);
        ctx.getPreAllocatedChunk().setSelection(tmpSel2);
        child3.eval(ctx);

        for (int i = 0; i < tmpBatchSize2; i++) {
            int j = tmpSel2[i];
            res[j] = array3[j];
            outputNulls[j] = outputNulls[j] || nulls3[j];
        }

        // recover
        ctx.getPreAllocatedChunk().setSelectionInUse(isSelectionInUse);
        ctx.getPreAllocatedChunk().setBatchSize(batchSize);
        ctx.getPreAllocatedChunk().setSelection(sel);

    }

    private void doNormal(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock inputVec1 = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock inputVec2 = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());
        RandomAccessBlock inputVec3 = chunk.slotIn(children[2].getOutputIndex(), children[2].getOutputDataType());

        long[] array1 = ((LongBlock) inputVec1).longArray();
        boolean[] nulls1 = inputVec1.nulls();
        boolean input1HasNull = inputVec1.hasNull();

        long[] array2 = ((LongBlock) inputVec2).longArray();
        boolean[] nulls2 = inputVec2.nulls();
        boolean input2HasNull = inputVec2.hasNull();

        long[] array3 = ((LongBlock) inputVec3).longArray();
        boolean[] nulls3 = inputVec3.nulls();
        boolean input3HasNull = inputVec3.hasNull();

        long[] res = ((LongBlock) outputVectorSlot).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();
        outputVectorSlot.setHasNull(input1HasNull | input2HasNull | input3HasNull);

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                boolean null1 = !input1HasNull ? false : nulls1[j];
                boolean null2 = !input2HasNull ? false : nulls2[j];
                boolean null3 = !input3HasNull ? false : nulls3[j];
                boolean b1 = (array1[j] != 0);
                boolean b2 = (array2[j] != 0);
                boolean b3 = (array3[j] != 0);

                outputNulls[j] = null1 || null2 || null3;
                res[j] = (b1 && b2 && b3) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null1 = !input1HasNull ? false : nulls1[i];
                boolean null2 = !input2HasNull ? false : nulls2[i];
                boolean null3 = !input3HasNull ? false : nulls3[i];
                boolean b1 = (array1[i] != 0);
                boolean b2 = (array2[i] != 0);
                boolean b3 = (array3[i] != 0);

                outputNulls[i] = null1 || null2 || null3;
                res[i] = (b1 && b2 && b3) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }
}
