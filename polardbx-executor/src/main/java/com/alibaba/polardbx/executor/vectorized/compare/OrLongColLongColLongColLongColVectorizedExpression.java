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

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"OR"}, argumentTypes = {"Long", "Long", "Long", "Long"},
    argumentKinds = {Variable, Variable, Variable, Variable})
public class OrLongColLongColLongColLongColVectorizedExpression extends AbstractVectorizedExpression {
    public OrLongColLongColLongColLongColVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock inputVec1 = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock inputVec2 = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());
        RandomAccessBlock inputVec3 = chunk.slotIn(children[2].getOutputIndex(), children[2].getOutputDataType());
        RandomAccessBlock inputVec4 = chunk.slotIn(children[3].getOutputIndex(), children[3].getOutputDataType());

        long[] array1 = ((LongBlock) inputVec1).longArray();
        boolean[] nulls1 = inputVec1.nulls();
        boolean input1HasNull = inputVec1.hasNull();

        long[] array2 = ((LongBlock) inputVec2).longArray();
        boolean[] nulls2 = inputVec2.nulls();
        boolean input2HasNull = inputVec2.hasNull();

        long[] array3 = ((LongBlock) inputVec3).longArray();
        boolean[] nulls3 = inputVec3.nulls();
        boolean input3HasNull = inputVec3.hasNull();

        long[] array4 = ((LongBlock) inputVec4).longArray();
        boolean[] nulls4 = inputVec4.nulls();
        boolean input4HasNull = inputVec4.hasNull();

        long[] res = ((LongBlock) outputVectorSlot).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();
        outputVectorSlot.setHasNull(input1HasNull | input2HasNull | input3HasNull | input4HasNull);

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                boolean null1 = !input1HasNull ? false : nulls1[j];
                boolean null2 = !input2HasNull ? false : nulls2[j];
                boolean null3 = !input3HasNull ? false : nulls3[j];
                boolean null4 = !input4HasNull ? false : nulls4[j];
                boolean b1 = (array1[j] != 0);
                boolean b2 = (array2[j] != 0);
                boolean b3 = (array3[j] != 0);
                boolean b4 = (array4[j] != 0);

                outputNulls[j] = null1 || null2 || null3 || null4;
                res[j] = (b1 || b2 || b3 || b4) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null1 = !input1HasNull ? false : nulls1[i];
                boolean null2 = !input2HasNull ? false : nulls2[i];
                boolean null3 = !input3HasNull ? false : nulls3[i];
                boolean null4 = !input4HasNull ? false : nulls4[i];
                boolean b1 = (array1[i] != 0);
                boolean b2 = (array2[i] != 0);
                boolean b3 = (array3[i] != 0);
                boolean b4 = (array4[i] != 0);

                outputNulls[i] = null1 || null2 || null3 || null4;
                res[i] = (b1 || b2 || b3 || b4) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }
}
