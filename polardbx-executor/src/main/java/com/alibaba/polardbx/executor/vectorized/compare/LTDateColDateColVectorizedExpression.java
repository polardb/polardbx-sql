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

import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"LT", "<"}, argumentTypes = {"Date", "Date"}, argumentKinds = {Variable, Variable})
public class LTDateColDateColVectorizedExpression extends AbstractVectorizedExpression {

    public LTDateColDateColVectorizedExpression(int outputIndex,
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
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        if (leftInputVectorSlot instanceof DateBlock && rightInputVectorSlot instanceof DateBlock) {
            long[] array1 = leftInputVectorSlot.cast(DateBlock.class).getPacked();
            long[] array2 = rightInputVectorSlot.cast(DateBlock.class).getPacked();
            long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    res[j] = (array1[j] < array2[j]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    res[i] = (array1[i] < array2[i]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        } else if (leftInputVectorSlot instanceof ReferenceBlock && rightInputVectorSlot instanceof ReferenceBlock) {
            long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, j);
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, j);

                    res[j] = lPack < rPack ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, i);
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, i);

                    res[i] = lPack < rPack ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        } else if (leftInputVectorSlot instanceof DateBlock && rightInputVectorSlot instanceof ReferenceBlock) {
            long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
            long[] array1 = leftInputVectorSlot.cast(DateBlock.class).getPacked();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, j);
                    res[j] = (array1[j] < rPack) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long rPack = VectorizedExpressionUtils.packedLong(rightInputVectorSlot, i);
                    res[i] = (array1[i] < rPack) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        } else if (leftInputVectorSlot instanceof ReferenceBlock && rightInputVectorSlot instanceof DateBlock) {
            long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
            long[] array2 = ((DateBlock) rightInputVectorSlot).getPacked();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, j);
                    res[j] = (lPack < array2[j]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long lPack = VectorizedExpressionUtils.packedLong(leftInputVectorSlot, i);
                    res[i] = (lPack < array2[i]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }

            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
                children[1].getOutputIndex());
        }

    }
}
