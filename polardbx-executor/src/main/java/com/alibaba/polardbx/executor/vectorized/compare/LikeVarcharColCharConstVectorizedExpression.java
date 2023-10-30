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

import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"LIKE"}, argumentTypes = {"Varchar", "Char"}, argumentKinds = {Variable, Const})
public class LikeVarcharColCharConstVectorizedExpression extends AbstractVectorizedExpression {
    private final CollationHandler collationHandler;

    private final boolean operand1IsNull;
    private final Slice operand1;

    public LikeVarcharColCharConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        SliceType sliceType = (SliceType) children[0].getOutputDataType();
        this.collationHandler = CharsetFactory.DEFAULT_CHARSET_HANDLER.getCollationHandler();

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (operand1Value == null) {
            operand1IsNull = true;
            operand1 = null;
        } else {
            operand1IsNull = false;
            operand1 = sliceType.convertFrom(operand1Value);
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        long[] output = ((LongBlock) outputVectorSlot).longArray();

        if (operand1IsNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            outputVectorSlot.setHasNull(true);
            for (int i = 0; i < batchSize; i++) {
                outputNulls[i] = true;
            }
            return;
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = (SliceBlock) leftInputVectorSlot;

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice slice = sliceBlock.getRegion(j);

                    output[j] = collationHandler.wildCompare(slice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {

                    Slice slice = sliceBlock.getRegion(i);

                    output[i] = collationHandler.wildCompare(slice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        } else if (leftInputVectorSlot instanceof ReferenceBlock) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[j] = collationHandler.wildCompare(lSlice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[i] = collationHandler.wildCompare(lSlice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        }
    }
}
