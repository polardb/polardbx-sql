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

package com.alibaba.polardbx.executor.vectorized.convert;

import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.optimizer.chunk.DecimalBlock;
import com.alibaba.polardbx.optimizer.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.chunk.RandomAccessBlock;
import com.alibaba.polardbx.optimizer.chunk.ULongBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"CastToUnsigned", "ConvertToUnsigned"}, argumentTypes = {"Decimal"},
    argumentKinds = {Variable})
public class CastDecimalToUnsignedVectorizedExpression extends AbstractVectorizedExpression {
    public CastDecimalToUnsignedVectorizedExpression(DataType<?> outputDataType, int outputIndex,
                                                   VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock inputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        Slice input = ((DecimalBlock) inputVectorSlot).getMemorySegments();
        long[] output = ((ULongBlock) outputVectorSlot).longArray();

        DecimalStructure tmpDecimal = new DecimalStructure();

        // handle nulls
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // The convert result will directly wrote to decimal memory segment
                DecimalStructure fromValue = new DecimalStructure(input.slice(fromIndex, DECIMAL_MEMORY_SIZE));

                tmpDecimal.reset();
                FastDecimalUtils.round(fromValue, tmpDecimal, 0, DecimalRoundMod.HALF_UP);
                long l = DecimalConverter.decimalToULong(tmpDecimal)[0];
                output[j] = l;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // The convert result will directly wrote to decimal memory segment
                DecimalStructure fromValue = new DecimalStructure(input.slice(fromIndex, DECIMAL_MEMORY_SIZE));

                tmpDecimal.reset();
                FastDecimalUtils.round(fromValue, tmpDecimal, 0, DecimalRoundMod.HALF_UP);
                long l = DecimalConverter.decimalToULong(tmpDecimal)[0];
                output[i] = l;
            }
        }
    }
}
