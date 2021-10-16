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

package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.optimizer.chunk.DecimalBlock;
import com.alibaba.polardbx.optimizer.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.IS_NEG_OFFSET;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.POSITIVE_FLAG;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"ABS"}, argumentTypes = {"Decimal"}, argumentKinds = {Variable})
public class AbsDecimalVectorizedExpression extends AbstractVectorizedExpression {
    public AbsDecimalVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.DecimalType, outputIndex, children);
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
        Slice output = ((DecimalBlock) outputVectorSlot).getMemorySegments();

        // handle nulls
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // The convert result will directly wrote to decimal memory segment
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);

                // directly copy from input memory segment
                output.setBytes(0, input, fromIndex, DECIMAL_MEMORY_SIZE);

                // set negative flag
                decimalMemorySegment.setByteUnchecked(IS_NEG_OFFSET, POSITIVE_FLAG);
            }
        } else {
            // copy all memory segment at once.
            output.setBytes(0, input, 0, batchSize * DECIMAL_MEMORY_SIZE);

            for (int i = 0; i < batchSize; i++) {

                // find the position of sign and set negative flag
                int fromIndex = i * DECIMAL_MEMORY_SIZE + IS_NEG_OFFSET;

                output.setByteUnchecked(fromIndex, POSITIVE_FLAG);
            }
        }
    }
}