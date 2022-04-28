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
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"CastToDecimal", "ConvertToDecimal"}, argumentTypes = {"Float"}, argumentKinds = {Variable})
public class CastFloatToDecimalVectorizedExpression extends AbstractVectorizedExpression {

    public CastFloatToDecimalVectorizedExpression(DataType<?> outputDataType, int outputIndex, VectorizedExpression[] children) {
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

        float[] input = ((FloatBlock) inputVectorSlot).floatArray();
        Slice output = ((DecimalBlock) outputVectorSlot).getMemorySegments();

        DecimalStructure tmpDecimal = new DecimalStructure();
        int precision = outputDataType.getPrecision();
        int scale = outputDataType.getScale();
        boolean isUnsigned = children[0].getOutputDataType().isUnsigned();

        // handle nulls
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // The convert result will directly wrote to decimal memory segment
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do convert operation
                tmpDecimal.reset();

                // parse string value.
                byte[] bytesValue = Float.toString(input[j]).getBytes();
                DecimalConverter.parseString(bytesValue, tmpDecimal, false);

                DecimalConverter.rescale(tmpDecimal, toValue, precision, scale, isUnsigned);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // The convert result will directly wrote to decimal memory segment
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do convert operation
                tmpDecimal.reset();

                // parse string value.
                byte[] bytesValue = Float.toString(input[i]).getBytes();
                DecimalConverter.parseString(bytesValue, tmpDecimal, false);

                DecimalConverter.rescale(tmpDecimal, toValue, precision, scale, isUnsigned);
            }
        }
    }
}