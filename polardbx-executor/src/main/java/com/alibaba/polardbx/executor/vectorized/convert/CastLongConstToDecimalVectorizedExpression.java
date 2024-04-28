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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;

@SuppressWarnings("unused")
@ExpressionSignatures(
    names = {"CastToDecimal", "ConvertToDecimal"},
    argumentTypes = {"Long"},
    argumentKinds = {Const})
public class CastLongConstToDecimalVectorizedExpression extends AbstractVectorizedExpression {

    private final Decimal operand1;
    private final boolean operand1IsNull;
    private final boolean useOperand1WithScale;
    private final long operand1WithScale;

    public CastLongConstToDecimalVectorizedExpression(DataType<?> outputDataType, int outputIndex,
                                                      VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);
        Object operand1Value = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        if (operand1Value == null) {
            operand1IsNull = true;
            operand1 = Decimal.ZERO;
            operand1WithScale = 0;
            useOperand1WithScale = true;
        } else {
            operand1IsNull = false;
            operand1 = DataTypes.DecimalType.convertFrom(operand1Value);
            long left = (long) operand1Value;
            if (left == 0) {
                operand1WithScale = 0;
                useOperand1WithScale = true;
                return;
            }
            int scale = outputDataType.getScale();
            if (scale < 0 || scale >= DecimalTypeBase.POW_10.length) {
                operand1WithScale = 0;
                useOperand1WithScale = false;
            } else {
                long power = DecimalTypeBase.POW_10[scale];
                operand1WithScale = left * power;
                useOperand1WithScale = !MathUtils.longMultiplyOverflow(left, power, operand1WithScale);
            }
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        DecimalBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType).cast(DecimalBlock.class);
        if (operand1IsNull) {
            VectorizedExpressionUtils.setNulls(chunk, outputIndex);
            return;
        }

        if (useOperand1WithScale) {
            castOperandToDecimal64(outputVectorSlot, batchSize, isSelectionInUse, sel);
        } else {
            castOperandToNormalDecimal(outputVectorSlot, batchSize, isSelectionInUse, sel);
        }
    }

    /**
     * Common cases: write long to decimal64 output slot
     */
    private void castOperandToDecimal64(DecimalBlock outputVectorSlot, int batchSize, boolean isSelectionInUse,
                                        int[] sel) {
        long[] decimal64Output = outputVectorSlot.allocateDecimal64();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                decimal64Output[j] = operand1WithScale;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                decimal64Output[i] = operand1WithScale;
            }
        }
    }

    private void castOperandToNormalDecimal(DecimalBlock outputVectorSlot, int batchSize, boolean isSelectionInUse,
                                            int[] sel) {
        Slice output = (outputVectorSlot.cast(DecimalBlock.class)).getMemorySegments();
        DecimalStructure tmpDecimal = new DecimalStructure();
        int precision = outputDataType.getPrecision();
        int scale = outputDataType.getScale();
        DecimalConverter.rescale(operand1.getDecimalStructure(), tmpDecimal, precision, scale, false);

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);
                tmpDecimal.copyTo(toValue);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);
                tmpDecimal.copyTo(toValue);
            }
        }
        outputVectorSlot.setFullState();
    }

}