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

import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

@ExpressionSignatures(
    names = {"-", "subtract"},
    argumentTypes = {"Long", "Decimal"},
    argumentKinds = {Const, Variable},
    priority = SPECIAL
)
public class FastSubLongConstDecimalColVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean leftIsNull;
    private final long left;

    public FastSubLongConstDecimalColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.DecimalType, outputIndex, children);
        Object leftValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        if (leftValue == null) {
            leftIsNull = true;
            left = (long) 0;
        } else {
            leftIsNull = false;
            left = (long) leftValue;
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[1].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        if (leftIsNull) {
            VectorizedExpressionUtils.setNulls(chunk, outputIndex);
            return;
        }

        DecimalBlock outputVectorSlot = (DecimalBlock) chunk.slotIn(outputIndex, outputDataType);
        DecimalBlock rightInputVectorSlot =
            (DecimalBlock) chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        Slice output = outputVectorSlot.getMemorySegments();

        DecimalStructure leftDec = new DecimalStructure();

        DecimalConverter.longToDecimal(left, leftDec, children[0].getOutputDataType().isUnsigned());

        boolean enableFastVec =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_DECIMAL_FAST_VEC);

        rightInputVectorSlot.collectDecimalInfo();
        boolean useFastMethod = !isSelectionInUse
            && (rightInputVectorSlot.isSimple() && rightInputVectorSlot.getInt2Pos() == -1);

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());
        boolean[] isNulls = outputVectorSlot.nulls();

        if (!useFastMethod || !enableFastVec) {
            normalSub(batchSize, isSelectionInUse, sel, rightInputVectorSlot, output, leftDec);
        } else {
            // a1 - (a2 + b2 * [-9])
            // = (a1 - a2) + (0 - b2) * [-9]
            // = (a1 - a2 - 1) + (1000_000_000 - b2) * [-9]

            final long a1 = left;
            long a2, b2;
            long sub0, sub9;
            boolean isNeg;

            for (int i = 0; i < batchSize; i++) {
                if (isNulls[i]) {
                    continue;
                }
                a2 = rightInputVectorSlot.fastInt1(i);
                b2 = rightInputVectorSlot.fastFrac(i);

                sub9 = b2 != 0 ? (1000_000_000 - b2) : 0;
                sub0 = a1 - a2 - (b2 != 0 ? 1 : 0);

                isNeg = sub0 < 0;
                sub0 = !isNeg ? sub0 : -sub0;

                if (sub0 < 1000_000_000) {
                    outputVectorSlot.setSubResult1(i, (int)sub0, (int)sub9, isNeg);
                } else {
                    outputVectorSlot.setSubResult2(i, 1, (int)(sub0 - 1000_000_000), (int)sub9, isNeg);
                }
            }
        }
    }

    private void normalSub(int batchSize, boolean isSelectionInUse, int[] sel, DecimalBlock rightInputVectorSlot,
                           Slice output, DecimalStructure leftDec) {
        DecimalStructure rightDec;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch right decimal value
                rightDec = new DecimalStructure(rightInputVectorSlot.getRegion(j));

                // do operator
                FastDecimalUtils.sub(leftDec, rightDec, toValue);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch right decimal value
                rightDec = new DecimalStructure(rightInputVectorSlot.getRegion(i));

                // do operator
                FastDecimalUtils.sub(leftDec, rightDec, toValue);
            }
        }
    }
}
