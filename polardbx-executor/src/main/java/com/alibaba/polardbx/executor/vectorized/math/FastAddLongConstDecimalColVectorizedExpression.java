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
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

@ExpressionSignatures(
    names = {"+", "add", "plus"},
    argumentTypes = {"Long", "Decimal"},
    argumentKinds = {Const, Variable},
    priority = SPECIAL
)
public class FastAddLongConstDecimalColVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean leftIsNull;
    private final long left;

    public FastAddLongConstDecimalColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
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

        boolean isNull[] = outputVectorSlot.nulls();

        boolean enableFastVec =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_DECIMAL_FAST_VEC);

        rightInputVectorSlot.collectDecimalInfo();
        boolean useFastMethod = !isSelectionInUse
            && (rightInputVectorSlot.isSimple() && rightInputVectorSlot.getInt2Pos() == -1);
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());

        if (!useFastMethod || !enableFastVec) {
            normalAdd(batchSize, isSelectionInUse, sel, rightInputVectorSlot, output, leftDec);
        } else {
            // a1 + (a2 + b2 * [-9])
            // = (a1 + a2) + b2 * [-9]
            final long a1 = left;
            long a2, b2;
            long sum;

            for (int i = 0; i < batchSize; i++) {
                if (isNull[i]) {
                    continue;
                }
                a2 = rightInputVectorSlot.fastInt1(i);
                b2 = rightInputVectorSlot.fastFrac(i);

                sum = a1 + a2;
                if (sum < 1000_000_000) {
                    outputVectorSlot.setAddResult1(i, (int) sum, (int) b2);
                } else {
                    outputVectorSlot.setAddResult2(i, 1, (int) (sum - 1000_000_000), (int) b2);
                }
            }

        }
    }

    private void normalAdd(int batchSize, boolean isSelectionInUse, int[] sel, DecimalBlock rightInputVectorSlot,
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
                FastDecimalUtils.add(leftDec, rightDec, toValue);
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
                FastDecimalUtils.add(leftDec, rightDec, toValue);
            }
        }
    }
}
