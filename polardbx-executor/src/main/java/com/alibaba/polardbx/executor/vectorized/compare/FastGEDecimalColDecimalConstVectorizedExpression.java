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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

@ExpressionSignatures(
    names = {"GE", ">="},
    argumentTypes = {"Decimal", "Decimal"},
    argumentKinds = {Variable, Const},
    priority = SPECIAL
)
public class FastGEDecimalColDecimalConstVectorizedExpression extends AbstractVectorizedExpression {

    private final boolean operand1IsNull;
    private final Decimal operand1;
    private final boolean useOperand1WithScale;
    private final long operand1WithScale;

    public FastGEDecimalColDecimalConstVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();

        if (operand1Value == null) {
            operand1IsNull = true;
            operand1 = Decimal.ZERO;
            operand1WithScale = 0;
            useOperand1WithScale = true;
            return;
        }
        operand1IsNull = false;
        operand1 = (Decimal) operand1Value;
        if (operand1.compareTo(Decimal.ZERO) == 0) {
            operand1WithScale = 0;
            useOperand1WithScale = true;
            return;
        }
        if (!DecimalConverter.isDecimal64(operand1.precision())) {
            operand1WithScale = 0;
            useOperand1WithScale = false;
        } else {
            DecimalStructure tmpBuffer = new DecimalStructure();
            operand1WithScale = operand1.unscale(tmpBuffer);
            useOperand1WithScale = true;
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
        DecimalBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType())
                .cast(DecimalBlock.class);

        long[] output = (outputVectorSlot.cast(LongBlock.class)).longArray();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
        boolean useDecimal64Compare = useOperand1WithScale && leftInputVectorSlot.isDecimal64()
            && checkSameScale(leftInputVectorSlot);
        if (useDecimal64Compare) {
            // do Decimal64 compare
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    long leftVal = leftInputVectorSlot.getLong(j);
                    output[j] = leftVal >= operand1WithScale ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    // fetch left decimal value
                    long leftVal = leftInputVectorSlot.getLong(i);
                    output[i] = leftVal >= operand1WithScale ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
            return;
        }
        boolean useDecimal128Compare = useOperand1WithScale && leftInputVectorSlot.isDecimal128()
            && checkSameScale(leftInputVectorSlot);
        if (useDecimal128Compare) {
            long[] decimal128Low = leftInputVectorSlot.getDecimal128LowValues();
            long[] decimal128High = leftInputVectorSlot.getDecimal128HighValues();
            long rightLow = operand1WithScale;
            long rightHigh = operand1WithScale >= 0 ? 0 : -1;

            // do Decimal128 compare
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    int pos = leftInputVectorSlot.realPositionOf(j);

                    long leftLow = decimal128Low[pos];
                    long leftHigh = decimal128High[pos];

                    boolean greatEqual = !((leftHigh < rightHigh) || (leftHigh == rightHigh && leftLow < rightLow));
                    output[j] = greatEqual ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    // fetch left decimal value
                    int pos = leftInputVectorSlot.realPositionOf(i);

                    long leftLow = decimal128Low[pos];
                    long leftHigh = decimal128High[pos];

                    boolean greatEqual = !((leftHigh < rightHigh) || (leftHigh == rightHigh && leftLow < rightLow));
                    output[i] = greatEqual ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
            return;
        }

        // do normal decimal compare
        DecimalStructure leftDec;
        DecimalStructure operand1Dec = operand1.getDecimalStructure();
        Slice cachedSlice = leftInputVectorSlot.allocCachedSlice();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                // fetch left decimal value
                leftDec =
                    new DecimalStructure((leftInputVectorSlot.cast(DecimalBlock.class)).getRegion(j, cachedSlice));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0;

                output[j] = b1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                // fetch left decimal value
                leftDec =
                    new DecimalStructure((leftInputVectorSlot.cast(DecimalBlock.class)).getRegion(i, cachedSlice));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0;

                output[i] = b1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }

    private boolean checkSameScale(DecimalBlock leftInputVectorSlot) {
        return leftInputVectorSlot.getScale() == operand1.scale();
    }
}
