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
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(
    names = {"BETWEEN ASYMMETRIC"},
    argumentTypes = {"Decimal", "Decimal", "Decimal"},
    argumentKinds = {Variable, Const, Const},
    priority = ExpressionPriority.SPECIAL
)
public class FastBetweenDecimalColDecimalConstDecimalConstVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean operand1IsNull;
    private final Decimal operand1;
    private long operand1Long;

    private final boolean operand2IsNull;
    private final Decimal operand2;
    private long operand2Long;

    /**
     * when operand1, operand2 and InputBlock are of the same scale,
     * which are also decimal64 type,
     * they can be compared in long value
     */
    private boolean useCompareWithDecimal64;

    public FastBetweenDecimalColDecimalConstDecimalConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        Object operand2Value = ((LiteralVectorizedExpression) children[2]).getConvertedValue();

        this.useCompareWithDecimal64 = false;
        if (operand1Value == null) {
            this.operand1 = Decimal.ZERO;
            this.operand1IsNull = true;
            this.operand1Long = 0;
        } else {
            this.operand1IsNull = false;
            this.operand1 = (Decimal) operand1Value;
        }
        if (operand2Value == null) {
            this.operand2 = Decimal.ZERO;
            this.operand2IsNull = true;
            this.operand2Long = 0;
        } else {
            this.operand2IsNull = false;
            this.operand2 = (Decimal) operand2Value;
        }
        if (operand1Value == null && operand2Value == null) {
            this.useCompareWithDecimal64 = true;
            return;
        }

        if (!checkSameScale()) {
            return;
        }

        if (!checkDecimal64Precision()) {
            return;
        }

        try {
            DecimalStructure tmpBuffer = new DecimalStructure();
            this.operand1Long = operand1.unscale(tmpBuffer);
            this.operand2Long = operand2.unscale(tmpBuffer);
            this.useCompareWithDecimal64 = true;
        } catch (Exception e) {
            this.useCompareWithDecimal64 = false;
        }
    }

    private boolean checkDecimal64Precision() {
        return DecimalConverter.isDecimal64(operand1.precision())
            && DecimalConverter.isDecimal64(operand2.precision());
    }

    private boolean checkSameScale() {
        return operand1.scale() == operand2.scale();
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
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType()).cast(DecimalBlock.class);

        long[] output = outputVectorSlot.cast(LongBlock.class).longArray();

        DecimalStructure leftDec;
        DecimalStructure operand1Dec = operand1.getDecimalStructure();
        DecimalStructure operand2Dec = operand2.getDecimalStructure();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (useCompareWithDecimal64) {
            boolean sameScale = (operand1.scale() == leftInputVectorSlot.getScale() &&
                operand2.scale() == leftInputVectorSlot.getScale());
            if (leftInputVectorSlot.isDecimal64() && sameScale) {
                compareByDecimal64(batchSize, isSelectionInUse, sel, leftInputVectorSlot, output);
                return;
            }
        }

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(j));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0;
                boolean b2 = FastDecimalUtils.compare(leftDec, operand2Dec) <= 0;

                output[j] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(i));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0;
                boolean b2 = FastDecimalUtils.compare(leftDec, operand2Dec) <= 0;

                output[i] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }

    private void compareByDecimal64(int batchSize, boolean isSelectionInUse,
                                    int[] sel, DecimalBlock leftInputVectorSlot, long[] output) {

        long[] array1 = leftInputVectorSlot.getDecimal64Values();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                boolean b1 = array1[j] >= operand1Long;
                boolean b2 = array1[j] <= operand2Long;

                output[j] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {

                boolean b1 = array1[i] >= operand1Long;
                boolean b2 = array1[i] <= operand2Long;

                output[i] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }
}
