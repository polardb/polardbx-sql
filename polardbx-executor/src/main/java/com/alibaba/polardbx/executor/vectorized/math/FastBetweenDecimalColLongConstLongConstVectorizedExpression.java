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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
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

import java.util.Arrays;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(
    names = {"BETWEEN ASYMMETRIC"},
    argumentTypes = {"Decimal", "Long", "Long"},
    argumentKinds = {Variable, Const, Const},
    priority = ExpressionPriority.SPECIAL
)
public class FastBetweenDecimalColLongConstLongConstVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean operand1IsNull;
    private final Decimal operand1;
    private final boolean operand2IsNull;
    private final Decimal operand2;

    private final long lower;
    private final long upper;

    public FastBetweenDecimalColLongConstLongConstVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        Object operand2Value = ((LiteralVectorizedExpression) children[2]).getConvertedValue();
        if (operand1Value == null) {
            operand1IsNull = true;
            operand1 = Decimal.ZERO;
            lower = 0;
        } else {
            operand1IsNull = false;
            operand1 = DataTypes.DecimalType.convertFrom(operand1Value);
            lower = DataTypes.LongType.convertFrom(operand1Value);
        }

        if (operand2Value == null) {
            operand2IsNull = true;
            operand2 = Decimal.ZERO;
            upper = 0;
        } else {
            operand2IsNull = false;
            operand2 = DataTypes.DecimalType.convertFrom(operand2Value);
            upper = DataTypes.LongType.convertFrom(operand2Value);
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
            (DecimalBlock) chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        boolean[] nulls = outputVectorSlot.nulls();

        if (operand1IsNull || operand2IsNull) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    nulls[j] = true;
                }
            } else {
                Arrays.fill(nulls, true);
            }
            return;
        }


        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        long[] output = ((LongBlock) outputVectorSlot).longArray();
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());

        boolean enableFastVec =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_DECIMAL_FAST_VEC);

        leftInputVectorSlot.collectDecimalInfo();
        boolean useFastMethod = (leftInputVectorSlot.isSimple() && leftInputVectorSlot.getInt2Pos() == -1);

        if (!useFastMethod || !enableFastVec) {
            doNormalCompare(batchSize, isSelectionInUse, sel, leftInputVectorSlot, output);
            return;
        }

        // a2 <= (a1 + b1 * [-9]) <= a3
        // =>
        // (b1 == 0 && a2 <= a1 <= a3)
        // ||
        // (b1 != 0 && a2 <= a1 < a3)
        long a1, b1;
        long a2 = lower;
        long a3 = upper;
        if(isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                a1 = leftInputVectorSlot.fastInt1(j);
                b1 = leftInputVectorSlot.fastFrac(j);

                boolean equal = (a2 <= a1 && ((b1 == 0 && a1 <= a3) || (b1 != 0 && a1 < a3)));
                output[j] = equal ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                a1 = leftInputVectorSlot.fastInt1(i);
                b1 = leftInputVectorSlot.fastFrac(i);

                boolean equal = (a2 <= a1 && ((b1 == 0 && a1 <= a3) || (b1 != 0 && a1 < a3)));
                output[i] = equal ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }

    private void doNormalCompare(int batchSize, boolean isSelectionInUse, int[] sel, DecimalBlock leftInputVectorSlot,
                                 long[] output) {
        DecimalStructure leftDec;
        DecimalStructure operand1Dec = operand1.getDecimalStructure();
        DecimalStructure operand2Dec = operand2.getDecimalStructure();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(j));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0 && FastDecimalUtils.compare(leftDec, operand2Dec) <= 0;

                output[j] = b1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {

                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(i));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0 && FastDecimalUtils.compare(leftDec, operand2Dec) <= 0;

                output[i] = b1 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }
}
