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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"BETWEEN ASYMMETRIC"}, argumentTypes = {"Decimal", "Decimal", "Decimal"}, argumentKinds = {Variable, Const, Const})
public class BetweenDecimalColDecimalConstDecimalConstVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean operand1IsNull;
    private final Decimal operand1;

    private final boolean operand2IsNull;
    private final Decimal operand2;

    public BetweenDecimalColDecimalConstDecimalConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (operand1Value  == null) {
            operand1IsNull = true;
            operand1 = Decimal.ZERO;
        } else {
            operand1IsNull = false;
            operand1 = (Decimal) operand1Value;
        }

        Object operand2Value = ((LiteralVectorizedExpression) children[2]).getConvertedValue();
        if (operand2Value  == null) {
            operand2IsNull = true;
            operand2 = Decimal.ZERO;
        } else {
            operand2IsNull = false;
            operand2 = (Decimal) operand2Value;
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

        DecimalStructure leftDec;
        DecimalStructure operand1Dec = operand1.getDecimalStructure();
        DecimalStructure operand2Dec = operand2.getDecimalStructure();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                // fetch left decimal value
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(j));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0;
                boolean b2 = FastDecimalUtils.compare(leftDec, operand2Dec) <= 0;

                output[j] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                // fetch left decimal value
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(i));

                boolean b1 = FastDecimalUtils.compare(leftDec, operand1Dec) >= 0;
                boolean b2 = FastDecimalUtils.compare(leftDec, operand2Dec) <= 0;

                output[i] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }
}
