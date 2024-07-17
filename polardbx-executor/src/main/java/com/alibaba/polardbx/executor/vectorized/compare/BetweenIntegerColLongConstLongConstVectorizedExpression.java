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

import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"BETWEEN ASYMMETRIC"}, argumentTypes = {"Integer", "Long", "Long"},
    argumentKinds = {Variable, Const, Const})
public class BetweenIntegerColLongConstLongConstVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean operand1IsNull;
    private final long operand1;

    private final boolean operand2IsNull;
    private final long operand2;

    public BetweenIntegerColLongConstLongConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (operand1Value == null) {
            operand1IsNull = true;
            operand1 = LongBlock.NULL_VALUE;
        } else {
            operand1IsNull = false;
            operand1 = DataTypes.LongType.convertFrom(operand1Value);
        }

        Object operand2Value = ((LiteralVectorizedExpression) children[2]).getConvertedValue();
        if (operand2Value == null) {
            operand2IsNull = true;
            operand2 = LongBlock.NULL_VALUE;
        } else {
            operand2IsNull = false;
            operand2 = DataTypes.LongType.convertFrom(operand2Value);
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

        long[] output = outputVectorSlot.cast(LongBlock.class).longArray();

        if (operand1IsNull || operand2IsNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            outputVectorSlot.setHasNull(true);
            for (int i = 0; i < batchSize; i++) {
                outputNulls[i] = true;
            }
            return;
        }

        int[] array1 = leftInputVectorSlot.cast(IntegerBlock.class).intArray();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                boolean b1 = array1[j] >= operand1;
                boolean b2 = array1[j] <= operand2;

                output[j] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {

                boolean b1 = array1[i] >= operand1;
                boolean b2 = array1[i] <= operand2;

                output[i] = b1 && b2 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }

    }
}