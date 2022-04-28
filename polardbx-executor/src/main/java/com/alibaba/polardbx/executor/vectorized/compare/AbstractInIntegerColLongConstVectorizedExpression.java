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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;

public abstract class AbstractInIntegerColLongConstVectorizedExpression extends AbstractVectorizedExpression {
    protected final boolean[] operandIsNulls;
    protected final long[] operands;

    public AbstractInIntegerColLongConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        this.operandIsNulls = new boolean[operandCount()];
        this.operands = new long[operandCount()];

        for (int operandIndex = 1; operandIndex <= operandCount(); operandIndex++) {
            Object operand1Value = ((LiteralVectorizedExpression) children[operandIndex]).getConvertedValue();
            if (operand1Value == null) {
                operandIsNulls[operandIndex - 1] = true;
                operands[operandIndex - 1] = LongBlock.NULL_VALUE;
            } else {
                operandIsNulls[operandIndex - 1] = false;
                operands[operandIndex - 1] = DataTypes.LongType.convertFrom(operand1Value);
            }
        }
    }

    abstract int operandCount();

    protected boolean anyOperandsNull() {
        boolean res = false;
        for (int operandIndex = 1; operandIndex <= operandCount(); operandIndex++) {
            res |= operandIsNulls[operandIndex - 1];
            if (res) {
                return true;
            }
        }
        return false;
    }

    protected long anyMatch(int left) {
        for (int operandIndex = 1; operandIndex <= operandCount(); operandIndex++) {
            if (left == operands[operandIndex - 1]) {
                return LongBlock.TRUE_VALUE;
            }
        }
        return LongBlock.FALSE_VALUE;
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

        if (anyOperandsNull()) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            outputVectorSlot.setHasNull(true);
            for (int i = 0; i < batchSize; i++) {
                outputNulls[i] = true;
            }
            return;
        }

        int[] intArray = ((IntegerBlock) leftInputVectorSlot).intArray();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                output[j] = anyMatch(intArray[j]);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {

                output[i] = anyMatch(intArray[i]);
            }
        }
    }
}
