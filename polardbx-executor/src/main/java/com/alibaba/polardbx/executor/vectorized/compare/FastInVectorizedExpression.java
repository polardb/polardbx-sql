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

import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InValuesVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;

public class FastInVectorizedExpression extends AbstractVectorizedExpression {

    private final InValuesVectorizedExpression.InValueSet inValuesSet;
    private final boolean operandsAllNull;

    public FastInVectorizedExpression(DataType dataType,
                                      int outputIndex,
                                      VectorizedExpression[] children) {
        super(dataType, outputIndex, children);
        Preconditions.checkArgument(children.length == 2,
            "Unexpected IN vec expression children length: " + children.length);
        Preconditions.checkArgument(children[1] instanceof InValuesVectorizedExpression,
            "Unexpected IN values expression type: " + children[1].getClass().getSimpleName());
        InValuesVectorizedExpression inExpr = (InValuesVectorizedExpression) children[1];
        this.operandsAllNull = inExpr.allNull();
        this.inValuesSet = inExpr.getInValueSet();
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();
        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);

        long[] output = outputVectorSlot.cast(LongBlock.class).longArray();
        if (operandsAllNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    outputNulls[j] = true;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    outputNulls[i] = true;
                }
            }
            return;
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        if (leftInputVectorSlot.isInstanceOf(LongBlock.class)) {
            evalLongIn(output, leftInputVectorSlot.cast(LongBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        if (leftInputVectorSlot.isInstanceOf(IntegerBlock.class)) {
            evalIntIn(output, leftInputVectorSlot.cast(IntegerBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        // for datetime / timestamp type and the left input is not intermediate result.
        if (leftInputVectorSlot.isInstanceOf(TimestampBlock.class)) {
            evalDatetimeIn(output, leftInputVectorSlot.cast(TimestampBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        // for date type and the left input is not intermediate result.
        if (leftInputVectorSlot.isInstanceOf(DateBlock.class)) {
            evalDateIn(output, leftInputVectorSlot.cast(DateBlock.class), batchSize, isSelectionInUse, sel);
            return;
        }

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(leftInputVectorSlot.elementAt(j)) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(leftInputVectorSlot.elementAt(i)) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }

    private void evalIntIn(long[] output, IntegerBlock leftInputSlot,
                           int batchSize, boolean isSelectionInUse,
                           int[] sel) {
        int[] intArray = leftInputSlot.intArray();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(intArray[j]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(intArray[i]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }

    private void evalDatetimeIn(long[] output, TimestampBlock leftInputSlot,
                                int batchSize, boolean isSelectionInUse,
                                int[] sel) {
        long[] longArray = leftInputSlot.getPacked();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(longArray[j]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(longArray[i]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }

    private void evalDateIn(long[] output, DateBlock leftInputSlot,
                            int batchSize, boolean isSelectionInUse,
                            int[] sel) {
        long[] longArray = leftInputSlot.getPacked();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(longArray[j]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(longArray[i]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }

    private void evalLongIn(long[] output, LongBlock leftInputSlot,
                            int batchSize, boolean isSelectionInUse,
                            int[] sel) {
        long[] longArray = leftInputSlot.longArray();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                output[j] = inValuesSet.contains(longArray[j]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                output[i] = inValuesSet.contains(longArray[i]) ?
                    LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }
    }
}