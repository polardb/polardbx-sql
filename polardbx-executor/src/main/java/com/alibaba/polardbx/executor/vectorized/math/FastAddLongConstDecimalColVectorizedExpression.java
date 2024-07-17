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
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.MathUtils;
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
    private final boolean useLeftWithScale;
    private final long leftWithScale;

    public FastAddLongConstDecimalColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.DecimalType, outputIndex, children);
        Object leftValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        if (leftValue == null) {
            leftIsNull = true;
            left = (long) 0;
            leftWithScale = 0;
            useLeftWithScale = true;
        } else {
            leftIsNull = false;
            left = (long) leftValue;
            if (left == 0) {
                leftWithScale = 0;
                useLeftWithScale = true;
                return;
            }
            int scale = children[1].getOutputDataType().getScale();
            if (scale < 0 || scale >= DecimalTypeBase.POW_10.length) {
                leftWithScale = 0;
                useLeftWithScale = false;
            } else {
                long power = DecimalTypeBase.POW_10[scale];
                leftWithScale = left * power;
                useLeftWithScale = !MathUtils.longMultiplyOverflow(left, power, leftWithScale);
            }
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

        DecimalBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType).cast(DecimalBlock.class);
        DecimalBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType()).cast(DecimalBlock.class);
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());
        boolean useLeftAsLong = useLeftWithScale
            && checkResultScale(rightInputVectorSlot.getScale(), outputVectorSlot.getScale());

        if (rightInputVectorSlot.isDecimal64() && useLeftAsLong) {
            boolean success = doDecimal64Add(batchSize, isSelectionInUse, sel, rightInputVectorSlot, outputVectorSlot);
            if (success) {
                return;
            }
        }

        if (rightInputVectorSlot.isDecimal128() && useLeftAsLong) {
            boolean success = doDecimal128Add(batchSize, isSelectionInUse, sel, rightInputVectorSlot, outputVectorSlot);
            if (success) {
                return;
            }
        }

        DecimalStructure leftDec = new DecimalStructure();

        DecimalConverter.longToDecimal(left, leftDec, children[0].getOutputDataType().isUnsigned());

        boolean isNull[] = outputVectorSlot.nulls();

        boolean enableFastVec =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_DECIMAL_FAST_VEC);

        rightInputVectorSlot.collectDecimalInfo();
        boolean useFastMethod = !isSelectionInUse
            && (rightInputVectorSlot.isSimple() && rightInputVectorSlot.getInt2Pos() == -1);

        if (!useFastMethod || !enableFastVec) {
            normalAdd(batchSize, isSelectionInUse, sel, rightInputVectorSlot, outputVectorSlot, leftDec);
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
                int carry = (int) (sum / 1000_000_000);
                if (sum < 1000_000_000) {
                    outputVectorSlot.setAddResult1(i, (int) sum, (int) b2);
                } else {
                    outputVectorSlot.setAddResult2(i, carry, (int) (sum - carry * 1000_000_000), (int) b2);
                }
            }
        }
    }

    private boolean checkResultScale(int scale, int resultScale) {
        return scale == resultScale;
    }

    /**
     * @return success: add done without overflow to normal
     */
    private boolean doDecimal64Add(int batchSize, boolean isSelectionInUse, int[] sel,
                                   DecimalBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        long[] decimal64Output = outputVectorSlot.allocateDecimal64();
        boolean isOverflowDec64 = false;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                long rightDec64 = rightInputVectorSlot.getLong(j);
                long result = leftWithScale + rightDec64;

                decimal64Output[j] = result;
                isOverflowDec64 |= (((left ^ result) & (rightDec64 ^ result)) < 0);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long rightDec64 = rightInputVectorSlot.getLong(i);
                long result = leftWithScale + rightDec64;

                decimal64Output[i] = result;
                isOverflowDec64 |= (((left ^ result) & (rightDec64 ^ result)) < 0);
            }
        }
        if (!isOverflowDec64) {
            return true;
        }
        // long + decimal64 不会溢出 decimal128
        outputVectorSlot.allocateDecimal128();

        long[] outputDec128Lows = outputVectorSlot.getDecimal128LowValues();
        long[] outputDec128Highs = outputVectorSlot.getDecimal128HighValues();

        long leftHigh = leftWithScale >= 0 ? 0 : -1;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                long rightDec128Low = rightInputVectorSlot.getLong(j);
                long rightDec128High = rightDec128Low >= 0 ? 0 : -1;
                long newDec128High = leftHigh + rightDec128High;
                long newDec128Low = leftWithScale + rightDec128Low;
                long carryOut = ((leftWithScale & rightDec128Low)
                    | ((leftWithScale | rightDec128Low) & (~newDec128Low))) >>> 63;
                long resultHigh = newDec128High + carryOut;

                outputDec128Lows[j] = newDec128Low;
                outputDec128Highs[j] = resultHigh;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long rightDec128Low = rightInputVectorSlot.getLong(i);
                long rightDec128High = rightDec128Low >= 0 ? 0 : -1;
                long newDec128High = leftHigh + rightDec128High;
                long newDec128Low = leftWithScale + rightDec128Low;
                long carryOut = ((leftWithScale & rightDec128Low)
                    | ((leftWithScale | rightDec128Low) & (~newDec128Low))) >>> 63;
                long resultHigh = newDec128High + carryOut;

                outputDec128Lows[i] = newDec128Low;
                outputDec128Highs[i] = resultHigh;
            }
        }
        return true;
    }

    /**
     * @return success: add done without overflow to normal
     */
    private boolean doDecimal128Add(int batchSize, boolean isSelectionInUse, int[] sel,
                                    DecimalBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        outputVectorSlot.allocateDecimal128();
        long[] outputDec128Lows = outputVectorSlot.getDecimal128LowValues();
        long[] outputDec128Highs = outputVectorSlot.getDecimal128HighValues();

        long leftHigh = leftWithScale >= 0 ? 0 : -1;

        boolean isOverflow = false;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                long rightDec128Low = rightInputVectorSlot.getDecimal128Low(j);
                long rightDec128High = rightInputVectorSlot.getDecimal128High(j);

                long newDec128High = leftHigh + rightDec128High;
                long newDec128Low = leftWithScale + rightDec128Low;
                long carryOut = ((leftWithScale & rightDec128Low)
                    | ((leftWithScale | rightDec128Low) & (~newDec128Low))) >>> 63;
                long resultHigh = newDec128High + carryOut;

                outputDec128Lows[j] = newDec128Low;
                outputDec128Highs[j] = resultHigh;
                isOverflow |= (((newDec128High ^ resultHigh) & (carryOut ^ resultHigh)) < 0);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long rightDec128Low = rightInputVectorSlot.getDecimal128Low(i);
                long rightDec128High = rightInputVectorSlot.getDecimal128High(i);
                long newDec128High = leftHigh + rightDec128High;
                long newDec128Low = leftWithScale + rightDec128Low;
                long carryOut = ((leftWithScale & rightDec128Low)
                    | ((leftWithScale | rightDec128Low) & (~newDec128Low))) >>> 63;
                long resultHigh = newDec128High + carryOut;

                outputDec128Lows[i] = newDec128Low;
                outputDec128Highs[i] = resultHigh;
                isOverflow |= (((newDec128High ^ resultHigh) & (carryOut ^ resultHigh)) < 0);
            }
        }
        if (isOverflow) {
            outputVectorSlot.deallocateDecimal128();
        }
        return !isOverflow;
    }

    private void normalAdd(int batchSize, boolean isSelectionInUse, int[] sel,
                           DecimalBlock rightInputVectorSlot,
                           DecimalBlock outputVectorSlot, DecimalStructure leftDec) {
        Slice output = outputVectorSlot.getMemorySegments();

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
        outputVectorSlot.setFullState();
    }
}
