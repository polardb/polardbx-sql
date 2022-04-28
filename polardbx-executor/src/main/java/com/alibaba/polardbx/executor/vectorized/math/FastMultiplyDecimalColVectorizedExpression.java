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

import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import java.util.Arrays;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

/*
 * This class is generated using freemarker and the DecimalAddSubMulOperatorColumnColumn.ftl template.
 */
@SuppressWarnings("unused")
@ExpressionSignatures(
    names = {"*", "multiply"},
    argumentTypes = {"Decimal", "Decimal"},
    argumentKinds = {Variable, Variable},
    priority = SPECIAL)
public class FastMultiplyDecimalColVectorizedExpression extends AbstractVectorizedExpression {
    // for fast decimal multiply
    long[] sum0s;
    long[] sum9s;
    long[] sum18s;
    long[] carry0s;
    long[] carry9s;
    long[] carry18s;
    int[] nonNullSelection;

    public FastMultiplyDecimalColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.DecimalType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        DecimalBlock outputVectorSlot = (DecimalBlock) chunk.slotIn(outputIndex, outputDataType);
        DecimalBlock leftInputVectorSlot =
            (DecimalBlock) chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        DecimalBlock rightInputVectorSlot =
            (DecimalBlock) chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        Slice output = outputVectorSlot.getMemorySegments();

        VectorizedExpressionUtils
            .mergeNulls(chunk, outputIndex, children[0].getOutputIndex(), children[1].getOutputIndex());
        boolean[] isNulls = outputVectorSlot.nulls();

        // prepare for fast method
        boolean enableFastVec =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_DECIMAL_FAST_VEC);
        leftInputVectorSlot.collectDecimalInfo();
        rightInputVectorSlot.collectDecimalInfo();
        boolean useFastMethod = !isSelectionInUse
            && (leftInputVectorSlot.isSimple() && leftInputVectorSlot.getInt2Pos() == -1)
            && (rightInputVectorSlot.isSimple() && rightInputVectorSlot.getInt2Pos() == -1);

        // normal multiply
        if (!useFastMethod) {
            normalMul(chunk, batchSize, isSelectionInUse, sel, outputVectorSlot,
                leftInputVectorSlot, rightInputVectorSlot, output);
            return;
        } else if (enableFastVec) {
            // fast multiply 1
            fastMul1(batchSize, outputVectorSlot, leftInputVectorSlot, rightInputVectorSlot, isNulls);
        } else {
            // fast multiply 2
            fastMul2(batchSize, outputVectorSlot, leftInputVectorSlot, rightInputVectorSlot, isNulls);
        }
    }

    private void fastMul1(int batchSize, DecimalBlock outputVectorSlot, DecimalBlock leftInputVectorSlot,
                          DecimalBlock rightInputVectorSlot, boolean[] isNulls) {
        long a1, b1;
        long a2, b2;
        long sum0, sum9, sum18;
        long carry0 = 0, carry9 = 0, carry18 = 0;
        for (int i = 0; i < batchSize; i++) {
            if (isNulls[i]) {
                continue;
            }

            a1 = leftInputVectorSlot.fastInt1(i);
            b1 = leftInputVectorSlot.fastFrac(i);

            a2 = rightInputVectorSlot.fastInt1(i);
            b2 = rightInputVectorSlot.fastFrac(i);

            // (a1 * [0] + b1 * [-9]) * (a2 * [0] + b2 * [-9])
            // = (a1 * a2) * [0]
            // + (a1 * b2 + b1 * a2) * [-9]
            // + (b1 * b2) * [-18]

            // handle carry:
            // (a + carry * 1000_000_000) * [9n] = (a) * [9n] + carry * [9(n+1)]

            sum18 = b1 * b2;
            if (sum18 > 1000_000_000L) {
                carry18 = sum18 / 1000_000_000L;
                sum18 -= carry18 * 1000_000_000L;
            }

            sum9 = a1 * b2 + b1 * a2 + carry18;
            if (sum9 > 1000_000_000L) {
                carry9 = sum9 / 1000_000_000L;
                sum9 -= carry9 * 1000_000_000L;
            }

            sum0 = a1 * a2 + carry9;
            if (sum0 > 1000_000_000L) {
                carry0 = sum0 / 1000_000_000L;
                sum0 -= carry0 * 1000_000_000L;
            }

            if (carry0 == 0 && sum18 == 0) {
                outputVectorSlot.setMultiResult1(i, (int) sum0, (int) sum9);
            } else if (carry0 > 0 && sum18 == 0) {
                outputVectorSlot.setMultiResult2(i, (int) carry0, (int) sum0, (int) sum9);
            } else if (carry0 == 0 && sum18 > 0) {
                outputVectorSlot.setMultiResult3(i, (int) sum0, (int) sum9, (int) sum18);
            } else {
                outputVectorSlot.setMultiResult4(i, (int) carry0, (int) sum0, (int) sum9, (int) sum18);
            }

            carry0 = carry9 = carry18 = 0;
        }
    }

    private void fastMul2(int batchSize, DecimalBlock outputVectorSlot, DecimalBlock leftInputVectorSlot,
                          DecimalBlock rightInputVectorSlot, boolean[] isNulls) {
        initForFastMethod();
        long a1, b1;
        long a2, b2;

        int nonNullBatchSize = 0;
        for (int i = 0; i < batchSize; i++) {
            if (!isNulls[i]) {
                nonNullSelection[nonNullBatchSize++] = i;
            }
        }

        // record sum
        for (int position = 0; position < nonNullBatchSize; position++) {
            int i = nonNullSelection[position];
            a1 = leftInputVectorSlot.fastInt1(i);
            b1 = leftInputVectorSlot.fastFrac(i);

            a2 = rightInputVectorSlot.fastInt1(i);
            b2 = rightInputVectorSlot.fastFrac(i);

            sum18s[i] = b1 * b2;
            sum9s[i] = a1 * b2 + b1 * a2;
            sum0s[i] = a1 * a2;
        }

        // record carry
        boolean allSum18Zero = true;
        boolean allCarry0Zero = true;
        for (int position = 0; position < nonNullBatchSize; position++) {
            int i = nonNullSelection[position];
            carry18s[i] = sum18s[i] / 1000_000_000L;
            sum18s[i] -= carry18s[i] * 1000_000_000L;

            allSum18Zero = allSum18Zero && sum18s[i] == 0;

            sum9s[i] += carry18s[i];

            carry9s[i] = sum9s[i] / 1000_000_000L;
            sum9s[i] -= carry9s[i] * 1000_000_000L;

            sum0s[i] += carry9s[i];

            carry0s[i] = sum0s[i] / 1000_000_000L;
            sum0s[i] -= carry0s[i] * 1000_000_000L;

            allCarry0Zero = allCarry0Zero && carry0s[i] == 0;
        }

        if (allCarry0Zero && allSum18Zero) {
            for (int position = 0; position < nonNullBatchSize; position++) {
                int i = nonNullSelection[position];

                outputVectorSlot.setMultiResult1(i, (int) sum0s[i], (int) sum9s[i]);
            }
        } else {
            for (int position = 0; position < nonNullBatchSize; position++) {
                int i = nonNullSelection[position];

                if (carry0s[i] == 0 && sum18s[i] == 0) {
                    outputVectorSlot.setMultiResult1(i, (int) sum0s[i], (int) sum9s[i]);
                } else if (carry0s[i] > 0 && sum18s[i] == 0) {
                    outputVectorSlot.setMultiResult2(i, (int) carry0s[i], (int) sum0s[i], (int) sum9s[i]);
                } else if (carry0s[i] == 0 && sum18s[i] > 0) {
                    outputVectorSlot.setMultiResult3(i, (int) sum0s[i], (int) sum9s[i], (int) sum18s[i]);
                } else {
                    outputVectorSlot
                        .setMultiResult4(i, (int) carry0s[i], (int) sum0s[i], (int) sum9s[i], (int) sum18s[i]);
                }
            }
        }

        Arrays.fill(carry0s, 0);
        Arrays.fill(carry9s, 0);
        Arrays.fill(carry18s, 0);

        Arrays.fill(nonNullSelection, 0);
    }

    private void normalMul(MutableChunk chunk, int batchSize, boolean isSelectionInUse, int[] sel,
                           RandomAccessBlock outputVectorSlot, DecimalBlock leftInputVectorSlot,
                           DecimalBlock rightInputVectorSlot, Slice output) {
        DecimalStructure leftDec;
        DecimalStructure rightDec;
        DecimalStructure tmpDec = new DecimalStructure();
        boolean isNull[] = outputVectorSlot.nulls();

        boolean isLeftUnsigned = children[0].getOutputDataType().isUnsigned();
        boolean isRightUnsigned = children[1].getOutputDataType().isUnsigned();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(j));

                // fetch right decimal value
                rightDec = new DecimalStructure(rightInputVectorSlot.getRegion(j));

                // do operator
                FastDecimalUtils.mul(leftDec, rightDec, toValue);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(i));

                // fetch right decimal value
                rightDec = new DecimalStructure(rightInputVectorSlot.getRegion(i));

                // do operator
                FastDecimalUtils.mul(leftDec, rightDec, toValue);
            }
        }
    }

    private void initForFastMethod() {
        if (sum0s != null) {
            return;
        }
        sum0s = new long[1000];
        sum9s = new long[1000];
        sum18s = new long[1000];
        carry0s = new long[1000];
        carry9s = new long[1000];
        carry18s = new long[1000];
        nonNullSelection = new int[1000];
    }
}
