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

import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;

import java.util.Arrays;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"NE","!=","<>"}, argumentTypes = {"Varchar", "Char"}, argumentKinds = {Variable, Const})
public class NEVarcharColCharConstVectorizedExpression extends AbstractVectorizedExpression {
    protected final CollationHandler collationHandler;

    protected final boolean operandIsNull;
    protected final Slice operand;

    public NEVarcharColCharConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        SliceType sliceType = (SliceType) children[0].getOutputDataType();
        this.collationHandler = sliceType.getCollationHandler();

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (operand1Value == null) {
            operandIsNull = true;
            operand = null;
        } else {
            operandIsNull = false;
            operand = sliceType.convertFrom(operand1Value);
        }

    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        final boolean compatible =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        Comparable operandSortKey;
        if (operand == null) {
            operandSortKey = null;
        } else if (compatible) {
            operandSortKey = collationHandler.getSortKey(operand, 1024);
        } else {
            operandSortKey = operand;
        }

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        long[] output = ((LongBlock) outputVectorSlot).longArray();

        if (operandIsNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            outputVectorSlot.setHasNull(true);
            Arrays.fill(outputNulls, true);
            return;
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = (SliceBlock) leftInputVectorSlot;

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Comparable sortKey = compatible ? sliceBlock.getSortKey(j) : sliceBlock.getRegion(j);

                    output[j] = (sortKey != null && sortKey.compareTo(operandSortKey) != 0)
                        ? LongBlock.TRUE_VALUE
                        : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {

                    Comparable sortKey = compatible ? sliceBlock.getSortKey(i) : sliceBlock.getRegion(i);

                    output[i] = (sortKey != null && sortKey.compareTo(operandSortKey) != 0)
                        ? LongBlock.TRUE_VALUE
                        : LongBlock.FALSE_VALUE;
                }
            }
        } else if (leftInputVectorSlot instanceof ReferenceBlock) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if (((ReferenceBlock<?>) leftInputVectorSlot).isNull(j)) {
                        continue;
                    }
                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    Comparable sortKey = compatible ? this.collationHandler.getSortKey(lSlice, 1024) : lSlice;

                    output[j] = sortKey.compareTo(operandSortKey) != 0
                        ? LongBlock.TRUE_VALUE
                        : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if (((ReferenceBlock<?>) leftInputVectorSlot).isNull(i)) {
                        continue;
                    }
                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    Comparable sortKey = compatible ? this.collationHandler.getSortKey(lSlice, 1024) : lSlice;

                    output[i] = sortKey.compareTo(operandSortKey) != 0
                        ? LongBlock.TRUE_VALUE
                        : LongBlock.FALSE_VALUE;
                }
            }
        }
    }
}
