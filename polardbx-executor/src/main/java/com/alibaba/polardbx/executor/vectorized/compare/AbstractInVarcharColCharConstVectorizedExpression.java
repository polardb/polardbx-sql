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
import com.alibaba.polardbx.executor.chunk.columnar.CommonLazyBlock;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.executor.operator.scan.impl.MultiDictionaryMapping;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractInVarcharColCharConstVectorizedExpression extends AbstractVectorizedExpression {
    protected final CollationHandler collationHandler;

    protected final boolean[] operandIsNulls;
    protected final Slice[] operands;
    protected boolean allOperandsNull = true;
    protected Comparable[] operandSortKeys;

    protected DictionaryMapping mapping;

    public AbstractInVarcharColCharConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        SliceType sliceType = (SliceType) children[0].getOutputDataType();
        this.collationHandler = sliceType.getCollationHandler();

        this.operandIsNulls = new boolean[operandCount()];
        this.operands = new Slice[operandCount()];

        Set<Slice> dictSet = new HashSet<>();
        List<Slice> dictList = new ArrayList<>();
        for (int operandIndex = 1; operandIndex <= operandCount(); operandIndex++) {
            Object operand1Value = ((LiteralVectorizedExpression) children[operandIndex]).getConvertedValue();
            if (operand1Value == null) {
                operandIsNulls[operandIndex - 1] = true;
                operands[operandIndex - 1] = null;
            } else {
                allOperandsNull = false;
                operandIsNulls[operandIndex - 1] = false;
                Slice operand = sliceType.convertFrom(operand1Value);
                operands[operandIndex - 1] = operand;

                // collect dictionary id
                if (!dictSet.contains(operand)) {
                    dictSet.add(operand);
                    dictList.add(operand);
                }
            }
        }

        if (!dictSet.isEmpty()) {
            mapping = new MultiDictionaryMapping(dictList);
        } else {
            mapping = null;
        }

    }

    abstract int operandCount();

    protected long anyMatch(Comparable sortKey) {
        for (int operandIndex = 1; operandIndex <= operandCount(); operandIndex++) {
            if (sortKey != null && sortKey.compareTo(operandSortKeys[operandIndex - 1]) == 0) {
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

        final boolean compatible =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        if (operandSortKeys == null) {
            this.operandSortKeys = new Comparable[operandCount()];
            for (int operandIndex = 1; operandIndex <= operandCount(); operandIndex++) {
                if (operandIsNulls[operandIndex - 1]) {
                    operandSortKeys[operandIndex - 1] = null;
                } else if (compatible) {
                    operandSortKeys[operandIndex - 1] =
                        collationHandler.getSortKey(operands[operandIndex - 1], 1024);
                } else {
                    operandSortKeys[operandIndex - 1] = operands[operandIndex - 1];
                }
            }

        }

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        long[] output = (outputVectorSlot.cast(LongBlock.class)).longArray();

        if (allOperandsNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            outputVectorSlot.setHasNull(true);
            Arrays.fill(outputNulls, true);
            return;
        } else {
            VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
        }

        // Try to use dictionary
        BlockDictionary blockDictionary;
        if (mapping != null
            && !compatible
            && (leftInputVectorSlot instanceof SliceBlock || leftInputVectorSlot instanceof CommonLazyBlock)
            && (blockDictionary = leftInputVectorSlot.cast(SliceBlock.class).getDictionary()) != null) {
            SliceBlock sliceBlock = leftInputVectorSlot.cast(SliceBlock.class);

            int[] reMapping = mapping.merge(blockDictionary);

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    int dictId = sliceBlock.getDictId(j);
                    output[j] = (dictId >= 0 && reMapping[dictId] >= 0)
                        ? LongBlock.TRUE_VALUE
                        : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    int dictId = sliceBlock.getDictId(i);
                    output[i] = (dictId >= 0 && reMapping[dictId] >= 0)
                        ? LongBlock.TRUE_VALUE
                        : LongBlock.FALSE_VALUE;
                }
            }

            return;
        }

        if (!compatible && leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = leftInputVectorSlot.cast(SliceBlock.class);
            // best case.
            if (operandCount() == 1) {
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        output[j] = sliceBlock.equals(j, (Slice) operandSortKeys[0]);
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        output[i] = sliceBlock.equals(i, (Slice) operandSortKeys[0]);
                    }
                }
            } else if (operandCount() == 2) {
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        output[j] = sliceBlock.anyMatch(j, (Slice) operandSortKeys[0], (Slice) operandSortKeys[1]);
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        output[i] = sliceBlock.anyMatch(i, (Slice) operandSortKeys[0], (Slice) operandSortKeys[1]);
                    }
                }
            } else if (operandCount() == 3) {
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        output[j] = sliceBlock.anyMatch(j, (Slice) operandSortKeys[0], (Slice) operandSortKeys[1],
                            (Slice) operandSortKeys[2]);
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        output[i] = sliceBlock.anyMatch(i, (Slice) operandSortKeys[0], (Slice) operandSortKeys[1],
                            (Slice) operandSortKeys[2]);
                    }
                }
            } else {
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        output[j] = sliceBlock.anyMatch(j, operandSortKeys);
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        output[i] = sliceBlock.anyMatch(i, operandSortKeys);
                    }
                }
            }
        } else {
            if (leftInputVectorSlot instanceof SliceBlock) {
                // normal case.
                SliceBlock sliceBlock = leftInputVectorSlot.cast(SliceBlock.class);

                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];

                        Comparable sortKey = compatible ? sliceBlock.getSortKey(j) : sliceBlock.getRegion(j);

                        output[j] = anyMatch(sortKey);
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {

                        Comparable sortKey = compatible ? sliceBlock.getSortKey(i) : sliceBlock.getRegion(i);

                        output[i] = anyMatch(sortKey);
                    }
                }
            } else if (leftInputVectorSlot instanceof ReferenceBlock) {
                // bad case.
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];

                        Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                        Comparable sortKey = compatible ? this.collationHandler.getSortKey(lSlice, 1024) : lSlice;

                        output[j] = anyMatch(sortKey);
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {

                        Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                        Comparable sortKey = compatible ? this.collationHandler.getSortKey(lSlice, 1024) : lSlice;

                        output[i] = anyMatch(sortKey);
                    }
                }
            }
        }
    }
}
