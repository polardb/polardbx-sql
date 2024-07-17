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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
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
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.alibaba.polardbx.optimizer.config.table.charset.CollationHandlers;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"LIKE"}, argumentTypes = {"Varchar", "Char"}, argumentKinds = {Variable, Const})
public class LikeVarcharColCharConstVectorizedExpression extends AbstractVectorizedExpression {
    private final CollationHandler originCollationHandler;
    private final CollationHandler latin1CollationHandler = CollationHandlers.COLLATION_HANDLER_LATIN1_BIN;
    private final boolean operand1IsNull;
    private final Slice operand1;
    /**
     * works with ENABLE_OSS_COMPATIBLE=false
     */
    private final boolean canUseLatin1Collation;
    private boolean isContainsCompare = false;
    private byte[] containBytes = null;
    private int[] lps = null;

    public LikeVarcharColCharConstVectorizedExpression(
        int outputIndex,
        VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        SliceType sliceType = (SliceType) children[0].getOutputDataType();
        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (operand1Value == null) {
            operand1IsNull = true;
            operand1 = null;
        } else {
            operand1IsNull = false;
            operand1 = sliceType.convertFrom(operand1Value);
        }
        // FIXME did not consider collation here
        this.originCollationHandler = CharsetFactory.DEFAULT_CHARSET_HANDLER.getCollationHandler();

        if (sliceType.isLatin1Encoding()) {
            canUseLatin1Collation = true;
            checkIsContainsCompare(operand1);
        } else {
            if (isAsciiEncoding(operand1)) {
                canUseLatin1Collation = true;
                checkIsContainsCompare(operand1);
            } else {
                canUseLatin1Collation = false;
            }
        }
    }

    public static int[] computeLPSArray(byte[] pattern) {
        int[] lps = new int[pattern.length];
        int length = 0;
        lps[0] = 0;
        int i = 1;

        while (i < pattern.length) {
            if (pattern[i] == pattern[length]) {
                length++;
                lps[i] = length;
                i++;
            } else {
                if (length != 0) {
                    length = lps[length - 1];
                } else {
                    lps[i] = length;
                    i++;
                }
            }
        }
        return lps;
    }

    private void checkIsContainsCompare(Slice operand) {
        if (operand == null || operand.length() < 2) {
            return;
        }
        if (!canUseLatin1Collation) {
            return;
        }
        byte[] bytes = operand.getBytes();
        if (bytes[0] == CollationHandler.WILD_MANY && bytes[bytes.length - 1] == CollationHandler.WILD_MANY) {
            for (int i = 1; i < bytes.length - 1; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ in the middle
                    return;
                }
            }
            this.isContainsCompare = true;
            this.containBytes = new byte[operand.length() - 2];
            System.arraycopy(bytes, 1, containBytes, 0, operand.length() - 2);
            this.lps = computeLPSArray(containBytes);
        }
    }

    private boolean isAsciiEncoding(Slice operand) {
        if (operand == null) {
            return true;
        }
        byte[] bytes = operand.getBytes();
        for (byte b : bytes) {
            if (b < 0 || b == CollationHandler.WILD_ONE) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);

        final boolean compatible =
            ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
        boolean useLatin1Compare = canUseLatin1Collation && !compatible;

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        if (operand1IsNull) {
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
        long[] output = (outputVectorSlot.cast(LongBlock.class)).longArray();

        if (useLatin1Compare) {
            doLatin1Like(leftInputVectorSlot, isSelectionInUse, sel, batchSize, output);
        } else {
            doCollationLike(leftInputVectorSlot, isSelectionInUse, sel, batchSize, output);
        }
    }

    private void doCollationLike(RandomAccessBlock leftInputVectorSlot, boolean isSelectionInUse, int[] sel,
                                 int batchSize, long[] output) {
        Slice cachedSlice = new Slice();

        if (leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = leftInputVectorSlot.cast(SliceBlock.class);

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice slice = sliceBlock.getRegion(j, cachedSlice);

                    output[j] = originCollationHandler.wildCompare(slice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {

                    Slice slice = sliceBlock.getRegion(i, cachedSlice);

                    output[i] = originCollationHandler.wildCompare(slice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        } else if (leftInputVectorSlot instanceof ReferenceBlock) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[j] = originCollationHandler.wildCompare(lSlice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[i] = originCollationHandler.wildCompare(lSlice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        }
    }

    private void doLatin1Like(RandomAccessBlock leftInputVectorSlot, boolean isSelectionInUse,
                              int[] sel, int batchSize, long[] output) {
        if (leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = leftInputVectorSlot.cast(SliceBlock.class);
            doLatin1LikeSlice(sliceBlock, isSelectionInUse, sel, batchSize, output);
        } else if (leftInputVectorSlot instanceof ReferenceBlock) {
            doLatin1LikeReference(leftInputVectorSlot, isSelectionInUse, sel, batchSize, output);
        }
    }

    private void doLatin1LikeSlice(SliceBlock sliceBlock, boolean isSelectionInUse, int[] sel, int batchSize,
                                   long[] output) {
        Slice cachedSlice = new Slice();

        if (isSelectionInUse) {
            if (isContainsCompare && containBytes != null) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice slice = sliceBlock.getRegion(j, cachedSlice);

                    output[j] = latin1CollationHandler.containsCompare(slice, containBytes, lps)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice slice = sliceBlock.getRegion(j, cachedSlice);

                    output[j] = latin1CollationHandler.wildCompare(slice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        } else {
            if (isContainsCompare && containBytes != null) {
                for (int i = 0; i < batchSize; i++) {

                    Slice slice = sliceBlock.getRegion(i, cachedSlice);

                    output[i] = latin1CollationHandler.containsCompare(slice, containBytes, lps)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {

                    Slice slice = sliceBlock.getRegion(i, cachedSlice);

                    output[i] = latin1CollationHandler.wildCompare(slice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        }
    }

    private void doLatin1LikeReference(RandomAccessBlock leftInputVectorSlot, boolean isSelectionInUse, int[] sel,
                                       int batchSize, long[] output) {
        if (isSelectionInUse) {
            if (isContainsCompare && containBytes != null) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[j] = latin1CollationHandler.containsCompare(lSlice, containBytes, lps)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[j] = latin1CollationHandler.wildCompare(lSlice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        } else {
            if (isContainsCompare && containBytes != null) {
                for (int i = 0; i < batchSize; i++) {

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[i] = latin1CollationHandler.containsCompare(lSlice, containBytes, lps)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[i] = latin1CollationHandler.wildCompare(lSlice, operand1)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
            }
        }
    }
}
