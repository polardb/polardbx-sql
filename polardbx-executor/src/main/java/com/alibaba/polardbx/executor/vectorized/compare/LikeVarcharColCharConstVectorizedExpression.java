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
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import com.alibaba.polardbx.executor.operator.scan.impl.LocalBlockDictionary;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetFactory;
import com.alibaba.polardbx.optimizer.config.table.charset.CollationHandlers;
import com.alibaba.polardbx.optimizer.config.table.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Latin1BinCollationHandler;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(names = {"LIKE"}, argumentTypes = {"Varchar", "Char"}, argumentKinds = {Variable, Const})
public class LikeVarcharColCharConstVectorizedExpression extends AbstractVectorizedExpression {
    private final CollationHandler originCollationHandler;
    private final Latin1BinCollationHandler latin1CollationHandler =
        (Latin1BinCollationHandler) CollationHandlers.COLLATION_HANDLER_LATIN1_BIN;
    private final boolean operand1IsNull;
    private final Slice operand1;
    /**
     * works with ENABLE_OSS_COMPATIBLE=false
     */
    private final boolean canUseLatin1Collation;
    private LikeType likeType = LikeType.OTHERS;
    private byte[] compareBytes = null;
    private byte[] dictLikeResult = null;

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
            checkLikeType(operand1);
        } else {
            if (isAsciiEncoding(operand1)) {
                canUseLatin1Collation = true;
                checkLikeType(operand1);
            } else {
                canUseLatin1Collation = false;
            }
        }
    }

    /**
     * 1. only support fast path for latin_bin collation currently
     * 2. only support fast poth for middle, prefix and suffix matching
     */
    private void checkLikeType(Slice operand) {
        if (operand == null) {
            return;
        }
        if (!canUseLatin1Collation) {
            //
            return;
        }
        byte[] bytes = operand.getBytes();
        if (operand.length() >= 2 && bytes[0] == CollationHandler.WILD_MANY
            && bytes[bytes.length - 1] == CollationHandler.WILD_MANY) {
            for (int i = 1; i < bytes.length - 1; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ in the middle
                    return;
                }
            }
            this.likeType = LikeType.MIDDLE;
            this.compareBytes = new byte[operand.length() - 2];
            System.arraycopy(bytes, 1, compareBytes, 0, operand.length() - 2);
            return;
        }
        if (operand.length() >= 1 && bytes[0] == CollationHandler.WILD_MANY) {
            for (int i = 1; i < bytes.length; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ after the first one
                    return;
                }
            }
            this.likeType = LikeType.SUFFIX;
            this.compareBytes = new byte[operand.length() - 1];
            System.arraycopy(bytes, 1, compareBytes, 0, operand.length() - 1);
            return;
        }
        if (operand.length() >= 1 && bytes[bytes.length - 1] == CollationHandler.WILD_MANY) {
            for (int i = 0; i < bytes.length - 1; i++) {
                if (bytes[i] == CollationHandler.WILD_MANY || bytes[i] == CollationHandler.WILD_ONE) {
                    // no % _ before the last one
                    return;
                }
            }
            this.likeType = LikeType.PREFIX;
            this.compareBytes = new byte[operand.length() - 1];
            System.arraycopy(bytes, 0, compareBytes, 0, operand.length() - 1);
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

        if (leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = leftInputVectorSlot.cast(SliceBlock.class);
            if (canUseDictCompare(sliceBlock, batchSize)) {
                LocalBlockDictionary dictionary = (LocalBlockDictionary) sliceBlock.getDictionary();
                doCollationLikeDict(sliceBlock, dictionary, isSelectionInUse, sel, batchSize, output);
            } else {
                doCollationLikeSlice(sliceBlock, isSelectionInUse, sel, batchSize, output);
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
            boolean useDictCompare = canUseDictCompare(sliceBlock, batchSize);
            if (useDictCompare) {
                LocalBlockDictionary dictionary = (LocalBlockDictionary) sliceBlock.getDictionary();
                if (compareBytes != null) {
                    doLatin1SpecificLikeDict(sliceBlock, dictionary, isSelectionInUse, sel, batchSize, output);
                } else {
                    doLatin1LikeDict(sliceBlock, dictionary, isSelectionInUse, sel, batchSize, output);
                }
            } else {
                if (compareBytes != null) {
                    doLatin1SpecificLikeSlice(sliceBlock, isSelectionInUse, sel, batchSize, output);
                } else {
                    doLatin1LikeSlice(sliceBlock, isSelectionInUse, sel, batchSize, output);
                }
            }
        } else if (leftInputVectorSlot instanceof ReferenceBlock) {
            if (compareBytes != null) {
                doLatin1SpecificLikeReference(leftInputVectorSlot, isSelectionInUse, sel, batchSize, output);
            } else {
                doLatin1LikeReference(leftInputVectorSlot, isSelectionInUse, sel, batchSize, output);
            }
        }
    }

    private void doCollationLikeSlice(SliceBlock sliceBlock, boolean isSelectionInUse, int[] sel, int batchSize,
                                      long[] output) {
        Slice cachedSlice = new Slice();

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
    }

    private void doCollationLikeDict(SliceBlock sliceBlock, LocalBlockDictionary dictionary, boolean isSelectionInUse,
                                     int[] sel, int batchSize, long[] output) {
        Slice[] dict = dictionary.getDict();
        byte[] dictLikeResult = getDictResultBuffer(dict.length);
        for (int i = 0; i < dict.length; i++) {
            // may utilize selection to reduce the comparisons
            Slice slice = dict[i];
            dictLikeResult[i] = originCollationHandler.wildCompare(slice, operand1)
                ? (byte) 1 : 0;
        }
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int dictId = sliceBlock.getDictId(j);
                output[j] = dictId == -1 ? 0 : dictLikeResult[dictId];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int dictId = sliceBlock.getDictId(i);
                output[i] = dictId == -1 ? 0 : dictLikeResult[dictId];
            }
        }
    }

    private void doLatin1SpecificLikeSlice(SliceBlock sliceBlock, boolean isSelectionInUse, int[] sel, int batchSize,
                                           long[] output) {
        if (likeType == LikeType.OTHERS) {
            // should not reach here
            doLatin1LikeSlice(sliceBlock, isSelectionInUse, sel, batchSize, output);
            return;
        }

        Slice cachedSlice = new Slice();
        if (isSelectionInUse) {
            switch (likeType) {
            case PREFIX:
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    Slice slice = sliceBlock.getRegion(j, cachedSlice);
                    output[j] = latin1CollationHandler.startsWith(slice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case SUFFIX:
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    Slice slice = sliceBlock.getRegion(j, cachedSlice);
                    output[j] = latin1CollationHandler.endsWith(slice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case MIDDLE:
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    Slice slice = sliceBlock.getRegion(j, cachedSlice);
                    output[j] = latin1CollationHandler.contains(slice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported like type: " + likeType);
            }
        } else {
            switch (likeType) {
            case PREFIX:
                for (int i = 0; i < batchSize; i++) {
                    Slice slice = sliceBlock.getRegion(i, cachedSlice);
                    output[i] = latin1CollationHandler.startsWith(slice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case SUFFIX:
                for (int i = 0; i < batchSize; i++) {
                    Slice slice = sliceBlock.getRegion(i, cachedSlice);
                    output[i] = latin1CollationHandler.endsWith(slice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case MIDDLE:
                for (int i = 0; i < batchSize; i++) {
                    Slice slice = sliceBlock.getRegion(i, cachedSlice);
                    output[i] = latin1CollationHandler.contains(slice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported like type: " + likeType);
            }
        }
    }

    /**
     * @param dictionary already checked not null and low cardinality,
     * should be local dictionary but not global
     */
    private void doLatin1SpecificLikeDict(SliceBlock sliceBlock, LocalBlockDictionary dictionary,
                                          boolean isSelectionInUse, int[] sel,
                                          int batchSize, long[] output) {
        if (likeType == LikeType.OTHERS) {
            // should not reach here
            doLatin1LikeDict(sliceBlock, dictionary, isSelectionInUse, sel, batchSize, output);
            return;
        }

        Slice[] dict = dictionary.getDict();
        byte[] dictLikeResult = getDictResultBuffer(dict.length);
        for (int i = 0; i < dict.length; i++) {
            // may utilize selection to reduce the comparisons
            Slice slice = dict[i];
            switch (likeType) {
            case PREFIX:
                dictLikeResult[i] = latin1CollationHandler.startsWith(slice, compareBytes)
                    ? (byte) 1 : 0;
                break;
            case SUFFIX:
                dictLikeResult[i] = latin1CollationHandler.endsWith(slice, compareBytes)
                    ? (byte) 1 : 0;
                break;
            case MIDDLE:
                dictLikeResult[i] = latin1CollationHandler.contains(slice, compareBytes)
                    ? (byte) 1 : 0;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported like type: " + likeType);
            }
        }

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int dictId = sliceBlock.getDictId(j);
                output[j] = dictId == -1 ? 0 : dictLikeResult[dictId];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int dictId = sliceBlock.getDictId(i);
                output[i] = dictId == -1 ? 0 : dictLikeResult[dictId];
            }
        }
    }

    private void doLatin1LikeDict(SliceBlock sliceBlock, LocalBlockDictionary dictionary,
                                  boolean isSelectionInUse, int[] sel,
                                  int batchSize, long[] output) {
        Slice[] dict = dictionary.getDict();
        byte[] dictLikeResult = getDictResultBuffer(dict.length);
        for (int i = 0; i < dict.length; i++) {
            // may utilize selection to reduce the comparisons
            Slice slice = dict[i];
            dictLikeResult[i] = latin1CollationHandler.wildCompare(slice, operand1)
                ? (byte) 1 : 0;
        }
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int dictId = sliceBlock.getDictId(j);
                output[j] = dictId == -1 ? 0 : dictLikeResult[dictId];
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int dictId = sliceBlock.getDictId(i);
                output[i] = dictId == -1 ? 0 : dictLikeResult[dictId];
            }
        }
    }

    private void doLatin1LikeSlice(SliceBlock sliceBlock, boolean isSelectionInUse, int[] sel, int batchSize,
                                   long[] output) {
        Slice cachedSlice = new Slice();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                Slice slice = sliceBlock.getRegion(j, cachedSlice);
                output[j] = latin1CollationHandler.wildCompare(slice, operand1)
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

    private void doLatin1SpecificLikeReference(RandomAccessBlock leftInputVectorSlot, boolean isSelectionInUse,
                                               int[] sel,
                                               int batchSize, long[] output) {
        if (likeType == LikeType.OTHERS) {
            // should not reach here
            doLatin1LikeReference(leftInputVectorSlot, isSelectionInUse, sel, batchSize, output);
            return;
        }

        if (isSelectionInUse) {
            switch (likeType) {
            case PREFIX:
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[j] = latin1CollationHandler.startsWith(lSlice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case SUFFIX:
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[j] = latin1CollationHandler.endsWith(lSlice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case MIDDLE:
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[j] = latin1CollationHandler.contains(lSlice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported like type: " + likeType);
            }
        } else {
            switch (likeType) {
            case PREFIX:
                for (int i = 0; i < batchSize; i++) {
                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[i] = latin1CollationHandler.startsWith(lSlice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case SUFFIX:
                for (int i = 0; i < batchSize; i++) {
                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[i] = latin1CollationHandler.endsWith(lSlice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            case MIDDLE:
                for (int i = 0; i < batchSize; i++) {
                    Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (lSlice == null) {
                        lSlice = Slices.EMPTY_SLICE;
                    }
                    output[i] = latin1CollationHandler.contains(lSlice, compareBytes)
                        ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported like type: " + likeType);
            }
        }
    }

    private void doLatin1LikeReference(RandomAccessBlock leftInputVectorSlot, boolean isSelectionInUse, int[] sel,
                                       int batchSize, long[] output) {
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                Slice lSlice = ((Slice) leftInputVectorSlot.elementAt(j));
                if (lSlice == null) {
                    lSlice = Slices.EMPTY_SLICE;
                }
                output[j] = latin1CollationHandler.wildCompare(lSlice, operand1)
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

    /**
     * 1. has dictionary which is local
     * 2. the size of dictionary is small
     */
    private boolean canUseDictCompare(SliceBlock sliceBlock, int batchSize) {
        BlockDictionary dictionary = sliceBlock.getDictionary();
        if (!(dictionary instanceof LocalBlockDictionary)) {
            return false;
        }
        int dictSize = dictionary.size();

        return dictSize <= (batchSize / 2);
    }

    private byte[] getDictResultBuffer(int length) {
        if (this.dictLikeResult == null || this.dictLikeResult.length < length) {
            this.dictLikeResult = new byte[length];
            return this.dictLikeResult;
        }
        return this.dictLikeResult;
    }

}
