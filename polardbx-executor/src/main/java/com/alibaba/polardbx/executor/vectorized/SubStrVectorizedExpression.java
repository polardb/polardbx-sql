package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(
    names = {"SUBSTRING"},
    argumentTypes = {"Varchar", "Long", "Long"},
    argumentKinds = {Variable, Const, Const}
)
public class SubStrVectorizedExpression extends AbstractVectorizedExpression {
    private boolean shouldReturnNull;
    private boolean shouldReturnEmpty;
    private boolean useNegativeStart;
    private int startPos;
    private int subStrLen;

    public SubStrVectorizedExpression(DataType<?> outputDataType,
                                      int outputIndex, VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        Object operand2Value = ((LiteralVectorizedExpression) children[2]).getConvertedValue();
        if (operand1Value == null || operand2Value == null) {
            startPos = 0;
            subStrLen = 0;
            shouldReturnNull = true;
            shouldReturnEmpty = false;
        } else {
            startPos = DataTypes.LongType.convertFrom(operand1Value).intValue();

            // Assumes that the maximum length of a String is < INT_MAX32
            subStrLen = DataTypes.LongType.convertFrom(operand2Value).intValue();

            // Negative or zero length, will return empty string.
            if (subStrLen <= 0) {
                shouldReturnNull = false;
                shouldReturnEmpty = true;
            }

            // handle start position
            // In MySQL: start= ((start < 0) ? res->numchars() + start : start - 1);
            if (startPos < 0) {
                useNegativeStart = true;
            } else if (startPos == 0) {
                shouldReturnEmpty = true;
            } else {
                useNegativeStart = false;
                startPos = startPos - 1;
            }
        }

    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] selection = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        if (shouldReturnNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            outputVectorSlot.setHasNull(true);
            for (int i = 0; i < batchSize; i++) {
                outputNulls[i] = true;
            }
            return;
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        Object[] objectArray = ((ReferenceBlock) outputVectorSlot).objectArray();
        if (shouldReturnEmpty) {
            // Directly returning the empty slice.
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = selection[i];
                    objectArray[j] = Slices.EMPTY_SLICE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    objectArray[i] = Slices.EMPTY_SLICE;
                }
            }
            return;
        }

        if (leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = (SliceBlock) leftInputVectorSlot;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = selection[i];

                    Slice slice = sliceBlock.getRegion(j);
                    Slice result;

                    int start = useNegativeStart ? slice.length() + startPos : startPos;
                    int len = Math.min(slice.length() - start, subStrLen);
                    if (start < 0 || start + 1 > slice.length()) {
                        // check start pos out of bound
                        result = Slices.EMPTY_SLICE;
                    } else {
                        result = slice.slice(start, len);
                    }

                    objectArray[j] = result;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    Slice slice = sliceBlock.getRegion(i);
                    Slice result;

                    int start = useNegativeStart ? slice.length() + startPos : startPos;
                    int len = Math.min(slice.length() - start, subStrLen);
                    if (start < 0 || start + 1 > slice.length()) {
                        // check start pos out of bound
                        result = Slices.EMPTY_SLICE;
                    } else {
                        result = slice.slice(start, len);
                    }

                    objectArray[i] = result;
                }
            }
        } else if (leftInputVectorSlot instanceof ReferenceBlock) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = selection[i];

                    Slice slice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (slice == null) {
                        objectArray[j] = Slices.EMPTY_SLICE;
                    } else {
                        Slice result;

                        int start = useNegativeStart ? slice.length() + startPos : startPos;
                        int len = Math.min(slice.length() - start, subStrLen);
                        if (start < 0 || start + 1 > slice.length()) {
                            // check start pos out of bound
                            result = Slices.EMPTY_SLICE;
                        } else {
                            result = slice.slice(start, len);
                        }

                        objectArray[j] = result;

                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    Slice slice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (slice == null) {
                        objectArray[i] = Slices.EMPTY_SLICE;
                    } else {
                        Slice result;

                        int start = useNegativeStart ? slice.length() + startPos : startPos;
                        int len = Math.min(slice.length() - start, subStrLen);
                        if (start < 0 || start + 1 > slice.length()) {
                            // check start pos out of bound
                            result = Slices.EMPTY_SLICE;
                        } else {
                            result = slice.slice(start, len);
                        }

                        objectArray[i] = result;
                    }
                }
            }
        }
    }
}
