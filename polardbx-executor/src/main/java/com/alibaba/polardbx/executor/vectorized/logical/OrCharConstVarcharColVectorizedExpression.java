package com.alibaba.polardbx.executor.vectorized.logical;

import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
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
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

@SuppressWarnings("unused")
@ExpressionSignatures(
    names = {"OR", "||"},
    argumentTypes = {"Char", "Varchar"},
    argumentKinds = {Const, Variable},
    priority = SPECIAL
)
public class OrCharConstVarcharColVectorizedExpression extends AbstractVectorizedExpression {

    private final boolean operand1IsNull;
    private boolean operand1;

    public OrCharConstVarcharColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);
        if (children.length != 2) {
            throw new IllegalArgumentException("OR expression should have 2 children");
        }
        Object operand1Value = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        if (operand1Value == null) {
            this.operand1IsNull = true;
            this.operand1 = false;
        } else {
            this.operand1IsNull = false;
            this.operand1 = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(operand1Value));
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        doOrEval(ctx);
    }

    private void doOrEval(EvaluationContext ctx) {
        children[1].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        boolean[] outputNulls = outputVectorSlot.nulls();

        if (operand1IsNull) {
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

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());
        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        RandomAccessBlock inputVec = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());
        boolean input1HasNull = inputVec.hasNull();

        if (operand1) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    res[j] = LongBlock.TRUE_VALUE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    res[i] = LongBlock.TRUE_VALUE;
                }
            }
            return;
        }


        // operand1 is false
        if (inputVec instanceof SliceBlock) {
            SliceBlock sliceBlock = inputVec.cast(SliceBlock.class);
            orSliceBlock(sliceBlock, isSelectionInUse, sel, batchSize, outputVectorSlot);

        } else if (inputVec instanceof ReferenceBlock){
            ReferenceBlock referenceBlock = inputVec.cast(ReferenceBlock.class);
            orReferenceBlock(referenceBlock, isSelectionInUse, sel, batchSize, outputVectorSlot);

        } else {
            throw new UnsupportedOperationException("Unknown varchar block type: " + inputVec.getClass().getSimpleName());
        }
    }

    private void orReferenceBlock(ReferenceBlock referenceBlock, boolean isSelectionInUse, int[] sel, int batchSize,
                                  RandomAccessBlock outputVectorSlot) {
        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        boolean input1HasNull = referenceBlock.hasNull();
        boolean[] nulls1 = referenceBlock.nulls();
        boolean outputVectorHasNull = false;

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                if (input1HasNull && nulls1[j]) {
                    res[j] = LongBlock.NULL_VALUE;
                    outputNulls[j] = true;
                    outputVectorHasNull = true;
                    continue;
                }
                Slice slice = ((Slice) referenceBlock.elementAt(j));
                long[] result = StringNumericParser.parseString(slice);
                if (result[StringNumericParser.ERROR_INDEX] > 0) {
                    res[j] = LongBlock.FALSE_VALUE;
                    outputNulls[j] = false;
                } else {
                    res[j] = result[StringNumericParser.NUMERIC_INDEX] != 0 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[j] = false;
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                if (input1HasNull && nulls1[i]) {
                    res[i] = LongBlock.NULL_VALUE;
                    outputNulls[i] = true;
                    outputVectorHasNull = true;
                    continue;
                }
                Slice slice = ((Slice) referenceBlock.elementAt(i));
                long[] result = StringNumericParser.parseString(slice);
                if (result[StringNumericParser.ERROR_INDEX] > 0) {
                    res[i] = LongBlock.FALSE_VALUE;
                    outputNulls[i] = false;
                } else {
                    res[i] = result[StringNumericParser.NUMERIC_INDEX] != 0 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[i] = false;
                }
            }
        }
        outputVectorSlot.setHasNull(outputVectorHasNull);
    }

    private void orSliceBlock(SliceBlock sliceBlock, boolean isSelectionInUse, int[] sel, int batchSize,
                              RandomAccessBlock outputVectorSlot) {
        long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
        boolean[] outputNulls = outputVectorSlot.nulls();

        boolean input1HasNull = sliceBlock.hasNull();
        boolean[] nulls1 = sliceBlock.nulls();
        boolean outputVectorHasNull = false;

        Slice cachedSlice = new Slice();

        // did not consider dictionary
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                if (input1HasNull && nulls1[j]) {
                    res[j] = LongBlock.NULL_VALUE;
                    outputNulls[j] = true;
                    outputVectorHasNull = true;
                    continue;
                }
                Slice slice = sliceBlock.getRegion(j, cachedSlice);
                long[] result = StringNumericParser.parseString(slice);
                if (result[StringNumericParser.ERROR_INDEX] > 0) {
                    res[j] = LongBlock.FALSE_VALUE;
                    outputNulls[j] = false;
                } else {
                    res[j] = result[StringNumericParser.NUMERIC_INDEX] != 0 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[j] = false;
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                if (input1HasNull && nulls1[i]) {
                    res[i] = LongBlock.NULL_VALUE;
                    outputNulls[i] = true;
                    outputVectorHasNull = true;
                    continue;
                }
                Slice slice = sliceBlock.getRegion(i, cachedSlice);
                long[] result = StringNumericParser.parseString(slice);
                if (result[StringNumericParser.ERROR_INDEX] > 0) {
                    res[i] = LongBlock.FALSE_VALUE;
                    outputNulls[i] = false;
                } else {
                    res[i] = result[StringNumericParser.NUMERIC_INDEX] != 0 ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    outputNulls[i] = false;
                }
            }
        }
        outputVectorSlot.setHasNull(outputVectorHasNull);
    }

}


