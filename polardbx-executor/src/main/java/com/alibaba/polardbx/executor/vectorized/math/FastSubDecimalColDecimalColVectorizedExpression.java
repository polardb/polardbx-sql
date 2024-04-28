package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

@ExpressionSignatures(
    names = {"-", "subtract"},
    argumentTypes = {"Decimal", "Decimal"},
    argumentKinds = {Variable, Variable},
    priority = SPECIAL)
public class FastSubDecimalColDecimalColVectorizedExpression extends AbstractVectorizedExpression {

    static final int MAX_SCALE_DIFF = 8;

    public FastSubDecimalColDecimalColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.DecimalType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        DecimalBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType).cast(DecimalBlock.class);
        DecimalBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType()).cast(DecimalBlock.class);
        DecimalBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType()).cast(DecimalBlock.class);

        if (leftInputVectorSlot.isDecimal64() && rightInputVectorSlot.isDecimal64()) {
            boolean success =
                doDecimal64Sub(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot,
                    outputVectorSlot);
            if (success) {
                return;
            }
        }

        // do normal sub
        Slice output = outputVectorSlot.getMemorySegments();

        DecimalStructure leftDec;
        DecimalStructure rightDec;

        Slice leftOutput = leftInputVectorSlot.allocCachedSlice();
        Slice rightOutput = rightInputVectorSlot.allocCachedSlice();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
            children[1].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch left decimal value
                leftDec = new DecimalStructure((leftInputVectorSlot).getRegion(j, leftOutput));

                // fetch right decimal value
                rightDec = new DecimalStructure((rightInputVectorSlot).getRegion(j, rightOutput));

                // do operator
                FastDecimalUtils.sub(leftDec, rightDec, toValue);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch left decimal value
                leftDec = new DecimalStructure((leftInputVectorSlot).getRegion(i, leftOutput));

                // fetch right decimal value
                rightDec = new DecimalStructure((rightInputVectorSlot).getRegion(i, rightOutput));

                // do operator
                FastDecimalUtils.sub(leftDec, rightDec, toValue);
            }
        }
        outputVectorSlot.setFullState();
    }

    private boolean doDecimal64Sub(int batchSize, boolean isSelectionInUse, int[] sel,
                                   DecimalBlock leftInputVectorSlot, DecimalBlock rightInputVectorSlot,
                                   DecimalBlock outputVectorSlot) {
        if (leftInputVectorSlot.getScale() == rightInputVectorSlot.getScale()) {
            if (leftInputVectorSlot.getScale() != outputVectorSlot.getScale()) {
                // derived type does not match, which is a rare case
                return false;
            }
            return doDecimal64SubSameScale(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot,
                outputVectorSlot);
        }
        return doDecimal64SubDiffScale(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot,
            outputVectorSlot);
    }

    private boolean doDecimal64SubSameScale(int batchSize, boolean isSelectionInUse, int[] sel,
                                            DecimalBlock leftInputVectorSlot, DecimalBlock rightInputVectorSlot,
                                            DecimalBlock outputVectorSlot) {
        long[] decimal64Output = outputVectorSlot.allocateDecimal64();
        long[] leftArray = leftInputVectorSlot.decimal64Values();
        long[] rightArray = rightInputVectorSlot.decimal64Values();

        boolean overflow = false;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                long leftDec64 = leftArray[j];
                long rightDec64 = rightArray[j];
                long result = leftDec64 - rightDec64;
                decimal64Output[j] = result;

                overflow |= ((leftDec64 ^ rightDec64) & (leftDec64 ^ result)) < 0;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long leftDec64 = leftArray[i];
                long rightDec64 = rightArray[i];
                long result = leftDec64 - rightDec64;
                decimal64Output[i] = result;

                overflow |= ((leftDec64 ^ rightDec64) & (leftDec64 ^ result)) < 0;
            }
        }
        if (overflow) {
            return doDecimal64SameScaleSubTo128(batchSize, isSelectionInUse, sel, leftInputVectorSlot,
                rightInputVectorSlot,
                outputVectorSlot);
        }
        return true;
    }

    /**
     * decimal64 - decimal64 will not overflow
     */
    private boolean doDecimal64SameScaleSubTo128(int batchSize, boolean isSelectionInUse, int[] sel,
                                                 DecimalBlock leftInputVectorSlot, DecimalBlock rightInputVectorSlot,
                                                 DecimalBlock outputVectorSlot) {
        outputVectorSlot.allocateDecimal128();
        long[] output128Low = outputVectorSlot.getDecimal128LowValues();
        long[] output128High = outputVectorSlot.getDecimal128HighValues();
        long[] leftArray = leftInputVectorSlot.decimal64Values();
        long[] rightArray = rightInputVectorSlot.decimal64Values();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                long leftLow = leftArray[j];
                long rightLow = rightArray[j];

                long leftHigh = leftLow >= 0 ? 0 : -1;
                long rightHigh = rightLow >= 0 ? 0 : -1;

                long newDec128High = leftHigh - rightHigh;
                long newDec128Low = leftLow - rightLow;
                long borrow = ((~leftLow & rightLow)
                    | (~(leftLow ^ rightLow) & newDec128Low)) >>> 63;
                newDec128High = newDec128High - borrow;

                output128Low[j] = newDec128Low;
                output128High[j] = newDec128High;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long leftLow = leftArray[i];
                long rightLow = rightArray[i];

                long leftHigh = leftLow >= 0 ? 0 : -1;
                long rightHigh = rightLow >= 0 ? 0 : -1;

                long newDec128High = leftHigh - rightHigh;
                long newDec128Low = leftLow - rightLow;
                long borrow = ((~leftLow & rightLow)
                    | (~(leftLow ^ rightLow) & newDec128Low)) >>> 63;
                newDec128High = newDec128High - borrow;

                output128Low[i] = newDec128Low;
                output128High[i] = newDec128High;
            }
        }
        return true;
    }

    private boolean doDecimal64SubDiffScale(int batchSize, boolean isSelectionInUse, int[] sel,
                                            DecimalBlock leftInputVectorSlot, DecimalBlock rightInputVectorSlot,
                                            DecimalBlock outputVectorSlot) {
        int leftScale = leftInputVectorSlot.getScale();
        int rightScale = rightInputVectorSlot.getScale();
        int maxScale = Math.max(leftScale, rightScale);
        if (outputVectorSlot.getScale() != maxScale) {
            // derived type does not match, which is a rare case
            return false;
        }
        int scaleDiff = leftScale - rightScale;
        int scaleDiffAbs = Math.abs(scaleDiff);
        if (scaleDiffAbs > MAX_SCALE_DIFF) {
            return false;
        }
        long[] decimal64Output = outputVectorSlot.allocateDecimal64();
        long[] leftArray = leftInputVectorSlot.decimal64Values();
        long[] rightArray = rightInputVectorSlot.decimal64Values();

        boolean overflow = false;
        final long pow = DecimalTypeBase.POW_10[scaleDiffAbs];
        if (scaleDiff > 0) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    long leftDec64 = leftArray[j];
                    long rightDec64 = rightArray[j];
                    long scaledRightDec64 = rightDec64 * pow;
                    long result = leftDec64 - scaledRightDec64;
                    decimal64Output[j] = result;

                    overflow |= MathUtils.longMultiplyOverflow(rightDec64, pow, scaledRightDec64);
                    overflow |= ((leftDec64 ^ scaledRightDec64) & (leftDec64 ^ result)) < 0;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long leftDec64 = leftArray[i];
                    long rightDec64 = rightArray[i];
                    long scaledRightDec64 = rightDec64 * pow;
                    long result = leftDec64 - scaledRightDec64;
                    decimal64Output[i] = result;

                    overflow |= MathUtils.longMultiplyOverflow(rightDec64, pow, scaledRightDec64);
                    overflow |= ((leftDec64 ^ scaledRightDec64) & (leftDec64 ^ result)) < 0;
                }
            }
        } else if (scaleDiff < 0) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];

                    long leftDec64 = leftArray[j];
                    long rightDec64 = rightArray[j];
                    long scaledLeftDec64 = leftDec64 * pow;
                    long result = scaledLeftDec64 - rightDec64;
                    decimal64Output[j] = result;

                    overflow |= MathUtils.longMultiplyOverflow(leftDec64, pow, scaledLeftDec64);
                    overflow |= ((scaledLeftDec64 ^ rightDec64) & (scaledLeftDec64 ^ result)) < 0;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    long leftDec64 = leftArray[i];
                    long rightDec64 = rightArray[i];
                    long scaledLeftDec64 = leftDec64 * pow;
                    long result = scaledLeftDec64 - rightDec64;
                    decimal64Output[i] = result;

                    overflow |= MathUtils.longMultiplyOverflow(leftDec64, pow, scaledLeftDec64);
                    overflow |= ((scaledLeftDec64 ^ rightDec64) & (scaledLeftDec64 ^ result)) < 0;
                }
            }

        } else {
            throw new IllegalStateException("Should not reach here");
        }

        if (overflow) {
            outputVectorSlot.deallocateDecimal64();
            return false;
        }
        return true;
    }
}