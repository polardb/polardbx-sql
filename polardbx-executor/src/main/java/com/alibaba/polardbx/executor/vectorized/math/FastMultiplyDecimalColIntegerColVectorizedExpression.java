package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
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

@ExpressionSignatures(names = {"*", "multiply"}, argumentTypes = {"Decimal", "Integer"},
    argumentKinds = {Variable, Variable}, priority = SPECIAL)
public class FastMultiplyDecimalColIntegerColVectorizedExpression extends AbstractVectorizedExpression {

    private final boolean isRightUnsigned;

    public FastMultiplyDecimalColIntegerColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.DecimalType, outputIndex, children);
        isRightUnsigned = children[1].getOutputDataType().isUnsigned();
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
        IntegerBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType()).cast(IntegerBlock.class);

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
            children[1].getOutputIndex());
        boolean sameResultScale = checkResultScale(leftInputVectorSlot.getScale(),
            outputVectorSlot.getScale());
        if (leftInputVectorSlot.isDecimal64() && sameResultScale) {
            boolean success;
            if (!isRightUnsigned) {
                success = doDecimal64Mul(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot,
                    outputVectorSlot);
            } else {
                success =
                    doDecimal64MulUnsigned(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot,
                        outputVectorSlot);
            }
            if (success) {
                // expect never overflow for decimal64
                return;
            }
        }

        if (leftInputVectorSlot.isDecimal128() && sameResultScale) {
            boolean success;
            if (!isRightUnsigned) {
                success = doDecimal128Mul(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot,
                    outputVectorSlot);
            } else {
                success =
                    doDecimal128MulUnsigned(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot,
                        outputVectorSlot);
            }
            if (success) {
                return;
            }
        }

        doNormalMul(batchSize, isSelectionInUse, sel, leftInputVectorSlot, rightInputVectorSlot, outputVectorSlot);
    }

    private boolean checkResultScale(int scale, int outputVectorSlotScale) {
        return scale == outputVectorSlotScale;
    }

    private boolean doDecimal64Mul(int batchSize, boolean isSelectionInUse, int[] sel, DecimalBlock leftInputVectorSlot,
                                   IntegerBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        long[] decimal64Output = outputVectorSlot.allocateDecimal64();
        long[] leftArray = leftInputVectorSlot.decimal64Values();
        int[] rightArray = rightInputVectorSlot.intArray();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                long x = leftArray[j];
                int y = rightArray[j];
                long result = leftArray[j] * rightArray[j];
                if (MathUtils.longMultiplyOverflow(x, y, result)) {
                    return doDecimal64MulTo128(batchSize, isSelectionInUse, sel, leftInputVectorSlot,
                        rightInputVectorSlot,
                        outputVectorSlot);
                }

                decimal64Output[j] = result;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long x = leftArray[i];
                int y = rightArray[i];
                long result = x * y;
                if (MathUtils.longMultiplyOverflow(x, y, result)) {
                    return doDecimal64MulTo128(batchSize, isSelectionInUse, sel, leftInputVectorSlot,
                        rightInputVectorSlot,
                        outputVectorSlot);
                }

                decimal64Output[i] = result;
            }
        }
        return true;
    }

    private boolean doDecimal64MulUnsigned(int batchSize, boolean isSelectionInUse, int[] sel,
                                           DecimalBlock leftInputVectorSlot, IntegerBlock rightInputVectorSlot,
                                           DecimalBlock outputVectorSlot) {
        long[] decimal64Output = outputVectorSlot.allocateDecimal64();
        long[] leftArray = leftInputVectorSlot.decimal64Values();
        int[] rightArray = rightInputVectorSlot.intArray();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                long x = leftArray[j];
                long y = rightArray[j] & 0xFFFFFFFFL;
                long result = leftArray[j] * rightArray[j];
                if (MathUtils.longMultiplyOverflow(x, y, result)) {
                    return doDecimal64MulUnsignedTo128(batchSize, isSelectionInUse, sel, leftInputVectorSlot,
                        rightInputVectorSlot,
                        outputVectorSlot);
                }

                decimal64Output[j] = result;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long x = leftArray[i];
                long y = rightArray[i] & 0xFFFFFFFFL;
                long result = x * y;
                if (MathUtils.longMultiplyOverflow(x, y, result)) {
                    return doDecimal64MulUnsignedTo128(batchSize, isSelectionInUse, sel, leftInputVectorSlot,
                        rightInputVectorSlot,
                        outputVectorSlot);
                }

                decimal64Output[i] = result;
            }
        }
        return true;
    }

    /**
     * decimal64 * int will not overflow decimal128
     */
    private boolean doDecimal64MulTo128(int batchSize, boolean isSelectionInUse, int[] sel,
                                        DecimalBlock leftInputVectorSlot,
                                        IntegerBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        outputVectorSlot.allocateDecimal128();
        long[] outputDecimal128Low = outputVectorSlot.getDecimal128LowValues();
        long[] outputDecimal128High = outputVectorSlot.getDecimal128HighValues();
        long[] leftArray = leftInputVectorSlot.decimal64Values();
        int[] rightArray = rightInputVectorSlot.intArray();
        if (isSelectionInUse) {
            mul64To128(batchSize, sel, leftArray, rightArray, outputDecimal128Low, outputDecimal128High);
        } else {
            mul64To128(batchSize, leftArray, rightArray, outputDecimal128Low, outputDecimal128High);
        }
        return true;
    }

    /**
     * decimal64 * unsigned int will not overflow decimal128
     */
    private boolean doDecimal64MulUnsignedTo128(int batchSize, boolean isSelectionInUse, int[] sel,
                                                DecimalBlock leftInputVectorSlot,
                                                IntegerBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        outputVectorSlot.allocateDecimal128();
        long[] outputDecimal128Low = outputVectorSlot.getDecimal128LowValues();
        long[] outputDecimal128High = outputVectorSlot.getDecimal128HighValues();
        long[] leftArray = leftInputVectorSlot.decimal64Values();
        int[] rightArray = rightInputVectorSlot.intArray();
        if (isSelectionInUse) {
            mul64UnsignedTo128(batchSize, sel, leftArray, rightArray, outputDecimal128Low, outputDecimal128High);
        } else {
            mul64UnsignedTo128(batchSize, leftArray, rightArray, outputDecimal128Low, outputDecimal128High);
        }
        return true;
    }

    /**
     * no overflow
     */
    private void mul64To128(int batchSize, long[] leftArray, int[] rightArray, long[] outputDecimal128Low,
                            long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            long decimal64 = leftArray[i];
            int multiplier = rightArray[i];

            if (decimal64 == 0 || multiplier == 0) {
                outputDecimal128Low[i] = 0;
                outputDecimal128High[i] = 0;
                continue;
            }
            if (decimal64 == 1) {
                outputDecimal128Low[i] = multiplier;
                outputDecimal128High[i] = multiplier >= 0 ? 0 : 1;
                continue;
            }
            if (decimal64 == -1) {
                long negMultiplier = -((long) multiplier);
                outputDecimal128Low[i] = negMultiplier;
                outputDecimal128High[i] = negMultiplier >= 0 ? 0 : 1;
                continue;
            }
            if (multiplier == 1) {
                outputDecimal128Low[i] = decimal64;
                outputDecimal128High[i] = decimal64 >= 0 ? 0 : -1;
                continue;
            }
            if (multiplier == -1 && decimal64 != 0x8000000000000000L) {
                outputDecimal128Low[i] = -decimal64;
                outputDecimal128High[i] = -decimal64 >= 0 ? 0 : -1;
                continue;
            }
            boolean positive;
            long multiplierAbs = multiplier;
            long decimal64Abs = Math.abs(decimal64);
            if (multiplier < 0) {
                multiplierAbs = -multiplierAbs;
                positive = decimal64 < 0;
            } else {
                positive = decimal64 >= 0;
            }
            long sum;

            int x1 = (int) decimal64Abs;
            int x2 = (int) (decimal64Abs >>> 32);
            int x3 = 0;
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (sum >>> 32);
            x3 = (int) sum;
            if (positive) {
                outputDecimal128Low[i] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[i] = (x3 & 0xFFFFFFFFL);
            } else {
                outputDecimal128Low[i] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[i] = ~((x3 & 0xFFFFFFFFL));
                if (outputDecimal128Low[i] == 0) {
                    outputDecimal128High[i] += 1;
                }
            }
        }
    }

    /**
     * no overflow
     */
    private void mul64To128(int batchSize, int[] sel, long[] leftArray, int[] rightArray,
                            long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            int j = sel[i];

            long decimal64 = leftArray[j];
            int multiplier = rightArray[j];

            if (decimal64 == 0 || multiplier == 0) {
                outputDecimal128Low[j] = 0;
                outputDecimal128High[j] = 0;
                continue;
            }
            if (decimal64 == 1) {
                outputDecimal128Low[j] = multiplier;
                outputDecimal128High[j] = multiplier >= 0 ? 0 : 1;
                continue;
            }
            if (decimal64 == -1) {
                long negMultiplier = -((long) multiplier);
                outputDecimal128Low[j] = negMultiplier;
                outputDecimal128High[j] = negMultiplier >= 0 ? 0 : 1;
                continue;
            }
            if (multiplier == 1) {
                outputDecimal128Low[j] = decimal64;
                outputDecimal128High[j] = decimal64 >= 0 ? 0 : -1;
                continue;
            }
            if (multiplier == -1 && decimal64 != 0x8000000000000000L) {
                outputDecimal128Low[j] = -decimal64;
                outputDecimal128High[j] = -decimal64 >= 0 ? 0 : -1;
                continue;
            }
            boolean positive;
            long multiplierAbs = multiplier;
            long decimal64Abs = Math.abs(decimal64);
            if (multiplier < 0) {
                multiplierAbs = -multiplierAbs;
                positive = decimal64 < 0;
            } else {
                positive = decimal64 >= 0;
            }
            long sum;

            int x1 = (int) decimal64Abs;
            int x2 = (int) (decimal64Abs >>> 32);
            int x3 = 0;
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (sum >>> 32);
            x3 = (int) sum;
            if (positive) {
                outputDecimal128Low[j] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[j] = (x3 & 0xFFFFFFFFL);
            } else {
                outputDecimal128Low[j] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[j] = ~((x3 & 0xFFFFFFFFL));
                if (outputDecimal128Low[j] == 0) {
                    outputDecimal128High[j] += 1;
                }
            }
        }
    }

    private void mul64UnsignedTo128(int batchSize, long[] leftArray, int[] rightArray, long[] outputDecimal128Low,
                                    long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            long decimal64 = leftArray[i];
            int multiplier = rightArray[i];

            if (decimal64 == 0 || multiplier == 0) {
                outputDecimal128Low[i] = 0;
                outputDecimal128High[i] = 0;
                continue;
            }
            if (decimal64 == 1) {
                outputDecimal128Low[i] = multiplier;
                outputDecimal128High[i] = multiplier >= 0 ? 0 : 1;
                continue;
            }
            if (decimal64 == -1) {
                long negMultiplier = -((long) multiplier);
                outputDecimal128Low[i] = negMultiplier;
                outputDecimal128High[i] = negMultiplier >= 0 ? 0 : 1;
                continue;
            }
            if (multiplier == 1) {
                outputDecimal128Low[i] = decimal64;
                outputDecimal128High[i] = decimal64 >= 0 ? 0 : -1;
                continue;
            }
            boolean positive = decimal64 >= 0;
            long multiplierAbs = multiplier & 0xFFFFFFFFL;
            long decimal64Abs = Math.abs(decimal64);
            long sum;

            int x1 = (int) decimal64Abs;
            int x2 = (int) (decimal64Abs >>> 32);
            int x3 = 0;
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (sum >>> 32);
            x3 = (int) sum;
            if (positive) {
                outputDecimal128Low[i] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[i] = (x3 & 0xFFFFFFFFL);
            } else {
                outputDecimal128Low[i] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[i] = ~((x3 & 0xFFFFFFFFL));
                if (outputDecimal128Low[i] == 0) {
                    outputDecimal128High[i] += 1;
                }
            }
        }
    }

    /**
     * no overflow
     */
    private void mul64UnsignedTo128(int batchSize, int[] sel, long[] leftArray, int[] rightArray,
                                    long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            int j = sel[i];

            long decimal64 = leftArray[j];
            int multiplier = rightArray[j];

            if (decimal64 == 0 || multiplier == 0) {
                outputDecimal128Low[j] = 0;
                outputDecimal128High[j] = 0;
                continue;
            }
            if (decimal64 == 1) {
                outputDecimal128Low[j] = multiplier;
                outputDecimal128High[j] = multiplier >= 0 ? 0 : 1;
                continue;
            }
            if (decimal64 == -1) {
                long negMultiplier = -((long) multiplier);
                outputDecimal128Low[j] = negMultiplier;
                outputDecimal128High[j] = negMultiplier >= 0 ? 0 : 1;
                continue;
            }
            if (multiplier == 1) {
                outputDecimal128Low[j] = decimal64;
                outputDecimal128High[j] = decimal64 >= 0 ? 0 : -1;
                continue;
            }
            boolean positive = decimal64 >= 0;
            long multiplierAbs = multiplier & 0xFFFFFFFFL;
            long decimal64Abs = Math.abs(decimal64);
            long sum;

            int x1 = (int) decimal64Abs;
            int x2 = (int) (decimal64Abs >>> 32);
            int x3 = 0;
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (sum >>> 32);
            x3 = (int) sum;
            if (positive) {
                outputDecimal128Low[j] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[j] = (x3 & 0xFFFFFFFFL);
            } else {
                outputDecimal128Low[j] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[j] = ~((x3 & 0xFFFFFFFFL));
                if (outputDecimal128Low[j] == 0) {
                    outputDecimal128High[j] += 1;
                }
            }
        }
    }

    private boolean doDecimal128Mul(int batchSize, boolean isSelectionInUse, int[] sel,
                                    DecimalBlock leftInputVectorSlot,
                                    IntegerBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        outputVectorSlot.allocateDecimal128();
        int[] rightArray = rightInputVectorSlot.intArray();
        long[] outputDecimal128Low = outputVectorSlot.getDecimal128LowValues();
        long[] outputDecimal128High = outputVectorSlot.getDecimal128HighValues();
        long[] leftDecimal128Low = leftInputVectorSlot.getDecimal128LowValues();
        long[] leftDecimal128High = leftInputVectorSlot.getDecimal128HighValues();

        if (isSelectionInUse) {
            if (!mul128To128(batchSize, sel, leftDecimal128Low, leftDecimal128High,
                rightArray, outputDecimal128Low, outputDecimal128High)) {
                outputVectorSlot.deallocateDecimal128();
                return false;
            }
            return true;
        } else {
            if (!mul128To128(batchSize, leftDecimal128Low, leftDecimal128High,
                rightArray, outputDecimal128Low, outputDecimal128High)) {
                outputVectorSlot.deallocateDecimal128();
                return false;
            }
            return true;
        }
    }

    private boolean doDecimal128MulUnsigned(int batchSize, boolean isSelectionInUse, int[] sel,
                                            DecimalBlock leftInputVectorSlot,
                                            IntegerBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        outputVectorSlot.allocateDecimal128();
        int[] rightArray = rightInputVectorSlot.intArray();
        long[] outputDecimal128Low = outputVectorSlot.getDecimal128LowValues();
        long[] outputDecimal128High = outputVectorSlot.getDecimal128HighValues();
        long[] leftDecimal128Low = leftInputVectorSlot.getDecimal128LowValues();
        long[] leftDecimal128High = leftInputVectorSlot.getDecimal128HighValues();

        if (isSelectionInUse) {
            if (!mul128UnsignedTo128(batchSize, sel, leftDecimal128Low, leftDecimal128High,
                rightArray, outputDecimal128Low, outputDecimal128High)) {
                outputVectorSlot.deallocateDecimal128();
                return false;
            }
            return true;
        } else {
            if (!mul128UnsignedTo128(batchSize, leftDecimal128Low, leftDecimal128High,
                rightArray, outputDecimal128Low, outputDecimal128High)) {
                outputVectorSlot.deallocateDecimal128();
                return false;
            }
            return true;
        }
    }

    private boolean mul128To128(int batchSize, long[] leftDecimal128Low, long[] leftDecimal128High,
                                int[] rightArray, long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            long decimal128Low = leftDecimal128Low[i];
            long decimal128High = leftDecimal128High[i];
            int multiplier = rightArray[i];

            if (multiplier == 0 || (decimal128Low == 0 && decimal128High == 0)) {
                outputDecimal128Low[i] = 0;
                outputDecimal128High[i] = 0;
                continue;
            }

            if (multiplier == 1) {
                outputDecimal128Low[i] = decimal128Low;
                outputDecimal128High[i] = decimal128High;
                continue;
            }
            if (multiplier == -1) {
                outputDecimal128Low[i] = ~decimal128Low + 1;
                outputDecimal128High[i] = ~decimal128High;
                if (outputDecimal128Low[i] == 0) {
                    outputDecimal128High[i] += 1;
                }
                continue;
            }

            boolean positive;
            long multiplierAbs = multiplier;
            if (multiplier < 0) {
                multiplierAbs = -multiplierAbs;
                positive = decimal128High < 0;
            } else {
                positive = decimal128High >= 0;
            }
            if (decimal128High < 0) {
                decimal128Low = ~decimal128Low + 1;
                decimal128High = ~decimal128High;
                decimal128High += (decimal128Low == 0) ? 1 : 0;
            }
            long sum;
            int x1 = (int) decimal128Low;
            int x2 = (int) (decimal128Low >>> 32);
            int x3 = (int) decimal128High;
            int x4 = (int) (decimal128High >>> 32);
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (x3 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x3 = (int) sum;
            sum = (x4 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x4 = (int) sum;

            if ((sum >> 32) != 0) {
                return false;
            }
            if (positive) {
                outputDecimal128Low[i] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[i] = (x3 & 0xFFFFFFFFL) | (((long) x4) << 32);
            } else {
                outputDecimal128Low[i] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[i] = ~((x3 & 0xFFFFFFFFL) | (((long) x4) << 32));
                if (outputDecimal128Low[i] == 0) {
                    outputDecimal128High[i] += 1;
                }
            }
        }
        return true;
    }

    private boolean mul128To128(int batchSize, int[] sel, long[] leftDecimal128Low, long[] leftDecimal128High,
                                int[] rightArray, long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            int j = sel[i];

            long decimal128Low = leftDecimal128Low[j];
            long decimal128High = leftDecimal128High[j];
            int multiplier = rightArray[j];

            if (multiplier == 0 || (decimal128Low == 0 && decimal128High == 0)) {
                outputDecimal128Low[j] = 0;
                outputDecimal128High[j] = 0;
                continue;
            }

            if (multiplier == 1) {
                outputDecimal128Low[j] = decimal128Low;
                outputDecimal128High[j] = decimal128High;
                return true;
            }
            if (multiplier == -1) {
                outputDecimal128Low[j] = ~decimal128Low + 1;
                outputDecimal128High[j] = ~decimal128High;
                if (outputDecimal128Low[j] == 0) {
                    outputDecimal128High[j] += 1;
                }
                return true;
            }

            boolean positive;
            long multiplierAbs = multiplier;
            if (multiplier < 0) {
                multiplierAbs = -multiplierAbs;
                positive = decimal128High < 0;
            } else {
                positive = decimal128High >= 0;
            }
            if (decimal128High < 0) {
                decimal128Low = ~decimal128Low + 1;
                decimal128High = ~decimal128High;
                decimal128High += (decimal128Low == 0) ? 1 : 0;
            }
            long sum;
            int x1 = (int) decimal128Low;
            int x2 = (int) (decimal128Low >>> 32);
            int x3 = (int) decimal128High;
            int x4 = (int) (decimal128High >>> 32);
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (x3 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x3 = (int) sum;
            sum = (x4 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x4 = (int) sum;

            if ((sum >> 32) != 0) {
                return false;
            }
            if (positive) {
                outputDecimal128Low[j] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[j] = (x3 & 0xFFFFFFFFL) | (((long) x4) << 32);
            } else {
                outputDecimal128Low[j] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[j] = ~((x3 & 0xFFFFFFFFL) | (((long) x4) << 32));
                if (outputDecimal128Low[j] == 0) {
                    outputDecimal128High[j] += 1;
                }
            }
        }
        return true;
    }

    private boolean mul128UnsignedTo128(int batchSize, long[] leftDecimal128Low, long[] leftDecimal128High,
                                        int[] rightArray, long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            long decimal128Low = leftDecimal128Low[i];
            long decimal128High = leftDecimal128High[i];
            int multiplier = rightArray[i];

            if (multiplier == 0 || (decimal128Low == 0 && decimal128High == 0)) {
                outputDecimal128Low[i] = 0;
                outputDecimal128High[i] = 0;
                continue;
            }

            if (multiplier == 1) {
                outputDecimal128Low[i] = decimal128Low;
                outputDecimal128High[i] = decimal128High;
                continue;
            }

            boolean positive = decimal128High >= 0;
            long multiplierAbs = multiplier & 0xFFFFFFFFL;
            if (decimal128High < 0) {
                decimal128Low = ~decimal128Low + 1;
                decimal128High = ~decimal128High;
                decimal128High += (decimal128Low == 0) ? 1 : 0;
            }
            long sum;
            int x1 = (int) decimal128Low;
            int x2 = (int) (decimal128Low >>> 32);
            int x3 = (int) decimal128High;
            int x4 = (int) (decimal128High >>> 32);
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (x3 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x3 = (int) sum;
            sum = (x4 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x4 = (int) sum;

            if ((sum >> 32) != 0) {
                return false;
            }
            if (positive) {
                outputDecimal128Low[i] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[i] = (x3 & 0xFFFFFFFFL) | (((long) x4) << 32);
            } else {
                outputDecimal128Low[i] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[i] = ~((x3 & 0xFFFFFFFFL) | (((long) x4) << 32));
                if (outputDecimal128Low[i] == 0) {
                    outputDecimal128High[i] += 1;
                }
            }
        }
        return true;
    }

    private boolean mul128UnsignedTo128(int batchSize, int[] sel, long[] leftDecimal128Low, long[] leftDecimal128High,
                                        int[] rightArray, long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            int j = sel[i];

            long decimal128Low = leftDecimal128Low[j];
            long decimal128High = leftDecimal128High[j];
            int multiplier = rightArray[j];

            if (multiplier == 0 || (decimal128Low == 0 && decimal128High == 0)) {
                outputDecimal128Low[j] = 0;
                outputDecimal128High[j] = 0;
                continue;
            }

            if (multiplier == 1) {
                outputDecimal128Low[j] = decimal128Low;
                outputDecimal128High[j] = decimal128High;
                continue;
            }

            boolean positive = decimal128High >= 0;
            long multiplierAbs = multiplier & 0xFFFFFFFFL;
            if (decimal128High < 0) {
                decimal128Low = ~decimal128Low + 1;
                decimal128High = ~decimal128High;
                decimal128High += (decimal128Low == 0) ? 1 : 0;
            }
            long sum;
            int x1 = (int) decimal128Low;
            int x2 = (int) (decimal128Low >>> 32);
            int x3 = (int) decimal128High;
            int x4 = (int) (decimal128High >>> 32);
            sum = (x1 & 0xFFFFFFFFL) * multiplierAbs;
            x1 = (int) sum;
            sum = (x2 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x2 = (int) sum;
            sum = (x3 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x3 = (int) sum;
            sum = (x4 & 0xFFFFFFFFL) * multiplierAbs + (sum >>> 32);
            x4 = (int) sum;

            if ((sum >> 32) != 0) {
                return false;
            }
            if (positive) {
                outputDecimal128Low[j] = (x1 & 0xFFFFFFFFL) | (((long) x2) << 32);
                outputDecimal128High[j] = (x3 & 0xFFFFFFFFL) | (((long) x4) << 32);
            } else {
                outputDecimal128Low[j] = ~((x1 & 0xFFFFFFFFL) | (((long) x2) << 32)) + 1;
                outputDecimal128High[j] = ~((x3 & 0xFFFFFFFFL) | (((long) x4) << 32));
                if (outputDecimal128Low[j] == 0) {
                    outputDecimal128High[j] += 1;
                }
            }
        }
        return true;
    }

    private void doNormalMul(int batchSize, boolean isSelectionInUse, int[] sel, DecimalBlock leftInputVectorSlot,
                             IntegerBlock rightInputVectorSlot, DecimalBlock outputVectorSlot) {
        DecimalStructure leftDec;
        DecimalStructure rightDec = new DecimalStructure();
        // use cached slice
        Slice cachedSlice = leftInputVectorSlot.allocCachedSlice();

        int[] array2 = rightInputVectorSlot.intArray();
        Slice output = outputVectorSlot.getMemorySegments();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset
                rightDec.reset();

                // fetch left decimal value
                leftDec = new DecimalStructure((leftInputVectorSlot).getRegion(j, cachedSlice));

                // fetch right decimal value
                DecimalConverter.longToDecimal(array2[j], rightDec, isRightUnsigned);

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
                rightDec.reset();

                // fetch left decimal value
                leftDec = new DecimalStructure((leftInputVectorSlot).getRegion(i, cachedSlice));

                // fetch right decimal value
                DecimalConverter.longToDecimal(array2[i], rightDec, isRightUnsigned);

                // do operator
                FastDecimalUtils.mul(leftDec, rightDec, toValue);
            }
        }
        outputVectorSlot.setFullState();
    }
}
