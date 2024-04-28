package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBox;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.accumulator.state.DecimalBoxGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.annotations.VisibleForTesting;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DEC128;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DEC64;

/**
 * does not support mixed scale input
 */
public class DecimalSumAccumulator extends AbstractAccumulator {
    private final DecimalStructure decimalStructure = new DecimalStructure();

    private final DataType[] inputTypes;
    private final DecimalBoxGroupState state;
    private Decimal cache;
    private int scale;

    /**
     * low bits, high bits, error code
     */
    private final long[] results = new long[3];

    public DecimalSumAccumulator(int capacity, DataType inputType) {
        this.cache = new Decimal();
        this.inputTypes = new DataType[1];
        this.inputTypes[0] = inputType;
        this.scale = inputType.getScale();

        this.state = new DecimalBoxGroupState(capacity, scale);
    }

    @Override
    public void appendInitValue() {
        state.appendNull();
    }

    @Override
    public void accumulate(int[] groupIds, Chunk inputChunk, int positionCount) {
        doAccumulateV2(groupIds, inputChunk, positionCount);
    }

    private void doAccumulateV2(int[] groupIds, Chunk inputChunk, int positionCount) {
        Block inputBlock = inputChunk.getBlock(0);
        DecimalBlock decimalBlock = inputBlock.cast(DecimalBlock.class);

        if (decimalBlock.isDecimal64()) {
            rescale(decimalBlock.getScale());
            boolean[] nullArray = decimalBlock.mayHaveNull() ? decimalBlock.nulls() : null;
            long[] decimalValueArray = decimalBlock.getDecimal64Values();
            int[] selection = decimalBlock.getSelection();

            if (selection == null) {
                if (nullArray == null) {
                    // CASE 1: no selection & no nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        long decimal64Val = decimalValueArray[position];

                        accumulateDecimal64(groupId, decimal64Val);
                    }
                } else {
                    // CASE 2: has no selection but has nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        if (nullArray[position]) {
                            continue;
                        }
                        long decimal64Val = decimalValueArray[position];

                        accumulateDecimal64(groupId, decimal64Val);
                    }
                }
            } else {
                if (nullArray == null) {
                    // CASE 3: has selection & no nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        long decimal64Val = decimalValueArray[selection[position]];

                        accumulateDecimal64(groupId, decimal64Val);
                    }
                } else {
                    // CASE 4: has selection & has nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        if (nullArray[selection[position]]) {
                            continue;
                        }
                        long decimal64Val = decimalValueArray[selection[position]];

                        accumulateDecimal64(groupId, decimal64Val);
                    }
                }
            }
        } else if (decimalBlock.isDecimal128()) {
            rescale(decimalBlock.getScale());
            boolean[] nullArray = decimalBlock.mayHaveNull() ? decimalBlock.nulls() : null;
            long[] decimal128LowValues = decimalBlock.getDecimal128LowValues();
            long[] decimal128HighValues = decimalBlock.getDecimal128HighValues();
            int[] selection = decimalBlock.getSelection();

            if (selection == null) {
                if (nullArray == null) {
                    // CASE 1: no selection & no nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        long decimal128Low = decimal128LowValues[position];
                        long decimal128High = decimal128HighValues[position];

                        accumulateDecimal128(groupId, decimal128Low, decimal128High);
                    }
                } else {
                    // CASE 2: has no selection but has nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        if (nullArray[position]) {
                            continue;
                        }
                        long decimal128Low = decimal128LowValues[position];
                        long decimal128High = decimal128HighValues[position];

                        accumulateDecimal128(groupId, decimal128Low, decimal128High);
                    }
                }
            } else {
                if (nullArray == null) {
                    // CASE 3: has selection & no nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        long decimal128Low = decimal128LowValues[selection[position]];
                        long decimal128High = decimal128HighValues[selection[position]];

                        accumulateDecimal128(groupId, decimal128Low, decimal128High);
                    }
                } else {
                    // CASE 4: has selection & has nulls
                    for (int position = 0; position < positionCount; position++) {
                        int groupId = groupIds[position];
                        if (nullArray[selection[position]]) {
                            continue;
                        }
                        long decimal128Low = decimal128LowValues[selection[position]];
                        long decimal128High = decimal128HighValues[selection[position]];

                        accumulateDecimal128(groupId, decimal128Low, decimal128High);
                    }
                }
            }
        } else {
            // Fall back to row-by-row mode
            for (int position = 0; position < positionCount; position++) {
                accumulate(groupIds[position], inputBlock, position);
            }
        }
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int startIndexIncluded, int endIndexExcluded) {
        Block inputBlock = inputChunk.getBlock(0);
        DecimalBlock decimalBlock = inputBlock.cast(DecimalBlock.class);

        // Prepare result array and execute summary in vectorization mode.
        results[0] = results[1] = results[2] = 0;
        decimalBlock.cast(Block.class).sum(startIndexIncluded, endIndexExcluded, results);

        // Check sum result state and try to directly append sum result.
        if (results[2] == E_DEC_DEC64) {
            rescale(decimalBlock.getScale());
            long sumResult = results[0];
            accumulateDecimal64(groupId, sumResult);
        } else if (results[2] == E_DEC_DEC128) {
            rescale(decimalBlock.getScale());
            long decimal128Low = results[0];
            long decimal128High = results[1];
            accumulateDecimal128(groupId, decimal128Low, decimal128High);
        } else {
            // Fall back to row-by-row mode
            for (int position = startIndexIncluded; position < endIndexExcluded; position++) {
                accumulate(groupId, inputBlock, position);
            }
        }
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int[] groupIdSelection, int selSize) {
        Block inputBlock = inputChunk.getBlock(0);
        DecimalBlock decimalBlock = inputBlock.cast(DecimalBlock.class);

        // Prepare result array and execute summary in vectorization mode.
        results[0] = results[1] = results[2] = 0;
        decimalBlock.cast(Block.class).sum(groupIdSelection, selSize, results);

        // Check sum result state and try to directly append sum result.
        if (results[2] == E_DEC_DEC64) {
            rescale(decimalBlock.getScale());
            long sumResult = results[0];
            accumulateDecimal64(groupId, sumResult);
        } else if (results[2] == E_DEC_DEC128) {
            rescale(decimalBlock.getScale());
            long decimal128Low = results[0];
            long decimal128High = results[1];
            accumulateDecimal128(groupId, decimal128Low, decimal128High);
        } else {
            // Fall back to row-by-row mode
            for (int i = 0; i < selSize; i++) {
                int position = groupIdSelection[i];
                accumulate(groupId, inputBlock, position);
            }
        }
    }

    @Override
    public void accumulate(int groupId, Block block, int position) {
        if (block.isNull(position)) {
            return;
        }

        DecimalBlock decimalBlock = block.cast(DecimalBlock.class);
        if (decimalBlock.isDecimal64()) {
            accumulateDecimal64(groupId, decimalBlock, position);
        } else if (decimalBlock.isDecimal128()) {
            accumulateDecimal128(groupId, decimalBlock, position);
        } else {
            accumulateDecimal(groupId, decimalBlock, position);
        }
    }

    private void accumulateDecimal64(int groupId, DecimalBlock decimalBlock, int position) {
        rescale(decimalBlock.getScale());
        long decimal64Val = decimalBlock.getLong(position);
        if (state.isNull(groupId)) {
            state.set(groupId, decimal64Val);
        } else if (state.isDecimal64(groupId)) {
            long oldResult = state.getLong(groupId);
            long addResult = decimal64Val + oldResult;
            if (MathUtils.longAddOverflow(decimal64Val, oldResult, addResult)) {
                // decimal64 overflow to decimal128
                accumulateDecimal64ToDecimal128(groupId, decimal64Val);
            } else {
                state.set(groupId, addResult);
            }
        } else if (state.isDecimal128(groupId)) {
            accumulateDecimal64ToDecimal128(groupId, decimal64Val);
        } else {
            // already overflowed
            // fall back to normal decimal add
            normalAddDecimal64(groupId, decimal64Val);
        }
    }

    private void accumulateDecimal64ToDecimal128(int groupId, long decimal64) {
        if (decimal64 >= 0) {
            accumulateDecimal128(groupId, decimal64, 0);
        } else {
            accumulateDecimal128(groupId, decimal64, -1);
        }
    }

    private void accumulateDecimal128(int groupId, DecimalBlock decimalBlock, int position) {
        rescale(decimalBlock.getScale());
        long decimal128Low = decimalBlock.getDecimal128Low(position);
        long decimal128High = decimalBlock.getDecimal128High(position);
        accumulateDecimal128(groupId, decimal128Low, decimal128High);
    }

    private void accumulateDecimal64(int groupId, long decimal64Val) {
        if (state.isNull(groupId)) {
            state.set(groupId, decimal64Val);
        } else if (state.isDecimal64(groupId)) {
            long oldResult = state.getLong(groupId);
            long addResult = decimal64Val + oldResult;
            if (MathUtils.longAddOverflow(decimal64Val, oldResult, addResult)) {
                // decimal64 overflow to decimal128
                accumulateDecimal64ToDecimal128(groupId, decimal64Val);
            } else {
                state.set(groupId, addResult);
            }
        } else if (state.isDecimal128(groupId)) {
            accumulateDecimal64ToDecimal128(groupId, decimal64Val);
        } else {
            // already overflowed
            // fall back to normal decimal add
            normalAddDecimal64(groupId, decimal64Val);
        }
    }

    private void accumulateDecimal128(int groupId, long decimal128Low, long decimal128High) {
        if (state.isNull(groupId)) {
            state.set(groupId, decimal128Low, decimal128High);
        } else if (state.isDecimal64(groupId)) {
            // convert state from decimal64 to decimal128
            long oldDecimal64 = state.getLong(groupId);
            long valHigh = oldDecimal64 >= 0 ? 0 : -1;
            decimal128High += valHigh;
            long newDecimal128Low = oldDecimal64 + decimal128Low;
            long carryOut = ((oldDecimal64 & decimal128Low)
                | ((oldDecimal64 | decimal128Low) & (~newDecimal128Low))) >>> 63;
            long newDecimal128High = decimal128High + carryOut;
            if (MathUtils.longAddOverflow(carryOut, decimal128High, newDecimal128High)) {
                // decimal128 result overflow
                normalAddDecimal128(groupId, decimal128Low, decimal128High);
            } else {
                state.set(groupId, newDecimal128Low, newDecimal128High);
            }
        } else if (state.isDecimal128(groupId)) {
            long oldDecimal128Low = state.getDecimal128Low(groupId);
            long oldDecimal128High = state.getDecimal128High(groupId);
            long newDecimal128High = oldDecimal128High + decimal128High;
            if (MathUtils.longAddOverflow(oldDecimal128High, decimal128High, newDecimal128High)) {
                // decimal128 result overflow
                normalAddDecimal128(groupId, decimal128Low, decimal128High);
                return;
            }

            long newDecimal128Low = oldDecimal128Low + decimal128Low;
            long carryOut = ((oldDecimal128Low & decimal128Low)
                | ((oldDecimal128Low | decimal128Low) & (~newDecimal128Low))) >>> 63;
            newDecimal128High += carryOut;
            state.set(groupId, newDecimal128Low, newDecimal128High);
        } else {
            // already overflowed
            // fall back to normal decimal add
            normalAddDecimal128(groupId, decimal128Low, decimal128High);
        }
    }

    private void normalAddDecimal64(int groupId, long decimal64Val) {
        Decimal value = new Decimal(decimal64Val, scale);
        if (state.isNull(groupId)) {
            // initialize the operand (not null)
            state.set(groupId, value);
            return;
        }
        Decimal beforeValue;
        if (state.isNormalDecimal(groupId)) {
            beforeValue = state.getDecimal(groupId);
        } else if (state.isDecimal64(groupId)) {
            DecimalStructure buffer = decimalStructure;
            DecimalStructure result = new DecimalStructure();
            FastDecimalUtils.setDecimal128WithScale(buffer, result,
                state.getDecimal128Low(groupId), state.getDecimal128High(groupId), scale);
            beforeValue = new Decimal(result);
        } else if (state.isDecimal128(groupId)) {
            throw new UnsupportedOperationException();
        } else if (state.isDecimalBox(groupId)) {
            beforeValue = state.getBox(groupId).getDecimalSum();
        } else {
            throw new IllegalStateException("Expected Decimal64 state");
        }

        // avoid reset memory to 0
        FastDecimalUtils.add(
            beforeValue.getDecimalStructure(),
            value.getDecimalStructure(),
            cache.getDecimalStructure(),
            false);

        // swap variants to avoid allocating memory
        Decimal afterValue = cache;
        cache = beforeValue;

        state.set(groupId, afterValue);
    }

    private void normalAddDecimal128(int groupId, long decimal128Low, long decimal128High) {
        DecimalStructure buffer = decimalStructure;
        DecimalStructure result = new DecimalStructure();
        FastDecimalUtils.setDecimal128WithScale(buffer, result, decimal128Low, decimal128High, scale);
        Decimal value = new Decimal(result);
        if (state.isNull(groupId)) {
            // initialize the operand (not null)
            state.set(groupId, value);
            return;
        }
        Decimal beforeValue;
        if (state.isNormalDecimal(groupId)) {
            beforeValue = state.getDecimal(groupId);
        } else if (state.isDecimal64(groupId)) {
            beforeValue = new Decimal(state.getLong(groupId), scale);
        } else if (state.isDecimal128(groupId)) {
            DecimalStructure result2 = new DecimalStructure();
            FastDecimalUtils.setDecimal128WithScale(buffer, result2,
                state.getDecimal128Low(groupId), state.getDecimal128High(groupId), scale);
            beforeValue = new Decimal(result2);
        } else if (state.isDecimalBox(groupId)) {
            beforeValue = state.getBox(groupId).getDecimalSum();
        } else {
            throw new IllegalStateException("Expected Decimal state: " + state.getFlag(groupId));
        }

        // avoid reset memory to 0
        FastDecimalUtils.add(
            beforeValue.getDecimalStructure(),
            value.getDecimalStructure(),
            cache.getDecimalStructure(),
            false);

        // swap variants to avoid allocating memory
        Decimal afterValue = cache;
        cache = beforeValue;

        state.set(groupId, afterValue);
    }

    private void accumulateDecimal(int groupId, DecimalBlock decimalBlock, int position) {
        boolean isSimple = decimalBlock.isSimple();
        if (state.isNormalDecimal(groupId)) {
            // normalDecimal + ANY -> normalDecimal
            normalAddDecimal(groupId, decimalBlock, position);
            return;
        }

        if (isSimple) {
            // 1. best case: all decimal value in block is simple
            if (state.isNull(groupId)) {
                // null + decimalBox -> decimalBox
                DecimalBox box = new DecimalBox(scale);
                int a1 = decimalBlock.fastInt1(position);
                int a2 = decimalBlock.fastInt2(position);
                int b = decimalBlock.fastFrac(position);
                box.add(a1, a2, b);

                state.set(groupId, box);
            } else if (state.isDecimal64(groupId)) {
                // decimal64 + decimalBox -> normalDecimal
                state.toNormalDecimalGroupState();
                normalAddDecimal(groupId, decimalBlock, position);
            } else if (state.isDecimal128(groupId)) {
                // decimal128 + decimalBox -> normalDecimal
                state.toNormalDecimalGroupState();
                normalAddDecimal(groupId, decimalBlock, position);
            } else if (state.isDecimalBox(groupId)) {
                // decimalBox + decimalBox -> decimalBox
                DecimalBox box = state.getBox(groupId);

                int a1 = decimalBlock.fastInt1(position);
                int a2 = decimalBlock.fastInt2(position);
                int b = decimalBlock.fastFrac(position);
                box.add(a1, a2, b);
            } else {
                throw new UnsupportedOperationException("Unsupported decimal group state: "
                    + state.getFlag(groupId));
            }
            // state.isNormalDecimal(groupId) is already handled
        } else {
            // 2. bad case: a decimal value is not simple in the block
            // change state to normal
            state.toNormalDecimalGroupState();

            // do normal add
            normalAddDecimal(groupId, decimalBlock, position);
        }
    }

    private void normalAddDecimal(int groupId, DecimalBlock decimalBlock, int position) {
        DecimalStructure decimalStructure = this.decimalStructure;
        decimalBlock.getDecimalStructure(decimalStructure, position);

        if (state.isNull(groupId)) {
            // initialize the operand (not null)
            state.set(groupId, new Decimal(decimalStructure.copy()));
        } else {
            Decimal beforeValue = state.getDecimal(groupId);

            // avoid reset memory to 0
            FastDecimalUtils.add(
                beforeValue.getDecimalStructure(),
                decimalStructure,
                cache.getDecimalStructure(),
                false);

            // swap variants to avoid allocating memory
            Decimal afterValue = cache;
            cache = beforeValue;

            state.set(groupId, afterValue);
        }
    }

    private void rescale(int newScale) {
        if (scale == newScale) {
            return;
        }
        if (!((DecimalType) inputTypes[0]).isDefaultScale()) {
            throw new IllegalStateException("Decimal sum agg input scale does not match in runtime");
        }
        this.scale = newScale;
        this.inputTypes[0] = new DecimalType(inputTypes[0].getPrecision(), newScale);
        this.state.rescale(newScale);
    }

    @Override
    public DataType[] getInputTypes() {
        return inputTypes;
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        DecimalBlockBuilder decimalBlockBuilder = (DecimalBlockBuilder) bb;
        if (decimalBlockBuilder.isUnset()) {
            decimalBlockBuilder.setScale(scale);
        }
        if (state.isNull(groupId)) {
            decimalBlockBuilder.appendNull();
        } else if (state.isDecimal64(groupId)) {
            decimalBlockBuilder.writeLong(state.getLong(groupId));
        } else if (state.isDecimal128(groupId)) {
            decimalBlockBuilder.writeDecimal128(
                state.getDecimal128Low(groupId), state.getDecimal128High(groupId));
        } else {
            decimalBlockBuilder.writeDecimal(state.getDecimal(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }

    @VisibleForTesting
    public boolean isOverflowDecimal64(int groupId) {
        if (state.isNull(groupId)) {
            return false;
        }
        return !state.isDecimal64(groupId);
    }

    @VisibleForTesting
    public boolean isOverflowDecimal128(int groupId) {
        if (state.isNull(groupId)) {
            return false;
        }
        return !state.isDecimal64(groupId) && !state.isDecimal128(groupId);
    }

    @VisibleForTesting
    public boolean isDecimalBox(int groupId) {
        if (state.isNull(groupId)) {
            return false;
        }
        return state.isDecimalBox(groupId);
    }
}
