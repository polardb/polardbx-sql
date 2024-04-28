package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.accumulator.state.NullableDecimalLongGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Optional;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DEFAULT_DIV_PRECISION_INCREMENT;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DIV_PRECISION_INCREMENT;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.E_DEC_DIV_ZERO;
import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.MAX_DECIMAL_SCALE;

public class DecimalAvgAccumulator extends AbstractAccumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.DecimalType};

    private final NullableDecimalLongGroupState state;

    private Decimal cache;

    /**
     * get div_precision_increment user variables from session.
     */
    private final int divPrecisionIncr;

    DecimalAvgAccumulator(int capacity, ExecutionContext context) {
        this.state = new NullableDecimalLongGroupState(capacity);
        this.divPrecisionIncr = Optional.ofNullable(context)
            .map(ExecutionContext::getServerVariables)
            .map(m -> m.get(DIV_PRECISION_INCREMENT))
            .map(n -> ((Number) n).intValue())
            .map(i -> Math.min(i, MAX_DECIMAL_SCALE))
            .orElse(DEFAULT_DIV_PRECISION_INCREMENT);
        this.cache = new Decimal();
    }

    public void appendInitValue() {
        state.appendNull();
    }

    @Override
    void accumulate(int groupId, Block block, int position) {
        if (block.isNull(position)) {
            return;
        }

        final Decimal value = block.getDecimal(position);
        if (state.isNull(groupId)) {
            state.set(groupId, value.copy(), 1);
        } else {
            Decimal before = state.getDecimal(groupId);

            // avoid to allocate memory
            before.add(value, cache);
            Decimal sum = cache;
            cache = before;
            long count = state.getLong(groupId) + 1;
            state.set(groupId, sum, count);
        }
    }

    @Override
    public DataType[] getInputTypes() {
        return INPUT_TYPES;
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (state.isNull(groupId)) {
            bb.appendNull();
        } else {
            DecimalStructure rounded = new DecimalStructure();
            DecimalStructure unRounded = new DecimalStructure();

            // fetch sum & count decimal value
            Decimal sum = state.getDecimal(groupId);
            Decimal count = Decimal.fromLong(state.getLong(groupId));

            // do divide
            int error = FastDecimalUtils.div(sum.getDecimalStructure(), count.getDecimalStructure(), unRounded,
                divPrecisionIncr);
            if (error == E_DEC_DIV_ZERO) {
                // divide zero, set null
                bb.appendNull();
            } else {
                // do round
                FastDecimalUtils.round(unRounded, rounded, divPrecisionIncr, DecimalRoundMod.HALF_UP);
                Decimal avg = new Decimal(rounded);
                bb.writeDecimal(avg);
            }
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}


