package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.accumulator.state.NullableDecimalGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class DecimalMaxMinAccumulator extends AbstractAccumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.DecimalType};

    private final NullableDecimalGroupState state;
    private final boolean isMin;

    public DecimalMaxMinAccumulator(int capacity, boolean isMin) {
        this.state = new NullableDecimalGroupState(capacity);
        this.isMin = isMin;
    }

    @Override
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
            state.set(groupId, value);
        } else {
            Decimal beforeValue = state.get(groupId);

            // compare the decimal & before decimal value, find the min/max value.
            int cmpRes = FastDecimalUtils.compare(beforeValue.getDecimalStructure(), value.getDecimalStructure());
            Decimal afterValue = isMin ?
                (cmpRes <= 0 ? beforeValue : value) :
                (cmpRes >= 0 ? beforeValue : value);
            state.set(groupId, afterValue);
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
            bb.writeDecimal(state.get(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}
