package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.accumulator.state.NullableDoubleGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class DoubleMaxMinAccumulator extends AbstractAccumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.DoubleType};

    private final NullableDoubleGroupState state;
    private final boolean isMin;

    DoubleMaxMinAccumulator(int capacity, boolean isMin) {
        this.state = new NullableDoubleGroupState(capacity);
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

        final double value = block.getDouble(position);
        if (state.isNull(groupId)) {
            state.set(groupId, value);
        } else {
            double beforeValue = state.get(groupId);
            double afterValue = isMin ? Math.min(beforeValue, value) : Math.max(beforeValue, value);
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
            bb.writeDouble(state.get(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}

