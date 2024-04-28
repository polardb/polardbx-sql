package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.accumulator.state.NullableDoubleLongGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class DoubleAvgAccumulator extends AbstractAccumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.DoubleType};

    private final NullableDoubleLongGroupState state;

    DoubleAvgAccumulator(int capacity) {
        this.state = new NullableDoubleLongGroupState(capacity);
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
            state.set(groupId, value, 1);
        } else {
            double sum = state.getDouble(groupId) + value;
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
            Double avg = (Double) DataTypes.DoubleType.getCalculator().divide(
                state.getDouble(groupId),
                state.getLong(groupId));
            if (avg == null) {
                bb.appendNull();
            } else {
                bb.writeDouble(avg);
            }
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}


