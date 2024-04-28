package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.accumulator.state.NullableLongGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class LongSumAccumulator extends AbstractAccumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.LongType};

    private final NullableLongGroupState state;

    LongSumAccumulator(int capacity) {
        this.state = new NullableLongGroupState(capacity);
    }

    @Override
    public DataType[] getInputTypes() {
        return INPUT_TYPES;
    }

    @Override
    public void appendInitValue() {
        state.appendNull();
    }

    @Override
    public void accumulate(int groupId, Block block, int position) {
        if (block.isNull(position)) {
            return;
        }

        long value = block.getLong(position);
        if (state.isNull(groupId)) {
            state.set(groupId, value);
        } else {
            long beforeValue = state.get(groupId);
            long afterValue = beforeValue + value;
            state.set(groupId, afterValue);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (state.isNull(groupId)) {
            bb.appendNull();
        } else {
            bb.writeLong(state.get(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}
