package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.accumulator.state.LongGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public class LongSum0Accumulator extends AbstractAccumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.LongType};

    private final LongGroupState state;

    LongSum0Accumulator(int capacity) {
        this.state = new LongGroupState(capacity);
    }

    @Override
    public DataType[] getInputTypes() {
        return INPUT_TYPES;
    }

    @Override
    public void appendInitValue() {
        state.append(0L);
    }

    @Override
    public void accumulate(int groupId, Block block, int position) {
        if (block.isNull(position)) {
            return;
        }

        long value = block.getLong(position);
        long beforeValue = state.get(groupId);
        long afterValue = beforeValue + value;
        state.set(groupId, afterValue);
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeLong(state.get(groupId));
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}
