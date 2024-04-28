package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.accumulator.state.LongGroupState;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class CountRowsAccumulator extends AbstractAccumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {};

    private final LongGroupState state;

    CountRowsAccumulator(int capacity) {
        this.state = new LongGroupState(capacity);
    }

    @Override
    public void appendInitValue() {
        state.append(0L);
    }

    @Override
    public DataType[] getInputTypes() {
        return INPUT_TYPES;
    }

    @Override
    public void accumulate(int groupId) {
        state.set(groupId, state.get(groupId) + 1);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int[] groupIdSelection, int selSize) {
        final int accumulation = selSize;
        state.set(groupId, state.get(groupId) + accumulation);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int startIndexIncluded, int endIndexExcluded) {
        final int accumulation = endIndexExcluded - startIndexIncluded;
        state.set(groupId, state.get(groupId) + accumulation);
    }

    @Override
    public void accumulate(int[] groupIds, Chunk inputChunk, int positionCount) {
        for (int i = 0; i < positionCount; i++) {
            int groupId = groupIds[i];
            state.set(groupId, state.get(groupId) + 1);
        }
    }

    @Override
    public void writeResultTo(int position, BlockBuilder bb) {
        bb.writeLong(state.get(position));
    }

    @Override
    public long estimateSize() {
        return state.estimateSize();
    }
}
