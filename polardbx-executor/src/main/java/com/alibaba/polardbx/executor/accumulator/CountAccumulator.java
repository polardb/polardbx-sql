package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.accumulator.state.NullableLongGroupState;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class CountAccumulator implements Accumulator {

    private final NullableLongGroupState state;

    public CountAccumulator(int capacity) {
        this.state = new NullableLongGroupState(capacity);
    }

    @Override
    public void appendInitValue() {
        state.append(0L);
    }

    @Override
    public void accumulate(int[] groupIds, Chunk inputChunk, int[] probePositions, int selSize) {
        if (inputChunk.getBlockCount() == 1) {
            inputChunk.getBlock(0).count(groupIds, probePositions, selSize, state);
        } else {
            Accumulator.super.accumulate(groupIds, inputChunk, probePositions, selSize);
        }
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        assert inputChunk.getBlockCount() > 0;
        boolean notNull = true;
        for (int i = 0; i < inputChunk.getBlockCount(); i++) {
            if (inputChunk.getBlock(i).isNull(position)) {
                notNull = false;
                break;
            }
        }
        if (notNull) {
            state.set(groupId, state.get(groupId) + 1);
        }
    }

    @Override
    public DataType[] getInputTypes() {
        // COUNT() accepts any input types
        return null;
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
