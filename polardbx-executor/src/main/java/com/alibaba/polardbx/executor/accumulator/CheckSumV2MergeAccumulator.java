package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.executor.accumulator.state.NullableCheckSumGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * @author yaozhili
 */
public class CheckSumV2MergeAccumulator implements Accumulator {

    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.LongType};

    private final NullableCheckSumGroupState groupState;

    private final static byte SEPARATOR_TAG = (byte) 255;
    private final static byte NULL_TAG = (byte) 254;

    public CheckSumV2MergeAccumulator(int capacity) {
        this.groupState = new NullableCheckSumGroupState(capacity, RevisableOrderInvariantHash.class);
    }

    @Override
    public DataType[] getInputTypes() {
        return INPUT_TYPES;
    }

    @Override
    public void appendInitValue() {
        this.groupState.appendNull();
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Block inputBlock = inputChunk.getBlock(0);
        if (inputBlock.isNull(position)) {
            return;
        }
        long toMerge = inputBlock.getLong(position);

        // write to group state
        if (groupState.isNull(groupId)) {
            RevisableOrderInvariantHash hash = new RevisableOrderInvariantHash();
            hash.add(toMerge).remove(0);
            groupState.set(groupId, hash);
        } else {
            RevisableOrderInvariantHash hash = (RevisableOrderInvariantHash) groupState.getHasher(groupId);
            hash.add(toMerge).remove(0);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            bb.writeLong(groupState.get(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
