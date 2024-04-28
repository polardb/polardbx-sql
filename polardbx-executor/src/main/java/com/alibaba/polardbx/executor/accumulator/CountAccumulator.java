/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
