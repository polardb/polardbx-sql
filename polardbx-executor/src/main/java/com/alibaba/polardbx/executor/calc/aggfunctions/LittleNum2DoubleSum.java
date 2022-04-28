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

package com.alibaba.polardbx.executor.calc.aggfunctions;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableDoubleGroupState;

public abstract class LittleNum2DoubleSum extends AbstractAggregator {
    private NullableDoubleGroupState groupState;

    public LittleNum2DoubleSum(int targetIndexes, boolean distinct, DataType inputType, DataType outputType,
                               int filterArg) {
        super(new int[] {targetIndexes}, distinct, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableDoubleGroupState(capacity);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Block block = inputChunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        final double value = getDouble(block, position);
        if (groupState.isNull(groupId)) {
            groupState.set(groupId, value);
        } else {
            double oldValue = groupState.get(groupId);
            groupState.set(groupId, value + oldValue);
        }
    }

    abstract double getDouble(Block block, int position);

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            double sum = groupState.get(groupId);
            bb.writeDouble(sum);
        }
    }

    @Override
    public void appendInitValue() {
        groupState.appendNull();
    }

    @Override
    public void resetToInitValue(int groupId) {
        groupState.setNull(groupId);
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}