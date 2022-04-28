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

import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableUInt64GroupState;

public abstract class SpecificType2UInt64BitAnd extends AbstractAggregator {
    private NullableUInt64GroupState groupState;

    public SpecificType2UInt64BitAnd(int index, DataType inputTypes, int filterArg) {
        super(new int[] {index}, false, new DataType[] {inputTypes}, DataTypes.ULongType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableUInt64GroupState(capacity);
    }

    @Override
    public void appendInitValue() {
        groupState.append(UInt64.MAX_UINT64);
    }

    @Override
    public void resetToInitValue(int groupId) {
        groupState.set(groupId, UInt64.MAX_UINT64);
    }

    @Override
    public void accumulate(int groupId, Chunk chunk, int position) {
        Block block = chunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        UInt64 value = getUInt64(block, position);
        UInt64 beforeValue = groupState.get(groupId);
        groupState.set(groupId, beforeValue.bitAnd(value));
    }

    abstract UInt64 getUInt64(Block block, int position);

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeObject(groupState.get(groupId));
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
