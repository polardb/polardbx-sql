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

package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableShortGroupState;

public class Short2ShortMax extends AbstractAggregator {
    protected NullableShortGroupState groupState;

    public Short2ShortMax(int index, DataType inputType, DataType outputType, int filterArg) {
        super(new int[] {index}, false, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableShortGroupState(capacity);
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
    public void accumulate(int groupId, Chunk chunk, int position) {
        Block block = chunk.getBlock(aggIndexInChunk[0]);
        if (block.isNull(position)) {
            return;
        }

        final short value = block.getShort(position);
        if (groupState.isNull(groupId)) {
            groupState.set(groupId, value);
        } else {
            short beforeValue = groupState.get(groupId);
            short afterValue = value > beforeValue ? value : beforeValue;
            groupState.set(groupId, afterValue);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            bb.writeShort(groupState.get(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
