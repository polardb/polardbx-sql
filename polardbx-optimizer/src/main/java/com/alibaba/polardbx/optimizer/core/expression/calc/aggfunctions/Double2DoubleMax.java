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

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.state.NullableDoubleGroupState;

public class Double2DoubleMax extends AbstractAggregator {

    protected NullableDoubleGroupState groupState;

    public Double2DoubleMax(int index, DataType inputType, DataType outputType, int filterArg) {
        super(new int[] {index}, false, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableDoubleGroupState(capacity);
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

        final double value = block.getDouble(position);
        if (groupState.isNull(groupId)) {
            groupState.set(groupId, value);
        } else {
            double beforeValue = groupState.get(groupId);
            double afterValue = Math.max(value, beforeValue);
            groupState.set(groupId, afterValue);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            bb.writeDouble(groupState.get(groupId));
        }
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}

