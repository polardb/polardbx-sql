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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableDoubleLongGroupState;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

public abstract class SpecificType2DoubleAvgV2 extends AbstractAggregator {
    private NullableDoubleLongGroupState groupState;

    public SpecificType2DoubleAvgV2(int index, boolean isDistict, DataType inputType, DataType outputType,
                                    int filterArg) {
        super(new int[] {index}, isDistict, new DataType[] {inputType}, outputType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableDoubleLongGroupState(capacity);
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

        final double value = getDouble(block, position);
        if (groupState.isNull(groupId)) {
            groupState.set(groupId, value, 1);
        } else {
            double sum = groupState.getDouble(groupId) + value;
            long count = groupState.getLong(groupId) + 1;
            groupState.set(groupId, sum, count);
        }
    }

    abstract double getDouble(Block block, int position);

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (groupState.isNull(groupId)) {
            bb.appendNull();
        } else {
            Double avg = (Double) DataTypes.DoubleType.getCalculator().divide(
                groupState.getDouble(groupId),
                groupState.getLong(groupId));
            if (avg == null) {
                bb.appendNull();
            } else {
                bb.writeDouble(avg);
            }
        }
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}



