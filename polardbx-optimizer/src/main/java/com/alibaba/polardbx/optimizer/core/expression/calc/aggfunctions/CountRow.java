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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.LongGroupState;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;

public class CountRow extends AbstractAggregator {
    private LongGroupState groupState;

    public CountRow(int[] targetIndexes, boolean distinct, int filterArg) {
        super(targetIndexes, distinct, null, DataTypes.LongType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new LongGroupState(capacity);
    }

    @Override
    public void appendInitValue() {
        groupState.append(0L);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        groupState.set(groupId, groupState.get(groupId) + 1);
    }

    @Override
    public void writeResultTo(int position, BlockBuilder bb) {
        bb.writeLong(groupState.get(position));
    }

    @Override
    public void resetToInitValue(int groupId) {
        groupState.set(groupId, 0L);
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
