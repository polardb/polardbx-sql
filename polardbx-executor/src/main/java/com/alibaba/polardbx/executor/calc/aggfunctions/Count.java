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

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.state.NullableLongGroupState;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;

public class Count extends AbstractAggregator {

    private NullableLongGroupState groupState;

    public Count(int[] targetIndexes, boolean distinct, int filterArg) {
        super(targetIndexes, distinct, null, DataTypes.LongType, filterArg);
    }

    @Override
    public void open(int capacity) {
        groupState = new NullableLongGroupState(capacity);
    }

    @Override
    public void appendInitValue() {
        groupState.append(0L);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        assert inputChunk.getBlockCount() > 0;
        boolean notNull = true;
        for (int i = 0; i < aggIndexInChunk.length; i++) {
            if (inputChunk.getBlock(aggIndexInChunk[i]).isNull(position)) {
                notNull = false;
                break;
            }
        }
        if (notNull) {
            groupState.set(groupId, groupState.get(groupId) + 1);
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
    public void resetToInitValue(int groupId) {
        groupState.set(groupId, 0L);
    }

    @Override
    public long estimateSize() {
        return groupState.estimateSize();
    }
}
