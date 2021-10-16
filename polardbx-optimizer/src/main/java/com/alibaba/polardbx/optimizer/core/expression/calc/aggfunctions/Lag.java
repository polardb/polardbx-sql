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

import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;

import java.util.HashMap;

public class Lag extends AbstractAggregator {
    protected long offset = 1;
    protected Object defaultValue;
    protected HashMap<Long, Chunk.ChunkRow> indexToValue = new HashMap<>();
    protected long count = 0;
    protected long index = 0;

    public Lag(int index, long offset, Object defaultValue, int filterArg) {
        super(new int[] {index}, false, null, DataTypes.StringType, filterArg);
        if (offset > 0) {
            this.offset = offset;
        }
        this.defaultValue = defaultValue;
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Chunk.ChunkRow row = inputChunk.rowAt(position);
        count++;
        indexToValue.put(count, row);
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        index++;
        if (index - offset > 0) {
            Object value = indexToValue.get(index - offset).getObject(aggIndexInChunk[0]);
            if (value == null) {
                bb.appendNull();
            } else {
                bb.writeString(value.toString());
            }
        } else if (defaultValue == null) {
            bb.appendNull();
        } else {
            bb.writeString(defaultValue.toString());
        }
    }

    @Override
    public void resetToInitValue(int groupId) {
        indexToValue = new HashMap<>();
        count = 0L;
        index = 0;
    }
}
