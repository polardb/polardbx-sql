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

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;

import java.util.List;

// rank不支持distinct，语义即不支持
public class PercentRank extends CumeDist {

    public PercentRank(int[] index, int filterArg) {
        super(index, filterArg);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        count++;
        if (aggIndexInChunk.length > 0) {
            Chunk.ChunkRow row = inputChunk.rowAt(position);
            bufferRows.add(row);
            if (!sameRank(lastRow, row)) {
                List<Object> rankKey = getAggregateKey(row);
                lastRow = row;
                rowKeysToRank.put(rankKey, count);
            }
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (rowIndex >= bufferRows.size()) {
            return;
        }
        List<Object> rowKeys = getAggregateKey(bufferRows.get(rowIndex++));
        Long rank = rowKeysToRank.get(rowKeys);
        if (rank == null || rank <= 1) {
            bb.writeDouble((double) 0.0);
        } else {
            bb.writeDouble((double) (rank - 1) / (count - 1));
        }
    }
}
