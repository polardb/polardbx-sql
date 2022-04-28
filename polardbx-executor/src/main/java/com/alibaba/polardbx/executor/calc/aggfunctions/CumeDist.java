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
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CumeDist extends Rank {
    protected HashMap<List<Object>, Long> rowKeysToRank = new HashMap<>();
    protected List<Chunk.ChunkRow> bufferRows = new ArrayList<>();
    int rowIndex = 0;

    public CumeDist(int[] index, int filterArg) {
        super(index, filterArg);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        count++;
        if (aggIndexInChunk.length > 0) {
            Chunk.ChunkRow row = inputChunk.rowAt(position);
            bufferRows.add(row);
            List<Object> rankKey = getAggregateKey(row);
            rowKeysToRank.put(rankKey, count);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (rowIndex >= bufferRows.size()) {
            return;
        }
        List<Object> rowKeys = getAggregateKey(bufferRows.get(rowIndex++));
        Long rank = rowKeysToRank.get(rowKeys);
        if (rank == null) {
            bb.writeDouble((double) 1.0);
        } else {
            bb.writeDouble((double) rank / count);
        }
    }

    @Override
    public void resetToInitValue(int groupId) {
        bufferRows = new ArrayList<>();
        rowKeysToRank = new HashMap<>();
        count = 0L;
        rowIndex = 0;
    }

    protected List<Object> getAggregateKey(Row row) {
        if (row == null) {
            return null;
        }
        if (aggIndexInChunk == null || aggIndexInChunk.length == 0) {
            return null;
        }
        List<Object> lastRowValues = row.getValues();
        List<Object> aggTargetIndexValues = new ArrayList<>();
        for (int index : aggIndexInChunk) {
            aggTargetIndexValues.add(lastRowValues.get(index));
        }
        return aggTargetIndexValues;
    }
}
