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
import com.alibaba.polardbx.executor.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;
import java.util.Objects;

/**
 * rank不支持distinct，语义即不支持
 */
public class Rank extends AbstractAggregator {
    protected long rank = 0;
    protected Long count = 0L;
    protected Row lastRow = null;

    public Rank(int[] index, int filterArg) {
        super(index != null && index.length > 0 && index[0] >= 0 ? index : new int[0], false, null, null, filterArg);
    }

    @Override
    public void accumulate(int groupId, Chunk inputChunk, int position) {
        Chunk.ChunkRow row = inputChunk.rowAt(position);
        count++;
        if (!sameRank(lastRow, row)) {
            rank = count;
            lastRow = row;
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeLong(rank);
    }

    @Override
    public void resetToInitValue(int groupId) {
        count = 0L;
        rank = 0L;
        lastRow = null;
    }

    protected boolean sameRank(Row lastRow, Row row) {
        if (lastRow == null) {
            return row == null;
        }
        List<Object> lastRowValues = lastRow.getValues();
        List<Object> rowValues = row.getValues();
        for (int index : aggIndexInChunk) {
            Object o1 = lastRowValues.get(index);
            Object o2 = rowValues.get(index);
            if (!(Objects.equals(o1, o2))) {
                return false;
            }
        }
        return true;
    }
}
